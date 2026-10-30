//! Split a generated `syn::File` into one file per namespace module.
//!
//! Every inline `pub mod x { … }` is moved into its own file and replaced by a
//! `#[path = "<stem>/x.rs"] pub mod x;` declaration, recursively, so the file
//! tree mirrors the Rust module tree. The explicit `#[path]` is what lets the
//! root file be `include!`d: a bare `mod x;` inside an `include!`d file
//! resolves against the *including* source file and fails to compile, whereas
//! a `#[path]` resolves against the directory of the file that carries it.
//! Item order is preserved, so the split output is exactly the single-file
//! output partitioned along module boundaries.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use syn::ext::IdentExt;
use syn::{AttrStyle, Item, parse_quote};

/// Split `root` along its inline modules. Returns the root file, with each
/// top-level module replaced by a `#[path]` declaration, plus one file per
/// module keyed by its path relative to the `<root_stem>/` directory beside the
/// root file (`a.rs`, `a/b.rs`, …).
pub(crate) fn split(root: syn::File, root_stem: &str) -> (syn::File, BTreeMap<PathBuf, syn::File>) {
    let mut modules = BTreeMap::new();
    let root = extract_modules(root, root_stem, Path::new(""), &mut modules);
    (root, modules)
}

/// Move every inline module of `file` out into `out`. `stem` is the `#[path]`
/// prefix for its children (relative to `file`'s own directory) and `dir` is
/// where those children live relative to the root module directory.
fn extract_modules(
    mut file: syn::File,
    stem: &str,
    dir: &Path,
    out: &mut BTreeMap<PathBuf, syn::File>,
) -> syn::File {
    for item in &mut file.items {
        let Item::Mod(module) = item else {
            continue;
        };
        let Some((_, items)) = module.content.take() else {
            continue;
        };
        // SPEC §8.3: module idents are unique among siblings (`check_name_collisions`) and
        // snake_cased, so the unraw'd idents give distinct lowercase file names.
        let name = module.ident.unraw().to_string();
        let (inner, outer): (Vec<_>, Vec<_>) = std::mem::take(&mut module.attrs)
            .into_iter()
            .partition(|attr| matches!(attr.style, AttrStyle::Inner(_)));
        let path_attr = format!("{stem}/{name}.rs");
        module.attrs = outer;
        module.attrs.push(parse_quote!(#[path = #path_attr]));
        module.semi = Some(Default::default());

        let child = syn::File {
            shebang: None,
            frontmatter: None,
            attrs: inner,
            items,
        };
        let child = extract_modules(child, &name, &dir.join(&name), out);
        out.insert(dir.join(format!("{name}.rs")), child);
    }
    file
}

#[cfg(test)]
mod tests {
    use super::*;

    fn render(file: &syn::File) -> String {
        crate::codegen::format_file(file)
    }

    /// Inverse of [`split`]: inline every `#[path]` module declaration back
    /// into its body, looking the file up relative to `dir`.
    fn join(mut file: syn::File, dir: &Path, modules: &BTreeMap<PathBuf, syn::File>) -> syn::File {
        for item in &mut file.items {
            let Item::Mod(module) = item else {
                continue;
            };
            let name = module.ident.unraw().to_string();
            let child = modules[&dir.join(format!("{name}.rs"))].clone();
            let child = join(child, &dir.join(&name), modules);
            module.attrs.retain(|attr| !attr.path().is_ident("path"));
            module.attrs.extend(child.attrs);
            module.content = Some((Default::default(), child.items));
            module.semi = None;
        }
        file
    }

    /// Splitting real generated output is lossless: re-inlining the module
    /// files reproduces the single-file token stream exactly, so items keep
    /// their order and nothing is dropped or duplicated across files.
    #[test]
    fn split_of_generated_output_round_trips() {
        let jsons: Vec<String> = [
            r#"{"type":"record","name":"Root","fields":[{"name":"x","type":"int"}]}"#,
            r#"{"type":"record","name":"Deep","namespace":"com.example","fields":[
                {"name":"e","type":"com.E"},
                {"name":"u","type":["int","string"]}
            ]}"#,
            r#"{"type":"enum","name":"E","namespace":"com","symbols":["A"]}"#,
            r#"{"type":"fixed","name":"F","namespace":"org.type","size":4}"#,
        ]
        .map(str::to_string)
        .to_vec();
        let resolved = crate::schema::parse(&jsons, None).expect("parse");
        let file = crate::codegen::generate_file(
            &resolved.schemas,
            &resolved.raw_names,
            &Default::default(),
            crate::codegen::CodegenOptions {
                non_exhaustive: true,
                builder: false,
                collapse_single_union: true,
                allow_unregistered_logical_types: false,
            },
        )
        .expect("codegen");
        let expected = quote::ToTokens::to_token_stream(&file).to_string();

        let (root, modules) = split(file, "gen");
        let paths: Vec<_> = modules.keys().cloned().collect();
        assert_eq!(
            paths,
            [
                PathBuf::from("com/example.rs"),
                PathBuf::from("com.rs"),
                PathBuf::from("org/type.rs"),
                PathBuf::from("org.rs"),
            ]
        );
        let rejoined = join(root, Path::new(""), &modules);
        assert_eq!(
            quote::ToTokens::to_token_stream(&rejoined).to_string(),
            expected
        );
    }

    #[test]
    fn nested_modules_become_path_declared_files() {
        let file: syn::File = parse_quote! {
            pub struct Root;
            pub mod a {
                pub struct A;
                pub mod b {
                    pub struct B;
                }
            }
            pub mod c {
                pub struct C;
            }
        };
        let (root, modules) = split(file, "out");

        let root = render(&root);
        assert!(root.contains("pub struct Root;"), "{root}");
        assert!(
            root.contains("#[path = \"out/a.rs\"]\npub mod a;"),
            "{root}"
        );
        assert!(
            root.contains("#[path = \"out/c.rs\"]\npub mod c;"),
            "{root}"
        );
        assert!(!root.contains("struct A"), "{root}");
        // Sibling module declarations are separated by a blank line.
        assert!(root.contains("pub mod a;\n\n#[path"), "{root}");

        let paths: Vec<_> = modules.keys().cloned().collect();
        assert_eq!(
            paths,
            [
                PathBuf::from("a/b.rs"),
                PathBuf::from("a.rs"),
                PathBuf::from("c.rs")
            ]
        );
        let a = render(&modules[Path::new("a.rs")]);
        assert!(a.contains("pub struct A;"), "{a}");
        // Relative to `out/a.rs`'s directory, so `out/a/b.rs`.
        assert!(a.contains("#[path = \"a/b.rs\"]\npub mod b;"), "{a}");
        assert!(!a.contains("struct B"), "{a}");
        let b = render(&modules[Path::new("a/b.rs")]);
        assert!(b.contains("pub struct B;"), "{b}");
    }

    /// A raw-escaped namespace module (`r#type`) is declared with its raw ident
    /// but stored in a file named without the `r#` prefix.
    #[test]
    fn raw_module_ident_file_name_is_unrawed() {
        let file: syn::File = parse_quote! {
            pub mod r#type {
                pub struct T;
            }
        };
        let (root, modules) = split(file, "gen");
        let root = render(&root);
        assert!(
            root.contains("#[path = \"gen/type.rs\"]\npub mod r#type;"),
            "{root}"
        );
        assert!(
            modules.contains_key(Path::new("type.rs")),
            "{:?}",
            modules.keys()
        );
    }

    #[test]
    fn file_without_modules_is_unchanged() {
        let file: syn::File = parse_quote! {
            pub struct Root;
        };
        let before = render(&file);
        let (root, modules) = split(file, "gen");
        assert!(modules.is_empty());
        assert_eq!(render(&root), before);
    }
}
