fn main() {
    let extension_module = std::env::var_os("CARGO_FEATURE_EXTENSION_MODULE").is_some();
    if extension_module {
        pyo3_build_config::add_extension_module_link_args();
        return;
    }

    if let Some(lib_dir) = pyo3_build_config::get().lib_dir.as_deref() {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{lib_dir}");
    }
}
