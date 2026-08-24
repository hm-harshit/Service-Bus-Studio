fn main() {
    if std::env::var_os("CARGO_CFG_WINDOWS").is_some() {
        let mut res = winresource::WindowsResource::new();
        res.set_icon("assets/icon.ico");
        res.set("ProductName", "Service Bus Explorer Advance");
        res.set("FileDescription", "Service Bus Explorer Advance");
        res.set("LegalCopyright", "© 2026 Harshit Mahendra");
        // icon/version info is cosmetic — never fail the build over it
        if let Err(e) = res.compile() {
            println!("cargo:warning=skipping exe resources: {e}");
        }
    }
}
