use dotenv;
// Example custom build script.
fn main() {
    dotenv::dotenv().ok();

    println!("cargo::rerun-if-changed=quantities.json");
    println!("cargo::rerun-if-changed=manage.py");

    subprocess::Exec::cmd("python")
        .arg("manage.py")
        .join()
        .unwrap();
}
