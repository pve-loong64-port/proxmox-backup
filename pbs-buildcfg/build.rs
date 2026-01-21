// build.rs
use std::env;
use std::process::Command;

fn main() {
    let repoid = match env::var("REPOID") {
        Ok(repoid) => repoid,
        Err(_) => match Command::new("git").args(["rev-parse", "HEAD"]).output() {
            Ok(output) => String::from_utf8(output.stdout).unwrap(),
            Err(err) => {
                panic!("git rev-parse failed: {err}");
            }
        },
    };

    println!("cargo:rustc-env=REPOID={repoid}");

    let multiarch = match env::var("CARGO_CFG_TARGET_ARCH")
        .as_ref()
        .map(String::as_ref)
    {
        Ok("x86_64") => "x86_64-linux-gnu",
        Ok("aarch64") => "aarch64-linux-gnu",
        Ok("riscv64") => "riscv64-linux-gnu",
        Ok(arch) => {
            panic!("Unsupported architecture: {arch}");
        }
        Err(err) => {
            panic!("Failed to get architecture from CARGO_CFG_TARGET_ARCH - {err}");
        }
    };
    println!("cargo:rustc-env=DEB_HOST_MULTIARCH={multiarch}");
}
