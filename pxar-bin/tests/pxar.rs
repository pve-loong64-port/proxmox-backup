use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};

// Test if xattrs are correctly archived and restored
#[test]
fn pxar_create_and_extract() {
    let src_dir = "../tests/catar_data/test_xattrs_src/";
    let dest_dir = "../tests/catar_data/test_xattrs_dest/";

    let target_subdir = std::env::var("DEB_HOST_RUST_TYPE").unwrap_or_default();

    let exec_path = if cfg!(debug_assertions) {
        format!("../target/{target_subdir}/debug/pxar")
    } else {
        format!("../target/{target_subdir}/release/pxar")
    };

    println!("run '{exec_path} create archive.pxar {src_dir}'");

    Command::new(&exec_path)
        .arg("create")
        .arg("./tests/archive.pxar")
        .arg(src_dir)
        .status()
        .unwrap_or_else(|err| panic!("Failed to invoke '{exec_path}': {err}"));

    println!("run '{exec_path} extract archive.pxar {dest_dir}'");

    Command::new(&exec_path)
        .arg("extract")
        .arg("./tests/archive.pxar")
        .arg("--target")
        .arg(dest_dir)
        .status()
        .unwrap_or_else(|err| panic!("Failed to invoke '{exec_path}': {err}"));

    println!(
        "run 'rsync --dry-run --itemize-changes --archive {src_dir} {dest_dir}' to verify'"
    );
    /* Use rsync with --dry-run and --itemize-changes to compare
    src_dir and dest_dir */
    let stdout = Command::new("rsync")
        .arg("--dry-run")
        .arg("--itemize-changes")
        .arg("--archive")
        .arg(src_dir)
        .arg(dest_dir)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap()
        .stdout
        .unwrap();

    let reader = BufReader::new(stdout);
    let line_iter = reader.lines().map(|l| l.unwrap());
    let mut linecount = 0;
    for curr in line_iter {
        println!("{curr}");
        linecount += 1;
    }
    println!("Rsync listed {linecount} differences to address");

    // Cleanup archive
    Command::new("rm")
        .arg("./tests/archive.pxar")
        .status()
        .unwrap_or_else(|err| panic!("Failed to invoke 'rm': {err}"));

    // Cleanup destination dir
    Command::new("rm")
        .arg("-r")
        .arg(dest_dir)
        .status()
        .unwrap_or_else(|err| panic!("Failed to invoke 'rm': {err}"));

    // If source and destination folder contain the same content,
    // the output of the rsync invocation should yield no lines.
    if linecount != 0 {
        panic!("pxar create and extract did not yield the same contents");
    }
}

#[test]
fn pxar_split_archive_test() {
    let src_dir = "../tests/catar_data/test_files_and_subdirs/";
    let dest_dir = "../tests/catar_data/test_files_and_subdirs_dest/";

    let target_subdir = std::env::var("DEB_HOST_RUST_TYPE").unwrap_or_default();

    let exec_path = if cfg!(debug_assertions) {
        format!("../target/{target_subdir}/debug/pxar")
    } else {
        format!("../target/{target_subdir}/release/pxar")
    };

    println!("run '{exec_path} create archive.mpxar {src_dir} --payload-output archive.ppxar'");

    Command::new(&exec_path)
        .arg("create")
        .arg("./tests/archive.mpxar")
        .arg(src_dir)
        .arg("--payload-output=./tests/archive.ppxar")
        .status()
        .unwrap_or_else(|err| panic!("Failed to invoke '{exec_path}': {err}"));

    let output = Command::new(&exec_path)
        .arg("list")
        .arg("./tests/archive.mpxar")
        .arg("--payload-input=./tests/archive.ppxar")
        .output()
        .expect("failed to run pxar list");
    assert!(output.status.success());

    let expected = "\"/\"
\"/a-test-symlink\"
\"/file1\"
\"/file2\"
\"/subdir1\"
\"/subdir1/subfile1\"
\"/subdir1/subfile2\"
";

    assert_eq!(expected.as_bytes(), output.stdout);

    println!("run '{exec_path} extract archive.mpxar {dest_dir} --payload-input archive.ppxar'");

    Command::new(&exec_path)
        .arg("extract")
        .arg("./tests/archive.mpxar")
        .arg("--payload-input=./tests/archive.ppxar")
        .arg("--target")
        .arg(dest_dir)
        .status()
        .unwrap_or_else(|err| panic!("Failed to invoke '{exec_path}': {err}"));

    println!("run 'rsync --dry-run --itemize-changes --archive {src_dir} {dest_dir}' to verify'");

    /* Use rsync with --dry-run and --itemize-changes to compare
    src_dir and dest_dir */
    let stdout = Command::new("rsync")
        .arg("--dry-run")
        .arg("--itemize-changes")
        .arg("--archive")
        .arg(src_dir)
        .arg(dest_dir)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap()
        .stdout
        .unwrap();

    let reader = BufReader::new(stdout);
    let line_iter = reader.lines().map(|l| l.unwrap());
    let mut linecount = 0;
    for curr in line_iter {
        println!("{curr}");
        linecount += 1;
    }
    println!("Rsync listed {linecount} differences to address");

    // Cleanup archive
    Command::new("rm")
        .arg("./tests/archive.mpxar")
        .arg("./tests/archive.ppxar")
        .status()
        .unwrap_or_else(|err| panic!("Failed to invoke 'rm': {err}"));

    // Cleanup destination dir
    Command::new("rm")
        .arg("-r")
        .arg(dest_dir)
        .status()
        .unwrap_or_else(|err| panic!("Failed to invoke 'rm': {err}"));

    // If source and destination folder contain the same content,
    // the output of the rsync invocation should yield no lines.
    if linecount != 0 {
        panic!("pxar create and extract did not yield the same contents");
    }
}
