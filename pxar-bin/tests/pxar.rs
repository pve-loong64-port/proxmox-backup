use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};

// Test if xattrs are correctly archived and restored
#[test]
fn pxar_create_and_extract() {
    let src_dir = "../tests/catar_data/test_xattrs_src/";
    let dest_dir = "../tests/catar_data/test_xattrs_dest/";

    let target_subdir = std::env::var("DEB_HOST_RUST_TYPE").unwrap_or(String::new());

    let exec_path = if cfg!(debug_assertions) {
        format!("../target/{target_subdir}/debug/pxar")
    } else {
        format!("../target/{target_subdir}/release/pxar")
    };

    println!("run '{} create archive.pxar {}'", exec_path, src_dir);

    Command::new(&exec_path)
        .arg("create")
        .arg("./tests/archive.pxar")
        .arg(src_dir)
        .status()
        .unwrap_or_else(|err| panic!("Failed to invoke '{}': {}", exec_path, err));

    println!("run '{} extract archive.pxar {}'", exec_path, dest_dir);

    Command::new(&exec_path)
        .arg("extract")
        .arg("./tests/archive.pxar")
        .arg("--target")
        .arg(dest_dir)
        .status()
        .unwrap_or_else(|err| panic!("Failed to invoke '{}': {}", exec_path, err));

    println!(
        "run 'rsync --dry-run --itemize-changes --archive {} {}' to verify'",
        src_dir, dest_dir
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
        println!("{}", curr);
        linecount += 1;
    }
    println!("Rsync listed {} differences to address", linecount);

    // Cleanup archive
    Command::new("rm")
        .arg("./tests/archive.pxar")
        .status()
        .unwrap_or_else(|err| panic!("Failed to invoke 'rm': {}", err));

    // Cleanup destination dir
    Command::new("rm")
        .arg("-r")
        .arg(dest_dir)
        .status()
        .unwrap_or_else(|err| panic!("Failed to invoke 'rm': {}", err));

    // If source and destination folder contain the same content,
    // the output of the rsync invocation should yield no lines.
    if linecount != 0 {
        panic!("pxar create and extract did not yield the same contents");
    }
}

#[test]
fn pxar_list_with_payload_input() {
    let target_subdir = std::env::var("DEB_HOST_RUST_TYPE").unwrap_or(String::new());

    let exec_path = if cfg!(debug_assertions) {
        format!("../target/{target_subdir}/debug/pxar")
    } else {
        format!("../target/{target_subdir}/release/pxar")
    };

    let output = Command::new(&exec_path)
        .args([
            "list",
            "../tests/pxar/backup-client-pxar-expected.mpxar",
            "--payload-input",
            "../tests/pxar/backup-client-pxar-expected.ppxar",
        ])
        .output()
        .expect("failed to run pxar list");
    assert!(output.status.success());

    let expected = "\"/\"
\"/folder_0\"
\"/folder_0/file_0\"
\"/folder_0/file_1\"
\"/folder_0/file_2\"
\"/folder_0/file_3\"
\"/folder_0/file_4\"
\"/folder_0/file_5\"
\"/folder_0/file_6\"
\"/folder_0/file_7\"
\"/folder_0/file_8\"
\"/folder_0/file_9\"
\"/folder_1\"
\"/folder_1/file_0\"
\"/folder_1/file_1\"
\"/folder_1/file_2\"
\"/folder_1/file_3\"
\"/folder_1/file_4\"
\"/folder_1/file_5\"
\"/folder_1/file_6\"
\"/folder_1/file_7\"
\"/folder_1/file_8\"
\"/folder_1/file_9\"
\"/folder_2\"
\"/folder_2/file_0\"
\"/folder_2/file_1\"
\"/folder_2/file_2\"
\"/folder_2/file_3\"
\"/folder_2/file_4\"
\"/folder_2/file_5\"
\"/folder_2/file_6\"
\"/folder_2/file_7\"
\"/folder_2/file_8\"
\"/folder_2/file_9\"
\"/folder_3\"
\"/folder_3/file_0\"
\"/folder_3/file_1\"
\"/folder_3/file_2\"
\"/folder_3/file_3\"
\"/folder_3/file_4\"
\"/folder_3/file_5\"
\"/folder_3/file_6\"
\"/folder_3/file_7\"
\"/folder_3/file_8\"
\"/folder_3/file_9\"
\"/folder_4\"
\"/folder_4/file_0\"
\"/folder_4/file_1\"
\"/folder_4/file_2\"
\"/folder_4/file_3\"
\"/folder_4/file_4\"
\"/folder_4/file_5\"
\"/folder_4/file_6\"
\"/folder_4/file_7\"
\"/folder_4/file_8\"
\"/folder_4/file_9\"
\"/folder_5\"
\"/folder_5/file_0\"
\"/folder_5/file_1\"
\"/folder_5/file_2\"
\"/folder_5/file_3\"
\"/folder_5/file_4\"
\"/folder_5/file_5\"
\"/folder_5/file_6\"
\"/folder_5/file_7\"
\"/folder_5/file_8\"
\"/folder_5/file_9\"
\"/folder_6\"
\"/folder_6/file_0\"
\"/folder_6/file_1\"
\"/folder_6/file_2\"
\"/folder_6/file_3\"
\"/folder_6/file_4\"
\"/folder_6/file_5\"
\"/folder_6/file_6\"
\"/folder_6/file_7\"
\"/folder_6/file_8\"
\"/folder_6/file_9\"
\"/folder_7\"
\"/folder_7/file_0\"
\"/folder_7/file_1\"
\"/folder_7/file_2\"
\"/folder_7/file_3\"
\"/folder_7/file_4\"
\"/folder_7/file_5\"
\"/folder_7/file_6\"
\"/folder_7/file_7\"
\"/folder_7/file_8\"
\"/folder_7/file_9\"
\"/folder_8\"
\"/folder_8/file_0\"
\"/folder_8/file_1\"
\"/folder_8/file_2\"
\"/folder_8/file_3\"
\"/folder_8/file_4\"
\"/folder_8/file_5\"
\"/folder_8/file_6\"
\"/folder_8/file_7\"
\"/folder_8/file_8\"
\"/folder_8/file_9\"
\"/folder_9\"
\"/folder_9/file_0\"
\"/folder_9/file_1\"
\"/folder_9/file_2\"
\"/folder_9/file_3\"
\"/folder_9/file_4\"
\"/folder_9/file_5\"
\"/folder_9/file_6\"
\"/folder_9/file_7\"
\"/folder_9/file_8\"
\"/folder_9/file_9\"
";

    assert_eq!(expected.as_bytes(), output.stderr);
}
