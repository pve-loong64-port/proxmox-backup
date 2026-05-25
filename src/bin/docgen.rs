use anyhow::{Error, bail};

use proxmox_schema::ApiType;
use proxmox_schema::format::dump_enum_properties;
use proxmox_section_config::dump_section_config;

use pbs_api_types::PRIVILEGES;

use proxmox_backup::api2;

fn get_args() -> (String, Vec<String>) {
    let mut args = std::env::args();
    let prefix = args.next().unwrap();
    let prefix = prefix.rsplit('/').next().unwrap().to_string(); // without path
    let args: Vec<String> = args.collect();

    (prefix, args)
}

fn main() -> Result<(), Error> {
    let (_prefix, args) = get_args();

    if args.is_empty() {
        bail!("missing arguments");
    }

    for arg in args.iter() {
        let text = match arg.as_ref() {
            "apidata.js" => generate_api_tree(),
            "datastore.cfg" => dump_section_config(&pbs_config::datastore::CONFIG),
            "domains.cfg" => dump_section_config(&pbs_config::domains::CONFIG),
            "notifications.cfg" => dump_section_config(proxmox_notify::config::config_parser()),
            "notifications-priv.cfg" => {
                dump_section_config(proxmox_notify::config::private_config_parser())
            }
            "tape.cfg" => dump_section_config(&pbs_config::drive::CONFIG),
            "tape-job.cfg" => dump_section_config(&pbs_config::tape_job::CONFIG),
            "user.cfg" => dump_section_config(&pbs_config::user::CONFIG),
            "remote.cfg" => dump_section_config(&pbs_config::remote::CONFIG),
            "sync.cfg" => dump_section_config(&pbs_config::sync::CONFIG),
            "verification.cfg" => dump_section_config(&pbs_config::verify::CONFIG),
            "prune.cfg" => dump_section_config(&pbs_config::prune::CONFIG),
            "media-pool.cfg" => dump_section_config(&pbs_config::media_pool::CONFIG),
            "config::acl::Role" => dump_enum_properties(&pbs_api_types::Role::API_SCHEMA)?,
            _ => bail!("docgen: got unknown type"),
        };
        println!("{text}");
    }

    Ok(())
}

fn generate_api_tree() -> String {
    let mut tree = Vec::new();

    let mut data = proxmox_docgen::generate_api_tree(&api2::ROUTER, ".", PRIVILEGES);
    data["path"] = "/".into();
    // hack: add invisible space to sort as first entry
    data["text"] = "&#x200b;Management API (HTTP)".into();
    tree.push(data);

    let mut data = proxmox_docgen::generate_api_tree(
        &api2::backup::BACKUP_API_ROUTER,
        "/backup/_upgrade_",
        PRIVILEGES,
    );
    data["path"] = "/backup/_upgrade_".into();
    data["text"] = "Backup API (HTTP/2)".into();
    data["expanded"] = false.into();
    tree.push(data);

    let mut data = proxmox_docgen::generate_api_tree(
        &api2::reader::READER_API_ROUTER,
        "/reader/_upgrade_",
        PRIVILEGES,
    );
    data["path"] = "/reader/_upgrade_".into();
    data["text"] = "Restore API (HTTP/2)".into();
    data["expanded"] = false.into();
    tree.push(data);

    format!(
        "var apiSchema = {};",
        serde_json::to_string_pretty(&tree).unwrap()
    )
}
