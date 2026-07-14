use std::process::Command;

use anyhow::{Error, bail, format_err};
use proxmox_sys::command::run_command;
use serde_json::{Value, json};

use proxmox_router::{Permission, Router};
use proxmox_schema::api;

use pbs_api_types::{NODE_SCHEMA, PRIV_SYS_MODIFY, TIME_ZONE_SCHEMA};

fn get_timezone() -> Result<String, Error> {
    if let Ok(timezone) = timedatectl_get_timezone() {
        return Ok(timezone);
    }

    // otherwise guess from the /etc/localtime symlink
    let link = std::fs::read_link("/etc/localtime")
        .map_err(|err| format_err!("failed to guess timezone - {}", err))?;

    let link = link.to_string_lossy();
    match link.rfind("/zoneinfo/") {
        Some(pos) => Ok(link[(pos + 10)..].to_string()),
        None => Ok(link.to_string()),
    }
}

fn timedatectl_get_timezone() -> Result<String, Error> {
    let mut command = Command::new("timedatectl");
    command.args(["show", "--property=Timezone", "--value"]);

    let timezone = run_command(command, None)?;

    Ok(timezone)
}

fn timedatectl_set_timezone(timezone: &str) -> Result<(), Error> {
    let mut command = Command::new("timedatectl");
    command.args(["set-timezone", timezone]);

    run_command(command, None)?;

    Ok(())
}

#[api(
    input: {
        properties: {
            node: {
                schema: NODE_SCHEMA,
            },
        },
    },
    returns: {
        description: "Returns server time and timezone.",
        properties: {
            timezone: {
                schema: TIME_ZONE_SCHEMA,
            },
            time: {
                type: i64,
                description: "Seconds since 1970-01-01 00:00:00 UTC.",
                minimum: 1_297_163_644,
            },
            localtime: {
                type: i64,
                description: "Seconds since 1970-01-01 00:00:00 UTC. (local time)",
                minimum: 1_297_163_644,
            },
        }
    },
    access: {
        permission: &Permission::Anybody,
    },
)]
/// Read server time and time zone settings.
fn get_time(_param: Value) -> Result<Value, Error> {
    let time = proxmox_time::epoch_i64();
    let tm = proxmox_time::localtime(time)?;
    let offset = tm.tm_gmtoff;

    let localtime = time + offset;

    Ok(json!({
        "timezone": get_timezone()?,
        "time": time,
        "localtime": localtime,
    }))
}

#[api(
    protected: true,
    reload_timezone: true,
    input: {
        properties: {
            node: {
                schema: NODE_SCHEMA,
            },
            timezone: {
                schema: TIME_ZONE_SCHEMA,
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&["system", "time"], PRIV_SYS_MODIFY, false),
    },
)]
/// Set time zone
fn set_timezone(timezone: String, _param: Value) -> Result<Value, Error> {
    if timedatectl_set_timezone(&timezone).is_ok() {
        return Ok(Value::Null);
    }

    let path = std::path::PathBuf::from(format!("/usr/share/zoneinfo/{timezone}"));

    if !path.exists() {
        bail!("No such timezone.");
    }

    let _ = std::fs::remove_file("/etc/localtime");

    use std::os::unix::fs::symlink;
    symlink(path, "/etc/localtime")?;

    Ok(Value::Null)
}

pub const ROUTER: Router = Router::new()
    .get(&API_METHOD_GET_TIME)
    .put(&API_METHOD_SET_TIMEZONE);
