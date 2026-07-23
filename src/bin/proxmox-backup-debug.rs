use proxmox_router::{
    RpcEnvironment,
    cli::{CliCommandMap, CliEnvironment, run_cli_command},
};

mod proxmox_backup_debug;
use proxmox_backup_debug::*;

fn main() {
    proxmox_log::Logger::from_env("PBS_LOG", proxmox_log::LevelFilter::INFO)
        .stderr()
        .init()
        .expect("failed to initiate logger");

    // The `api` subcommand executes API handlers in-process, which rely on the globally
    // cached api/priv user from proxmox-product-config.
    if let Err(err) = pbs_config::backup_user().and_then(|api_user| {
        pbs_config::priv_user().map(|priv_user| proxmox_product_config::init(api_user, priv_user))
    }) {
        eprintln!("failed to initialize product config: {err}");
        std::process::exit(1);
    }

    let cmd_def = CliCommandMap::new()
        .insert("inspect", inspect::inspect_commands())
        .insert("recover", recover::recover_commands())
        .insert("api", api::api_commands())
        .insert("diff", diff::diff_commands());

    let uid = nix::unistd::Uid::current();
    let username = match nix::unistd::User::from_uid(uid) {
        Ok(Some(user)) => user.name,
        _ => "root@pam".to_string(),
    };
    let mut rpcenv = CliEnvironment::new();
    rpcenv.set_auth_id(Some(format!("{username}@pam")));

    run_cli_command(
        cmd_def,
        rpcenv,
        Some(|future| proxmox_async::runtime::main(future)),
    );
}
