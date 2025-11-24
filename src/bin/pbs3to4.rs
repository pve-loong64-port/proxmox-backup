use pbs_buildcfg::{APT_PKG_STATE_FN, PROXMOX_PKG_RELEASE, PROXMOX_PKG_VERSION};
use proxmox_upgrade_checks::UpgradeCheckerBuilder;

fn main() -> Result<(), anyhow::Error> {
    UpgradeCheckerBuilder::new(
        "bookworm",
        "trixie",
        "proxmox-backup",
        3,
        4,
        0,
        &format!("{PROXMOX_PKG_VERSION}.{PROXMOX_PKG_RELEASE}"),
    )
    .apt_state_file_location(APT_PKG_STATE_FN)
    .add_service_to_checks("proxmox-backup.service")
    .add_service_to_checks("proxmox-backup-proxy.service")
    .build()
    .run()
}
