use serde::{Deserialize, Serialize};

use pbs_api_types::{DNS_ALIAS_FORMAT, DNS_NAME_FORMAT, PROXMOX_SAFE_ID_FORMAT};
use proxmox_schema::api;

#[api(
    properties: {
        "domain": { format: &DNS_NAME_FORMAT },
        "alias": {
            optional: true,
            format: &DNS_ALIAS_FORMAT,
        },
        "plugin": {
            optional: true,
            format: &PROXMOX_SAFE_ID_FORMAT,
        },
    },
    default_key: "domain",
)]
#[derive(Deserialize, Serialize)]
/// A domain entry for an ACME certificate.
pub struct AcmeDomain {
    /// The domain to certify for.
    pub domain: String,

    /// The domain to use for challenges instead of the default acme challenge domain.
    ///
    /// This is useful if you use CNAME entries to redirect `_acme-challenge.*` domains to a
    /// different DNS server.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,

    /// The plugin to use to validate this domain.
    ///
    /// Empty means standalone HTTP validation is used.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plugin: Option<String>,
}
