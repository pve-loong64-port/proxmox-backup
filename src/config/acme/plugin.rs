use anyhow::Error;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use pbs_api_types::PROXMOX_SAFE_ID_FORMAT;
use proxmox_schema::{api, Schema, StringSchema, Updater};
use proxmox_section_config::SectionConfigData;

pub const PLUGIN_ID_SCHEMA: Schema = StringSchema::new("ACME Challenge Plugin ID.")
    .format(&PROXMOX_SAFE_ID_FORMAT)
    .min_length(1)
    .max_length(32)
    .schema();

#[api(
    properties: {
        id: { schema: PLUGIN_ID_SCHEMA },
        disable: {
            optional: true,
            default: false,
        },
        "validation-delay": {
            default: 30,
            optional: true,
            minimum: 0,
            maximum: 2 * 24 * 60 * 60,
        },
    },
)]
/// DNS ACME Challenge Plugin core data.
#[derive(Deserialize, Serialize, Updater)]
#[serde(rename_all = "kebab-case")]
pub struct DnsPluginCore {
    /// Plugin ID.
    #[updater(skip)]
    pub id: String,

    /// DNS API Plugin Id.
    pub api: String,

    /// Extra delay in seconds to wait before requesting validation.
    ///
    /// Allows to cope with long TTL of DNS records.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub validation_delay: Option<u32>,

    /// Flag to disable the config.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub disable: Option<bool>,
}

#[api(
    properties: {
        core: { type: DnsPluginCore },
    },
)]
/// DNS ACME Challenge Plugin.
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct DnsPlugin {
    #[serde(flatten)]
    pub core: DnsPluginCore,

    // We handle this property separately in the API calls.
    /// DNS plugin data (base64url encoded without padding).
    #[serde(with = "proxmox_serde::string_as_base64url_nopad")]
    pub data: String,
}

impl DnsPlugin {
    pub fn decode_data(&self, output: &mut Vec<u8>) -> Result<(), Error> {
        Ok(proxmox_base64::url::decode_to_vec(&self.data, output)?)
    }
}

pub struct PluginData {
    data: SectionConfigData,
}

// And some convenience helpers.
impl PluginData {
    pub fn remove(&mut self, name: &str) -> Option<(String, Value)> {
        self.data.sections.remove(name)
    }

    pub fn contains_key(&mut self, name: &str) -> bool {
        self.data.sections.contains_key(name)
    }

    pub fn get(&self, name: &str) -> Option<&(String, Value)> {
        self.data.sections.get(name)
    }

    pub fn get_mut(&mut self, name: &str) -> Option<&mut (String, Value)> {
        self.data.sections.get_mut(name)
    }

    pub fn insert(&mut self, id: String, ty: String, plugin: Value) {
        self.data.sections.insert(id, (ty, plugin));
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &(String, Value))> + Send {
        self.data.sections.iter()
    }
}
