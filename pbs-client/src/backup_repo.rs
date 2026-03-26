use std::fmt;

use anyhow::{bail, format_err, Error};
use serde::{Deserialize, Serialize};

use proxmox_schema::*;

use pbs_api_types::{
    Authid, BackupNamespace, Userid, BACKUP_REPO_URL, BACKUP_REPO_URL_REGEX, DATASTORE_SCHEMA,
    IP_V6_REGEX,
};

pub const REPO_URL_SCHEMA: Schema =
    StringSchema::new("Repository URL: [[auth-id@]server[:port]:]datastore")
        .format(&BACKUP_REPO_URL)
        .max_length(256)
        .schema();

pub const BACKUP_REPO_SERVER_SCHEMA: Schema =
    StringSchema::new("Backup server address (hostname or IP). Default: localhost")
        .format(&api_types::DNS_NAME_OR_IP_FORMAT)
        .max_length(256)
        .schema();

pub const BACKUP_REPO_PORT_SCHEMA: Schema = IntegerSchema::new("Backup server port. Default: 8007")
    .minimum(1)
    .maximum(65535)
    .default(8007)
    .schema();

#[api(
    properties: {
        repository: {
            schema: REPO_URL_SCHEMA,
            optional: true,
        },
        server: {
            schema: BACKUP_REPO_SERVER_SCHEMA,
            optional: true,
        },
        port: {
            schema: BACKUP_REPO_PORT_SCHEMA,
            optional: true,
        },
        datastore: {
            schema: DATASTORE_SCHEMA,
            optional: true,
        },
        "auth-id": {
            type: Authid,
            optional: true,
        },
    },
)]
#[derive(Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// Backup repository location, specified either as a repository URL or as individual
/// components (server, port, datastore, auth-id).
pub struct BackupRepositoryArgs {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repository: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datastore: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_id: Option<Authid>,
}

#[api(
    properties: {
        target: {
            type: BackupRepositoryArgs,
            flatten: true,
        },
        ns: {
            type: BackupNamespace,
            optional: true,
        },
    },
)]
#[derive(Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// Backup target for CLI commands, combining the repository location with an
/// optional namespace.
pub struct BackupTargetArgs {
    #[serde(flatten)]
    pub target: BackupRepositoryArgs,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ns: Option<BackupNamespace>,
}

impl BackupRepositoryArgs {
    /// Returns `true` if any atom parameter (server, port, datastore, or auth-id) is set.
    pub fn has_atoms(&self) -> bool {
        self.server.is_some()
            || self.port.is_some()
            || self.datastore.is_some()
            || self.auth_id.is_some()
    }

    /// Check that `--repository` and atom options are not mixed.
    pub fn check_mutual_exclusion(&self) -> Result<(), Error> {
        if self.repository.is_some() && self.has_atoms() {
            bail!("--repository and --server/--port/--datastore/--auth-id are mutually exclusive");
        }
        Ok(())
    }

    /// Merge `self` with `fallback`, using values from `self` where present
    /// and filling in from `fallback` for fields that are `None`.
    pub fn merge_from(self, fallback: BackupRepositoryArgs) -> Self {
        Self {
            repository: self.repository.or(fallback.repository),
            server: self.server.or(fallback.server),
            port: self.port.or(fallback.port),
            datastore: self.datastore.or(fallback.datastore),
            auth_id: self.auth_id.or(fallback.auth_id),
        }
    }
}

impl TryFrom<BackupRepositoryArgs> for BackupRepository {
    type Error = anyhow::Error;

    /// Convert explicit CLI arguments into a [`BackupRepository`].
    ///
    /// * If `repository` and any atom are both set, returns an error.
    /// * If atoms are present, builds the repository from them (requires `datastore`).
    /// * If only `repository` is set, parses the repo URL.
    /// * If nothing is set, returns an error - callers must fall back to environment variables /
    ///   credentials themselves.
    fn try_from(args: BackupRepositoryArgs) -> Result<Self, Self::Error> {
        args.check_mutual_exclusion()?;

        if args.has_atoms() {
            let store = args.datastore.ok_or_else(|| {
                format_err!("--datastore is required when not using --repository")
            })?;
            return Ok(BackupRepository::new(
                args.auth_id,
                args.server,
                args.port,
                store,
            ));
        }

        if let Some(url) = args.repository {
            return url.parse();
        }

        bail!("no repository specified")
    }
}

/// Reference remote backup locations
///

#[derive(Debug)]
pub struct BackupRepository {
    /// The user name used for Authentication
    auth_id: Option<Authid>,
    /// The host name or IP address
    host: Option<String>,
    /// The port
    port: Option<u16>,
    /// The name of the datastore
    store: String,
}

impl BackupRepository {
    pub fn new(
        auth_id: Option<Authid>,
        host: Option<String>,
        port: Option<u16>,
        store: String,
    ) -> Self {
        let host = match host {
            Some(host) if (IP_V6_REGEX.regex_obj)().is_match(&host) => Some(format!("[{host}]")),
            other => other,
        };
        Self {
            auth_id,
            host,
            port,
            store,
        }
    }

    pub fn auth_id(&self) -> &Authid {
        if let Some(ref auth_id) = self.auth_id {
            return auth_id;
        }

        Authid::root_auth_id()
    }

    pub fn user(&self) -> &Userid {
        if let Some(auth_id) = &self.auth_id {
            return auth_id.user();
        }

        Userid::root_userid()
    }

    pub fn host(&self) -> &str {
        if let Some(ref host) = self.host {
            return host;
        }
        "localhost"
    }

    pub fn port(&self) -> u16 {
        if let Some(port) = self.port {
            return port;
        }
        8007
    }

    pub fn store(&self) -> &str {
        &self.store
    }
}

impl fmt::Display for BackupRepository {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match (&self.auth_id, &self.host, self.port) {
            (Some(auth_id), _, _) => write!(
                f,
                "{}@{}:{}:{}",
                auth_id,
                self.host(),
                self.port(),
                self.store
            ),
            (None, Some(host), None) => write!(f, "{}:{}", host, self.store),
            (None, _, Some(port)) => write!(f, "{}:{}:{}", self.host(), port, self.store),
            (None, None, None) => write!(f, "{}", self.store),
        }
    }
}

impl std::str::FromStr for BackupRepository {
    type Err = Error;

    /// Parse a repository URL.
    ///
    /// This parses strings like `user@host:datastore`. The `user` and
    /// `host` parts are optional, where `host` defaults to the local
    /// host, and `user` defaults to `root@pam`.
    fn from_str(url: &str) -> Result<Self, Self::Err> {
        let cap = (BACKUP_REPO_URL_REGEX.regex_obj)()
            .captures(url)
            .ok_or_else(|| format_err!("unable to parse repository url '{}'", url))?;

        Ok(Self {
            auth_id: cap
                .get(1)
                .map(|m| Authid::try_from(m.as_str().to_owned()))
                .transpose()?,
            host: cap.get(2).map(|m| m.as_str().to_owned()),
            port: cap.get(3).map(|m| m.as_str().parse::<u16>()).transpose()?,
            store: cap[4].to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_datastore_only() {
        let repo: BackupRepository = "mystore".parse().unwrap();
        assert_eq!(repo.store(), "mystore");
        assert_eq!(repo.host(), "localhost");
        assert_eq!(repo.port(), 8007);
        assert_eq!(repo.auth_id().to_string(), "root@pam");
    }

    #[test]
    fn parse_host_and_datastore() {
        let repo: BackupRepository = "myhost:mystore".parse().unwrap();
        assert_eq!(repo.host(), "myhost");
        assert_eq!(repo.store(), "mystore");
    }

    #[test]
    fn parse_full_with_port() {
        let repo: BackupRepository = "admin@pam@backuphost:8008:tank".parse().unwrap();
        assert_eq!(repo.auth_id().to_string(), "admin@pam");
        assert_eq!(repo.host(), "backuphost");
        assert_eq!(repo.port(), 8008);
        assert_eq!(repo.store(), "tank");
    }

    #[test]
    fn parse_ipv4_with_port() {
        let repo: BackupRepository = "192.168.1.1:1234:mystore".parse().unwrap();
        assert_eq!(repo.host(), "192.168.1.1");
        assert_eq!(repo.port(), 1234);
    }

    #[test]
    fn parse_ipv6_with_port() {
        let repo: BackupRepository = "[ff80::1]:9007:mystore".parse().unwrap();
        assert_eq!(repo.host(), "[ff80::1]");
        assert_eq!(repo.port(), 9007);
    }

    #[test]
    fn parse_api_token() {
        let repo: BackupRepository = "user@pbs!token@myhost:mystore".parse().unwrap();
        assert_eq!(repo.auth_id().to_string(), "user@pbs!token");
    }

    #[test]
    fn parse_invalid_url_errors() {
        assert!("".parse::<BackupRepository>().is_err());
    }

    #[test]
    fn display_round_trip() {
        for url in [
            "mystore",
            "myhost:mystore",
            "admin@pam@backuphost:8008:tank",
        ] {
            let repo: BackupRepository = url.parse().unwrap();
            assert_eq!(repo.to_string(), url, "round-trip failed for '{url}'");
        }
    }

    #[test]
    fn new_wraps_bare_ipv6_in_brackets() {
        let repo = BackupRepository::new(None, Some("ff80::1".into()), None, "s".into());
        assert_eq!(repo.host(), "[ff80::1]");
    }

    #[test]
    fn new_preserves_already_bracketed_ipv6() {
        let repo = BackupRepository::new(None, Some("[ff80::1]".into()), None, "s".into());
        assert_eq!(repo.host(), "[ff80::1]");
    }

    #[test]
    fn has_atoms() {
        assert!(!BackupRepositoryArgs::default().has_atoms());

        let with_server = BackupRepositoryArgs {
            server: Some("host".into()),
            ..Default::default()
        };
        assert!(with_server.has_atoms());

        let repo_only = BackupRepositoryArgs {
            repository: Some("myhost:mystore".into()),
            ..Default::default()
        };
        assert!(!repo_only.has_atoms());
    }

    #[test]
    fn try_from_atoms_only() {
        let args = BackupRepositoryArgs {
            server: Some("pbs.local".into()),
            port: Some(9000),
            datastore: Some("tank".into()),
            auth_id: Some("backup@pam".parse().unwrap()),
            ..Default::default()
        };
        let repo = BackupRepository::try_from(args).unwrap();
        assert_eq!(repo.host(), "pbs.local");
        assert_eq!(repo.port(), 9000);
        assert_eq!(repo.store(), "tank");
        assert_eq!(repo.auth_id().to_string(), "backup@pam");
    }

    #[test]
    fn try_from_atoms_datastore_only() {
        let args = BackupRepositoryArgs {
            datastore: Some("local".into()),
            ..Default::default()
        };
        let repo = BackupRepository::try_from(args).unwrap();
        assert_eq!(repo.store(), "local");
        assert_eq!(repo.host(), "localhost");
        assert_eq!(repo.port(), 8007);
    }

    #[test]
    fn try_from_url_only() {
        let args = BackupRepositoryArgs {
            repository: Some("admin@pam@backuphost:8008:mystore".into()),
            ..Default::default()
        };
        let repo = BackupRepository::try_from(args).unwrap();
        assert_eq!(repo.host(), "backuphost");
        assert_eq!(repo.port(), 8008);
        assert_eq!(repo.store(), "mystore");
    }

    #[test]
    fn try_from_mutual_exclusion_error() {
        let args = BackupRepositoryArgs {
            repository: Some("somehost:mystore".into()),
            server: Some("otherhost".into()),
            ..Default::default()
        };
        let err = BackupRepository::try_from(args).unwrap_err();
        assert!(err.to_string().contains("mutually exclusive"), "got: {err}");
    }

    #[test]
    fn try_from_nothing_set_error() {
        let err = BackupRepository::try_from(BackupRepositoryArgs::default()).unwrap_err();
        assert!(
            err.to_string().contains("no repository specified"),
            "got: {err}"
        );
    }

    #[test]
    fn try_from_atoms_without_datastore_error() {
        let args = BackupRepositoryArgs {
            server: Some("pbs.local".into()),
            ..Default::default()
        };
        let err = BackupRepository::try_from(args).unwrap_err();
        assert!(
            err.to_string().contains("--datastore is required"),
            "got: {err}"
        );
    }
}
