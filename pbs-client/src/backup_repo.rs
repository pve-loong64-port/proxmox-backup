use std::fmt;

use anyhow::{format_err, Error};

use pbs_api_types::{Authid, Userid, BACKUP_REPO_URL_REGEX, IP_V6_REGEX};

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
}
