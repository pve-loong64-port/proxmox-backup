use anyhow::{bail, format_err, Error};
use openssl::pkey::PKey;
use openssl::x509::X509;
use serde::{Deserialize, Serialize};
use tracing::info;

use pbs_api_types::{NODE_SCHEMA, PRIV_SYS_MODIFY};
use proxmox_acme_api::AcmeDomain;
use proxmox_rest_server::WorkerTask;
use proxmox_router::list_subdirs_api_method;
use proxmox_router::SubdirMap;
use proxmox_router::{Permission, Router, RpcEnvironment};
use proxmox_schema::api;

use pbs_buildcfg::configdir;
use pbs_tools::cert;

use crate::server::send_certificate_renewal_mail;

const SECONDS_PER_DAY: i64 = 24 * 60 * 60;

pub const ROUTER: Router = Router::new()
    .get(&list_subdirs_api_method!(SUBDIRS))
    .subdirs(SUBDIRS);

const SUBDIRS: SubdirMap = &[
    ("acme", &ACME_ROUTER),
    (
        "custom",
        &Router::new()
            .post(&API_METHOD_UPLOAD_CUSTOM_CERTIFICATE)
            .delete(&API_METHOD_DELETE_CUSTOM_CERTIFICATE),
    ),
    ("info", &Router::new().get(&API_METHOD_GET_INFO)),
];

const ACME_ROUTER: Router = Router::new()
    .get(&list_subdirs_api_method!(ACME_SUBDIRS))
    .subdirs(ACME_SUBDIRS);

const ACME_SUBDIRS: SubdirMap = &[(
    "certificate",
    &Router::new()
        .post(&API_METHOD_NEW_ACME_CERT)
        .put(&API_METHOD_RENEW_ACME_CERT),
)];

#[api(
    properties: {
        san: {
            type: Array,
            items: {
                description: "A SubjectAlternateName entry.",
                type: String,
            },
        },
    },
)]
/// Certificate information.
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct CertificateInfo {
    /// Certificate file name.
    pub filename: String,

    /// Certificate subject name.
    pub subject: String,

    /// List of certificate's SubjectAlternativeName entries.
    pub san: Vec<String>,

    /// Certificate issuer name.
    pub issuer: String,

    /// Certificate's notBefore timestamp (UNIX epoch).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notbefore: Option<i64>,

    /// Certificate's notAfter timestamp (UNIX epoch).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notafter: Option<i64>,

    /// Certificate in PEM format.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pem: Option<String>,

    /// Certificate's public key algorithm.
    pub public_key_type: String,

    /// Certificate's public key size if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub public_key_bits: Option<u32>,

    /// The SSL Fingerprint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
}

fn get_certificate_pem() -> Result<String, Error> {
    let cert_path = configdir!("/proxy.pem");
    let cert_pem = proxmox_sys::fs::file_get_contents(cert_path)?;
    String::from_utf8(cert_pem)
        .map_err(|_| format_err!("certificate in {:?} is not a valid PEM file", cert_path))
}

// to deduplicate error messages
fn pem_to_cert_info(pem: &[u8]) -> Result<cert::CertInfo, Error> {
    cert::CertInfo::from_pem(pem)
        .map_err(|err| format_err!("error loading proxy certificate: {}", err))
}

#[api(
    input: {
        properties: {
            node: { schema: NODE_SCHEMA },
        },
    },
    access: {
        permission: &Permission::Privilege(&["system", "certificates"], PRIV_SYS_MODIFY, false),
    },
    returns: {
        type: Array,
        items: { type: CertificateInfo },
        description: "List of certificate infos.",
    },
)]
/// Get certificate info.
pub fn get_info() -> Result<Vec<CertificateInfo>, Error> {
    let cert_pem = get_certificate_pem()?;
    let info = pem_to_cert_info(cert_pem.as_bytes())?;
    let pubkey = info.public_key()?;

    Ok(vec![CertificateInfo {
        filename: "proxy.pem".to_string(), // we only have the one
        pem: Some(cert_pem),
        subject: info.subject_name()?,
        san: info
            .subject_alt_names()
            .map(|san| {
                san.into_iter()
                    // FIXME: Support `.ipaddress()`?
                    .filter_map(|name| name.dnsname().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default(),
        issuer: info.issuer_name()?,
        notbefore: info.not_before_unix().ok(),
        notafter: info.not_after_unix().ok(),
        public_key_type: openssl::nid::Nid::from_raw(pubkey.id().as_raw())
            .long_name()
            .unwrap_or("<unsupported key type>")
            .to_owned(),
        public_key_bits: Some(pubkey.bits()),
        fingerprint: Some(info.fingerprint()?),
    }])
}

#[api(
    input: {
        properties: {
            node: { schema: NODE_SCHEMA },
            certificates: { description: "PEM encoded certificate (chain)." },
            key: {
                description: "PEM encoded private key.",
                optional: true,
            },
            // FIXME: widget-toolkit should have an option to disable using these 2 parameters...
            restart: {
                description: "UI compatibility parameter, ignored",
                type: Boolean,
                optional: true,
                default: false,
            },
            force: {
                description: "Force replacement of existing files.",
                type: Boolean,
                optional: true,
                default: false,
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&["system", "certificates"], PRIV_SYS_MODIFY, false),
    },
    returns: {
        type: Array,
        items: { type: CertificateInfo },
        description: "List of certificate infos.",
    },
    protected: true,
)]
/// Upload a custom certificate.
pub async fn upload_custom_certificate(
    certificates: String,
    key: Option<String>,
) -> Result<Vec<CertificateInfo>, Error> {
    let certificates = X509::stack_from_pem(certificates.as_bytes())
        .map_err(|err| format_err!("failed to decode certificate chain: {}", err))?;

    let key = match key {
        Some(key) => key,
        None => proxmox_sys::fs::file_read_string(configdir!("/proxy.key"))?,
    };

    let key = PKey::private_key_from_pem(key.as_bytes())
        .map_err(|err| format_err!("failed to parse private key: {}", err))?;

    let certificates = certificates
        .into_iter()
        .try_fold(Vec::<u8>::new(), |mut stack, cert| -> Result<_, Error> {
            if !stack.is_empty() {
                stack.push(b'\n');
            }
            stack.extend(cert.to_pem()?);
            Ok(stack)
        })
        .map_err(|err| format_err!("error formatting certificate chain as PEM: {}", err))?;

    let key = key.private_key_to_pem_pkcs8()?;

    crate::config::set_proxy_certificate(&certificates, &key)?;
    crate::server::reload_proxy_certificate().await?;

    get_info()
}

#[api(
    input: {
        properties: {
            node: { schema: NODE_SCHEMA },
            restart: {
                description: "UI compatibility parameter, ignored",
                type: Boolean,
                optional: true,
                default: false,
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&["system", "certificates"], PRIV_SYS_MODIFY, false),
    },
    protected: true,
)]
/// Delete the current certificate and regenerate a self signed one.
pub async fn delete_custom_certificate() -> Result<(), Error> {
    let cert_path = configdir!("/proxy.pem");
    // Here we fail since if this fails nothing else breaks anyway
    std::fs::remove_file(cert_path)
        .map_err(|err| format_err!("failed to unlink {:?} - {}", cert_path, err))?;

    let key_path = configdir!("/proxy.key");
    if let Err(err) = std::fs::remove_file(key_path) {
        // Here we just log since the certificate is already gone and we'd rather try to generate
        // the self-signed certificate even if this fails:
        log::error!(
            "failed to remove certificate private key {:?} - {}",
            key_path,
            err
        );
    }

    crate::config::update_self_signed_cert(true)?;
    crate::server::reload_proxy_certificate().await?;

    Ok(())
}

#[api(
    input: {
        properties: {
            node: { schema: NODE_SCHEMA },
            force: {
                description: "Force replacement of existing files.",
                type: Boolean,
                optional: true,
                default: false,
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&["system", "certificates"], PRIV_SYS_MODIFY, false),
    },
    protected: true,
)]
/// Order a new ACME certificate.
pub fn new_acme_cert(force: bool, rpcenv: &mut dyn RpcEnvironment) -> Result<String, Error> {
    spawn_certificate_worker("acme-new-cert", force, rpcenv)
}

#[api(
    input: {
        properties: {
            node: { schema: NODE_SCHEMA },
            force: {
                description: "Force replacement of existing files.",
                type: Boolean,
                optional: true,
                default: false,
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&["system", "certificates"], PRIV_SYS_MODIFY, false),
    },
    protected: true,
)]
/// Renew the current ACME certificate if it is within its renewal lead time (or always if the
/// `force` parameter is set).
pub fn renew_acme_cert(force: bool, rpcenv: &mut dyn RpcEnvironment) -> Result<String, Error> {
    let (expires_soon, lead_days) = check_renewal_needed()?;
    if !expires_soon && !force {
        bail!(
            "Certificate does not expire within the next {lead_days} days and 'force' is not set."
        )
    }

    spawn_certificate_worker("acme-renew-cert", force, rpcenv)
}

/// Renewal lead time in seconds for the given certificate.
///
/// Long-lived certs are renewed once 2/3 of their lifetime has elapsed; short-lived ones (under
/// ten days) already at 1/2, following Let's Encrypt's integration guide. A 3-day floor still
/// applies so the daily-update service has a couple of chances to retry transient failures.
fn cert_renew_lead_time(cert: &cert::CertInfo) -> i64 {
    if let (Some(notafter), Some(notbefore)) =
        (cert.not_after_unix().ok(), cert.not_before_unix().ok())
    {
        let lifetime = notafter - notbefore;
        let scale = if lifetime < 10 * SECONDS_PER_DAY {
            2
        } else {
            3
        };
        std::cmp::max(lifetime / scale, 3 * SECONDS_PER_DAY)
    } else {
        log::warn!(
            "certificate notBefore/notAfter unavailable, falling back to 30-day renewal lead time"
        );
        30 * SECONDS_PER_DAY
    }
}

/// Check whether the current certificate expires within its renewal lead time.
///
/// Returns `(expires_soon, lead_time_in_days)`; the lead time is returned so callers can produce
/// consistent user-facing messages without re-reading and re-parsing the certificate.
pub fn check_renewal_needed() -> Result<(bool, i64), Error> {
    let cert = pem_to_cert_info(get_certificate_pem()?.as_bytes())?;
    let lead = cert_renew_lead_time(&cert);
    let expires_soon = cert
        .is_expired_after_epoch(proxmox_time::epoch_i64() + lead)
        .map_err(|err| format_err!("Failed to check certificate expiration date: {}", err))?;
    Ok((expires_soon, lead / SECONDS_PER_DAY))
}

fn spawn_certificate_worker(
    name: &'static str,
    force: bool,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<String, Error> {
    // We only have 1 certificate path in PBS which makes figuring out whether or not it is a
    // custom one too hard... We keep the parameter because the widget-toolkit may be using it...
    let _ = force;

    let (node_config, _digest) = pbs_config::node::config()?;

    let auth_id = rpcenv.get_auth_id().unwrap();

    let acme_config = node_config.acme_config()?;

    let domains = node_config.acme_domains().try_fold(
        Vec::<AcmeDomain>::new(),
        |mut acc, domain| -> Result<_, Error> {
            let mut domain = domain?;
            domain.domain.make_ascii_lowercase();
            if let Some(alias) = &mut domain.alias {
                alias.make_ascii_lowercase();
            }
            acc.push(domain);
            Ok(acc)
        },
    )?;

    WorkerTask::spawn(name, None, auth_id, true, move |worker| async move {
        let work = || async {
            if let Some(cert) =
                proxmox_acme_api::order_certificate(worker, &acme_config, &domains).await?
            {
                crate::config::set_proxy_certificate(&cert.certificate, &cert.private_key_pem)?;
                crate::server::reload_proxy_certificate().await?;
            }

            Ok(())
        };

        let res = work().await;

        send_certificate_renewal_mail(&res)?;

        res
    })
}

#[api(
    input: {
        properties: {
            node: { schema: NODE_SCHEMA },
        },
    },
    access: {
        permission: &Permission::Privilege(&["system", "certificates"], PRIV_SYS_MODIFY, false),
    },
    protected: true,
)]
/// Renew the current ACME certificate if it expires within 30 days (or always if the `force`
/// parameter is set).
pub fn revoke_acme_cert(rpcenv: &mut dyn RpcEnvironment) -> Result<String, Error> {
    let (node_config, _digest) = pbs_config::node::config()?;

    let cert_pem = get_certificate_pem()?;

    let auth_id = rpcenv.get_auth_id().unwrap();

    let acme_config = node_config.acme_config()?;

    WorkerTask::spawn(
        "acme-revoke-cert",
        None,
        auth_id,
        true,
        move |_worker| async move {
            info!("Revoking old certificate");
            proxmox_acme_api::revoke_certificate(&acme_config, cert_pem.as_bytes()).await?;
            info!("Deleting certificate and regenerating a self-signed one");
            delete_custom_certificate().await?;
            Ok(())
        },
    )
}
