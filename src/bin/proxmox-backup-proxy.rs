use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::{Arc, Mutex};

use anyhow::{bail, format_err, Context, Error};
use futures::*;
use hyper::header;
use hyper::http::request::Parts;
use hyper::http::Response;
use hyper::{Body, StatusCode};
use tracing::level_filters::LevelFilter;
use tracing::{info, warn};
use url::form_urlencoded;

use openssl::ssl::SslAcceptor;
use serde_json::{json, Value};

use proxmox_lang::try_block;
use proxmox_log::init_logger;
use proxmox_router::{RpcEnvironment, RpcEnvironmentType};
use proxmox_sys::fs::CreateOptions;
use proxmox_sys::logrotate::LogRotate;

use pbs_datastore::DataStore;

use proxmox_rest_server::{
    cleanup_old_tasks, cookie_from_header, rotate_task_log_archive, ApiConfig, Redirector,
    RestEnvironment, RestServer, WorkerTask,
};

use proxmox_backup::{
    server::{
        auth::check_pbs_auth,
        jobstate::{self, Job},
    },
    traffic_control_cache::{SharedRateLimit, TRAFFIC_CONTROL_CACHE},
};

use pbs_buildcfg::configdir;
use proxmox_time::CalendarEvent;

use pbs_api_types::{
    Authid, DataStoreConfig, Operation, PruneJobConfig, SyncJobConfig, TapeBackupJobConfig,
    VerificationJobConfig,
};

use proxmox_backup::auth_helpers::*;
use proxmox_backup::config;
use proxmox_backup::server::{self, metric_collection};
use proxmox_backup::tools::PROXMOX_BACKUP_TCP_KEEPALIVE_TIME;

use proxmox_backup::api2::tape::backup::do_tape_backup_job;
use proxmox_backup::server::do_prune_job;
use proxmox_backup::server::do_sync_job;
use proxmox_backup::server::do_verification_job;

fn main() -> Result<(), Error> {
    pbs_tools::setup_libc_malloc_opts();

    proxmox_backup::tools::setup_safe_path_env();

    let backup_uid = pbs_config::backup_user()?.uid;
    let backup_gid = pbs_config::backup_group()?.gid;
    let running_uid = nix::unistd::Uid::effective();
    let running_gid = nix::unistd::Gid::effective();

    if running_uid != backup_uid || running_gid != backup_gid {
        bail!(
            "proxy not running as backup user or group (got uid {running_uid} gid {running_gid})"
        );
    }

    proxmox_async::runtime::main(run())
}

/// check for a cookie with the user-preferred language, fallback to the config one if not set or
/// not existing
fn get_language(headers: &hyper::http::HeaderMap) -> String {
    let exists = |l: &str| Path::new(&format!("/usr/share/pbs-i18n/pbs-lang-{l}.js")).exists();

    match cookie_from_header(headers, "PBSLangCookie") {
        Some(cookie_lang) if exists(&cookie_lang) => cookie_lang,
        _ => match config::node::config().map(|(cfg, _)| cfg.default_lang) {
            Ok(Some(default_lang)) if exists(&default_lang) => default_lang,
            _ => String::from(""),
        },
    }
}

fn get_theme(headers: &hyper::http::HeaderMap) -> String {
    let exists = |t: &str| {
        t.len() < 32
            && !t.contains('/')
            && Path::new(&format!(
                "/usr/share/javascript/proxmox-widget-toolkit/themes/theme-{t}.css"
            ))
            .exists()
    };

    match cookie_from_header(headers, "PBSThemeCookie") {
        Some(theme) if theme == "crisp" => String::from(""),
        Some(theme) if exists(&theme) => theme,
        _ => String::from("auto"),
    }
}

async fn get_index_future(env: RestEnvironment, parts: Parts) -> Response<Body> {
    let auth_id = env.get_auth_id();
    let api = env.api_config();

    // fixme: make all IO async

    let (userid, csrf_token) = match auth_id {
        Some(auth_id) => {
            let auth_id = auth_id.parse::<Authid>();
            match auth_id {
                Ok(auth_id) if !auth_id.is_token() => {
                    let userid = auth_id.user().clone();
                    let new_csrf_token = assemble_csrf_prevention_token(csrf_secret(), &userid);
                    (Some(userid), Some(new_csrf_token))
                }
                _ => (None, None),
            }
        }
        None => (None, None),
    };

    let nodename = proxmox_sys::nodename();
    let user = userid.as_ref().map(|u| u.as_str()).unwrap_or("");

    let csrf_token = csrf_token.unwrap_or_else(|| String::from(""));

    let mut debug = false;
    let mut template_file = "index";

    if let Some(query_str) = parts.uri.query() {
        for (k, v) in form_urlencoded::parse(query_str.as_bytes()).into_owned() {
            if k == "debug" && v != "0" && v != "false" {
                debug = true;
            } else if k == "console" {
                template_file = "console";
            }
        }
    }

    let theme = get_theme(&parts.headers);

    let consent = config::node::config()
        .ok()
        .and_then(|config| config.0.consent_text)
        .unwrap_or("".to_string());
    let data = json!({
        "NodeName": nodename,
        "UserName": user,
        "CSRFPreventionToken": csrf_token,
        "language": get_language(&parts.headers),
        "theme": theme,
        "auto": theme == "auto",
        "debug": debug,
        "consentText": consent,
    });

    let (ct, index) = match api.render_template(template_file, &data) {
        Ok(index) => ("text/html", index),
        Err(err) => ("text/plain", format!("Error rendering template: {err}")),
    };

    let mut resp = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, ct)
        .body(index.into())
        .unwrap();

    if let Some(userid) = userid {
        resp.extensions_mut().insert(Authid::from((userid, None)));
    }

    resp
}

async fn run() -> Result<(), Error> {
    init_logger("PBS_LOG", LevelFilter::INFO)?;

    proxmox_backup::auth_helpers::setup_auth_context(false);
    proxmox_backup::server::notifications::init()?;
    metric_collection::init()?;

    let mut indexpath = PathBuf::from(pbs_buildcfg::JS_DIR);
    indexpath.push("index.hbs");

    let mut config = ApiConfig::new(pbs_buildcfg::JS_DIR, RpcEnvironmentType::PUBLIC)
        .index_handler_func(|e, p| Box::pin(get_index_future(e, p)))
        .auth_handler_func(|h, m| Box::pin(check_pbs_auth(h, m)))
        .register_template("index", &indexpath)?
        .register_template("console", "/usr/share/pve-xtermjs/index.html.hbs")?
        .default_api2_handler(&proxmox_backup::api2::ROUTER)
        .aliases([
            ("novnc", "/usr/share/novnc-pve"),
            ("extjs", "/usr/share/javascript/extjs"),
            ("qrcodejs", "/usr/share/javascript/qrcodejs"),
            ("fontawesome", "/usr/share/fonts-font-awesome"),
            ("xtermjs", "/usr/share/pve-xtermjs"),
            ("locale", "/usr/share/pbs-i18n"),
            (
                "widgettoolkit",
                "/usr/share/javascript/proxmox-widget-toolkit",
            ),
            ("docs", "/usr/share/doc/proxmox-backup/html"),
        ]);

    let backup_user = pbs_config::backup_user()?;
    let mut command_sock = proxmox_daemon::command_socket::CommandSocket::new(backup_user.gid);

    let dir_opts = CreateOptions::new()
        .owner(backup_user.uid)
        .group(backup_user.gid);
    let file_opts = CreateOptions::new()
        .owner(backup_user.uid)
        .group(backup_user.gid);

    config = config
        .enable_access_log(
            pbs_buildcfg::API_ACCESS_LOG_FN,
            Some(dir_opts.clone()),
            Some(file_opts.clone()),
            &mut command_sock,
        )?
        .enable_auth_log(
            pbs_buildcfg::API_AUTH_LOG_FN,
            Some(dir_opts.clone()),
            Some(file_opts.clone()),
            &mut command_sock,
        )?;

    let rest_server = RestServer::new(config);
    let redirector = Redirector::new();
    proxmox_rest_server::init_worker_tasks(
        pbs_buildcfg::PROXMOX_BACKUP_LOG_DIR_M!().into(),
        file_opts.clone(),
    )?;

    //openssl req -x509 -newkey rsa:4096 -keyout /etc/proxmox-backup/proxy.key -out /etc/proxmox-backup/proxy.pem -nodes

    // we build the initial acceptor here as we cannot start if this fails
    let acceptor = make_tls_acceptor()?;
    let acceptor = Arc::new(Mutex::new(acceptor));

    // to renew the acceptor we just add a command-socket handler
    command_sock.register_command("reload-certificate".to_string(), {
        let acceptor = Arc::clone(&acceptor);
        move |_value| -> Result<_, Error> {
            log::info!("reloading certificate");
            match make_tls_acceptor() {
                Err(err) => log::error!("error reloading certificate: {err}"),
                Ok(new_acceptor) => {
                    let mut guard = acceptor.lock().unwrap();
                    *guard = new_acceptor;
                }
            }
            Ok(Value::Null)
        }
    })?;

    // to remove references for not configured datastores
    command_sock.register_command("datastore-removed".to_string(), |_value| {
        if let Err(err) = DataStore::remove_unused_datastores() {
            log::error!("could not refresh datastores: {err}");
        }
        Ok(Value::Null)
    })?;

    // clear cache entry for datastore that is in a specific maintenance mode
    command_sock.register_command("update-datastore-cache".to_string(), |value| {
        if let Some(name) = value.and_then(Value::as_str) {
            if let Err(err) = DataStore::update_datastore_cache(name) {
                log::error!("could not trigger update datastore cache: {err}");
            }
        }
        Ok(Value::Null)
    })?;

    let connections = proxmox_rest_server::connection::AcceptBuilder::new()
        .debug(tracing::enabled!(tracing::Level::DEBUG))
        .rate_limiter_lookup(Arc::new(lookup_rate_limiter))
        .tcp_keepalive_time(PROXMOX_BACKUP_TCP_KEEPALIVE_TIME);

    let server = proxmox_daemon::server::create_daemon(
        ([0, 0, 0, 0, 0, 0, 0, 0], 8007).into(),
        move |listener| {
            let (secure_connections, insecure_connections) =
                connections.accept_tls_optional(listener, acceptor);

            Ok(async {
                proxmox_systemd::notify::SystemdNotify::Ready.notify()?;

                let secure_server = hyper::Server::builder(secure_connections)
                    .serve(rest_server)
                    .with_graceful_shutdown(proxmox_daemon::shutdown_future())
                    .map_err(Error::from);

                let insecure_server = hyper::Server::builder(insecure_connections)
                    .serve(redirector)
                    .with_graceful_shutdown(proxmox_daemon::shutdown_future())
                    .map_err(Error::from);

                let (secure_res, insecure_res) =
                    try_join!(tokio::spawn(secure_server), tokio::spawn(insecure_server))
                        .context("failed to complete REST server task")?;

                let results = [secure_res, insecure_res];

                if results.iter().any(Result::is_err) {
                    let cat_errors = results
                        .into_iter()
                        .filter_map(|res| res.err().map(|err| err.to_string()))
                        .collect::<Vec<_>>()
                        .join("\n");

                    bail!(cat_errors);
                }

                Ok(())
            })
        },
        Some(pbs_buildcfg::PROXMOX_BACKUP_PROXY_PID_FN),
    );

    proxmox_rest_server::write_pid(pbs_buildcfg::PROXMOX_BACKUP_PROXY_PID_FN)?;

    let init_result: Result<(), Error> = try_block!({
        proxmox_rest_server::register_task_control_commands(&mut command_sock)?;
        command_sock.spawn(proxmox_rest_server::last_worker_future())?;
        proxmox_daemon::catch_shutdown_signal(proxmox_rest_server::last_worker_future())?;
        proxmox_daemon::catch_reload_signal(proxmox_rest_server::last_worker_future())?;
        Ok(())
    });

    if let Err(err) = init_result {
        bail!("unable to start daemon - {err}");
    }

    // stop gap for https://github.com/tokio-rs/tokio/issues/4730 where the thread holding the
    // IO-driver may block progress completely if it starts polling its own tasks (blocks).
    // So, trigger a notify to parked threads, as we're immediately ready the woken up thread will
    // acquire the IO driver, if blocked, before going to sleep, which allows progress again
    // TODO: remove once tokio solves this at their level (see proposals in linked comments)
    let rt_handle = tokio::runtime::Handle::current();
    std::thread::spawn(move || loop {
        rt_handle.spawn(std::future::ready(()));
        std::thread::sleep(Duration::from_secs(3));
    });

    start_task_scheduler();
    metric_collection::start_collection_task();
    start_traffic_control_updater();

    server.await?;
    log::info!("server shutting down, waiting for active workers to complete");
    proxmox_rest_server::last_worker_future().await;
    log::info!("done - exit server");

    Ok(())
}

fn make_tls_acceptor() -> Result<SslAcceptor, Error> {
    let key_path = configdir!("/proxy.key");
    let cert_path = configdir!("/proxy.pem");

    let (config, _) = config::node::config()?;
    let ciphers_tls_1_3 = config.ciphers_tls_1_3;
    let ciphers_tls_1_2 = config.ciphers_tls_1_2;

    let mut acceptor = proxmox_rest_server::connection::TlsAcceptorBuilder::new()
        .certificate_paths_pem(key_path, cert_path);

    //let mut acceptor = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls()).unwrap();
    if let Some(ciphers) = ciphers_tls_1_3.as_deref() {
        acceptor = acceptor.cipher_suites(ciphers.to_string());
    }
    if let Some(ciphers) = ciphers_tls_1_2.as_deref() {
        acceptor = acceptor.cipher_list(ciphers.to_string());
    }

    acceptor.build()
}

fn start_task_scheduler() {
    tokio::spawn(async {
        let abort_future = pin!(proxmox_daemon::shutdown_future());
        let future = pin!(run_task_scheduler());
        futures::future::select(future, abort_future).await;
    });
}

fn start_traffic_control_updater() {
    tokio::spawn(async {
        let abort_future = pin!(proxmox_daemon::shutdown_future());
        let future = pin!(run_traffic_control_updater());
        futures::future::select(future, abort_future).await;
    });
}

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn next_minute() -> Instant {
    let now = SystemTime::now();
    let epoch_now = match now.duration_since(UNIX_EPOCH) {
        Ok(d) => d,
        Err(err) => {
            eprintln!("task scheduler: compute next minute alignment failed - {err}");
            return Instant::now() + Duration::from_secs(60);
        }
    };
    let epoch_next = Duration::from_secs((epoch_now.as_secs() / 60 + 1) * 60);
    Instant::now() + epoch_next - epoch_now
}

async fn run_task_scheduler() {
    loop {
        // sleep first to align to next minute boundary for first round
        let delay_target = next_minute();
        tokio::time::sleep_until(tokio::time::Instant::from_std(delay_target)).await;

        match schedule_tasks().catch_unwind().await {
            Err(panic) => match panic.downcast::<&str>() {
                Ok(msg) => eprintln!("task scheduler panic: {msg}"),
                Err(_) => eprintln!("task scheduler panic - unknown type"),
            },
            Ok(Err(err)) => eprintln!("task scheduler failed - {err:?}"),
            Ok(Ok(_)) => {}
        }
    }
}

async fn schedule_tasks() -> Result<(), Error> {
    schedule_datastore_garbage_collection().await;
    schedule_datastore_prune_jobs().await;
    schedule_datastore_sync_jobs().await;
    schedule_datastore_verify_jobs().await;
    schedule_tape_backup_jobs().await;
    schedule_task_log_rotate().await;

    Ok(())
}

async fn schedule_datastore_garbage_collection() {
    let config = match pbs_config::datastore::config() {
        Err(err) => {
            eprintln!("unable to read datastore config - {err}");
            return;
        }
        Ok((config, _digest)) => config,
    };

    for (store, (_, store_config)) in config.sections {
        let store_config: DataStoreConfig = match serde_json::from_value(store_config) {
            Ok(c) => c,
            Err(err) => {
                eprintln!("datastore config from_value failed - {err}");
                continue;
            }
        };

        let event_str = match store_config.gc_schedule {
            Some(event_str) => event_str,
            None => continue,
        };

        let event: CalendarEvent = match event_str.parse() {
            Ok(event) => event,
            Err(err) => {
                eprintln!("unable to parse schedule '{event_str}' - {err}");
                continue;
            }
        };

        {
            // limit datastore scope due to Op::Lookup
            let datastore = match DataStore::lookup_datastore(&store, Some(Operation::Lookup)) {
                Ok(datastore) => datastore,
                Err(err) => {
                    eprintln!("lookup_datastore failed - {err}");
                    continue;
                }
            };

            if datastore.garbage_collection_running() {
                continue;
            }
        }

        let worker_type = "garbage_collection";

        let last = match jobstate::last_run_time(worker_type, &store) {
            Ok(time) => time,
            Err(err) => {
                eprintln!("could not get last run time of {worker_type} {store}: {err}");
                continue;
            }
        };

        let next = match event.compute_next_event(last) {
            Ok(Some(next)) => next,
            Ok(None) => continue,
            Err(err) => {
                eprintln!("compute_next_event for '{event_str}' failed - {err}");
                continue;
            }
        };

        let now = proxmox_time::epoch_i64();

        if next > now {
            continue;
        }

        let job = match Job::new(worker_type, &store) {
            Ok(job) => job,
            Err(_) => continue, // could not get lock
        };

        let datastore = match DataStore::lookup_datastore(&store, Some(Operation::Write)) {
            Ok(datastore) => datastore,
            Err(err) => {
                log::warn!("skipping scheduled GC on {store}, could look it up - {err}");
                continue;
            }
        };

        let auth_id = Authid::root_auth_id();

        if let Err(err) = crate::server::do_garbage_collection_job(
            job,
            datastore,
            auth_id,
            Some(event_str),
            false,
        ) {
            eprintln!("unable to start garbage collection job on datastore {store} - {err}");
        }
    }
}

async fn schedule_datastore_prune_jobs() {
    let config = match pbs_config::prune::config() {
        Err(err) => {
            eprintln!("unable to read prune job config - {err}");
            return;
        }
        Ok((config, _digest)) => config,
    };
    for (job_id, (_, job_config)) in config.sections {
        let job_config: PruneJobConfig = match serde_json::from_value(job_config) {
            Ok(c) => c,
            Err(err) => {
                eprintln!("prune job config from_value failed - {err}");
                continue;
            }
        };

        if job_config.disable {
            continue;
        }

        if !job_config.options.keeps_something() {
            continue; // no 'keep' values set, keep all
        }

        let worker_type = "prunejob";
        let auth_id = Authid::root_auth_id().clone();
        if check_schedule(worker_type, &job_config.schedule, &job_id) {
            let job = match Job::new(worker_type, &job_id) {
                Ok(job) => job,
                Err(_) => continue, // could not get lock
            };
            if let Err(err) = do_prune_job(
                job,
                job_config.options,
                job_config.store,
                &auth_id,
                Some(job_config.schedule),
            ) {
                eprintln!("unable to start datastore prune job {job_id} - {err}");
            }
        };
    }
}

async fn schedule_datastore_sync_jobs() {
    let config = match pbs_config::sync::config() {
        Err(err) => {
            eprintln!("unable to read sync job config - {err}");
            return;
        }
        Ok((config, _digest)) => config,
    };

    for (job_id, (_, job_config)) in config.sections {
        let job_config: SyncJobConfig = match serde_json::from_value(job_config) {
            Ok(c) => c,
            Err(err) => {
                eprintln!("sync job config from_value failed - {err}");
                continue;
            }
        };

        let event_str = match job_config.schedule {
            Some(ref event_str) => event_str.clone(),
            None => continue,
        };

        let worker_type = "syncjob";
        if check_schedule(worker_type, &event_str, &job_id) {
            let job = match Job::new(worker_type, &job_id) {
                Ok(job) => job,
                Err(_) => continue, // could not get lock
            };

            let auth_id = Authid::root_auth_id().clone();
            if let Err(err) = do_sync_job(job, job_config, &auth_id, Some(event_str), false) {
                eprintln!("unable to start datastore sync job {job_id} - {err}");
            }
        };
    }
}

async fn schedule_datastore_verify_jobs() {
    let config = match pbs_config::verify::config() {
        Err(err) => {
            eprintln!("unable to read verification job config - {err}");
            return;
        }
        Ok((config, _digest)) => config,
    };
    for (job_id, (_, job_config)) in config.sections {
        let job_config: VerificationJobConfig = match serde_json::from_value(job_config) {
            Ok(c) => c,
            Err(err) => {
                eprintln!("verification job config from_value failed - {err}");
                continue;
            }
        };
        let event_str = match job_config.schedule {
            Some(ref event_str) => event_str.clone(),
            None => continue,
        };

        let worker_type = "verificationjob";
        let auth_id = Authid::root_auth_id().clone();
        if check_schedule(worker_type, &event_str, &job_id) {
            let job = match Job::new(worker_type, &job_id) {
                Ok(job) => job,
                Err(_) => continue, // could not get lock
            };
            if let Err(err) = do_verification_job(job, job_config, &auth_id, Some(event_str), false)
            {
                eprintln!("unable to start datastore verification job {job_id} - {err}");
            }
        };
    }
}

async fn schedule_tape_backup_jobs() {
    let config = match pbs_config::tape_job::config() {
        Err(err) => {
            eprintln!("unable to read tape job config - {err}");
            return;
        }
        Ok((config, _digest)) => config,
    };
    for (job_id, (_, job_config)) in config.sections {
        let job_config: TapeBackupJobConfig = match serde_json::from_value(job_config) {
            Ok(c) => c,
            Err(err) => {
                eprintln!("tape backup job config from_value failed - {err}");
                continue;
            }
        };
        let event_str = match job_config.schedule {
            Some(ref event_str) => event_str.clone(),
            None => continue,
        };

        let worker_type = "tape-backup-job";
        let auth_id = Authid::root_auth_id().clone();
        if check_schedule(worker_type, &event_str, &job_id) {
            let job = match Job::new(worker_type, &job_id) {
                Ok(job) => job,
                Err(_) => continue, // could not get lock
            };
            if let Err(err) =
                do_tape_backup_job(job, job_config.setup, &auth_id, Some(event_str), false)
            {
                eprintln!("unable to start tape backup job {job_id} - {err}");
            }
        };
    }
}

async fn schedule_task_log_rotate() {
    let worker_type = "logrotate";
    let job_id = "access-log_and_task-archive";

    // schedule daily at 00:00 like normal logrotate
    let schedule = "00:00";

    if !check_schedule(worker_type, schedule, job_id) {
        // if we never ran the rotation, schedule instantly
        match jobstate::JobState::load(worker_type, job_id) {
            Ok(jobstate::JobState::Created { .. }) => {}
            _ => return,
        }
    }

    let mut job = match Job::new(worker_type, job_id) {
        Ok(job) => job,
        Err(_) => return, // could not get lock
    };

    if let Err(err) = WorkerTask::new_thread(
        worker_type,
        None,
        Authid::root_auth_id().to_string(),
        false,
        move |worker| {
            job.start(&worker.upid().to_string())?;
            info!("starting task log rotation");

            let result = try_block!({
                let max_size = 512 * 1024 - 1; // an entry has ~ 100b, so > 5000 entries/file
                let max_files = 20; // times twenty files gives > 100000 task entries

                let max_days = proxmox_backup::config::node::config()
                    .map(|(cfg, _)| cfg.task_log_max_days)
                    .ok()
                    .flatten();

                let user = pbs_config::backup_user()?;
                let options = proxmox_sys::fs::CreateOptions::new()
                    .owner(user.uid)
                    .group(user.gid);

                let has_rotated = rotate_task_log_archive(
                    max_size,
                    true,
                    Some(max_files),
                    max_days,
                    Some(options.clone()),
                )?;

                if has_rotated {
                    info!("task log archive was rotated");
                } else {
                    info!("task log archive was not rotated");
                }

                let max_size = 32 * 1024 * 1024 - 1;
                let max_files = 14;

                let mut logrotate = LogRotate::new(
                    pbs_buildcfg::API_ACCESS_LOG_FN,
                    true,
                    Some(max_files),
                    Some(options.clone()),
                )?;

                if logrotate.rotate(max_size)? {
                    println!("rotated access log, telling daemons to re-open log file");
                    proxmox_async::runtime::block_on(command_reopen_access_logfiles())?;
                    info!("API access log was rotated");
                } else {
                    info!("API access log was not rotated");
                }

                let mut logrotate = LogRotate::new(
                    pbs_buildcfg::API_AUTH_LOG_FN,
                    true,
                    Some(max_files),
                    Some(options),
                )?;

                if logrotate.rotate(max_size)? {
                    println!("rotated auth log, telling daemons to re-open log file");
                    proxmox_async::runtime::block_on(command_reopen_auth_logfiles())?;
                    info!("API authentication log was rotated");
                } else {
                    info!("API authentication log was not rotated");
                }

                if has_rotated {
                    info!("cleaning up old task logs");
                    if let Err(err) = cleanup_old_tasks(true) {
                        warn!("could not completely cleanup old tasks: {err}");
                    }
                }

                Ok(())
            });

            let status = worker.create_state(&result);

            if let Err(err) = job.finish(status) {
                eprintln!("could not finish job state for {worker_type}: {err}");
            }

            result
        },
    ) {
        eprintln!("unable to start task log rotation: {err}");
    }
}

async fn command_reopen_access_logfiles() -> Result<(), Error> {
    // only care about the most recent daemon instance for each, proxy & api, as other older ones
    // should not respond to new requests anyway, but only finish their current one and then exit.
    let sock = proxmox_daemon::command_socket::this_path();
    let f1 =
        proxmox_daemon::command_socket::send_raw(sock, "{\"command\":\"api-access-log-reopen\"}\n");

    let pid = proxmox_rest_server::read_pid(pbs_buildcfg::PROXMOX_BACKUP_API_PID_FN)?;
    let sock = proxmox_daemon::command_socket::path_from_pid(pid);
    let f2 =
        proxmox_daemon::command_socket::send_raw(sock, "{\"command\":\"api-access-log-reopen\"}\n");

    match futures::join!(f1, f2) {
        (Err(e1), Err(e2)) => Err(format_err!(
            "reopen commands failed, proxy: {e1}; api: {e2}"
        )),
        (Err(e1), Ok(_)) => Err(format_err!("reopen commands failed, proxy: {e1}")),
        (Ok(_), Err(e2)) => Err(format_err!("reopen commands failed, api: {e2}")),
        _ => Ok(()),
    }
}

async fn command_reopen_auth_logfiles() -> Result<(), Error> {
    // only care about the most recent daemon instance for each, proxy & api, as other older ones
    // should not respond to new requests anyway, but only finish their current one and then exit.
    let sock = proxmox_daemon::command_socket::this_path();
    let f1 =
        proxmox_daemon::command_socket::send_raw(sock, "{\"command\":\"api-auth-log-reopen\"}\n");

    let pid = proxmox_rest_server::read_pid(pbs_buildcfg::PROXMOX_BACKUP_API_PID_FN)?;
    let sock = proxmox_daemon::command_socket::path_from_pid(pid);
    let f2 =
        proxmox_daemon::command_socket::send_raw(sock, "{\"command\":\"api-auth-log-reopen\"}\n");

    match futures::join!(f1, f2) {
        (Err(e1), Err(e2)) => Err(format_err!(
            "reopen commands failed, proxy: {e1}; api: {e2}"
        )),
        (Err(e1), Ok(_)) => Err(format_err!("reopen commands failed, proxy: {e1}")),
        (Ok(_), Err(e2)) => Err(format_err!("reopen commands failed, api: {e2}")),
        _ => Ok(()),
    }
}

fn check_schedule(worker_type: &str, event_str: &str, id: &str) -> bool {
    let event: CalendarEvent = match event_str.parse() {
        Ok(event) => event,
        Err(err) => {
            eprintln!("unable to parse schedule '{event_str}' - {err}");
            return false;
        }
    };

    let last = match jobstate::last_run_time(worker_type, id) {
        Ok(time) => time,
        Err(err) => {
            eprintln!("could not get last run time of {worker_type} {id}: {err}");
            return false;
        }
    };

    let next = match event.compute_next_event(last) {
        Ok(Some(next)) => next,
        Ok(None) => return false,
        Err(err) => {
            eprintln!("compute_next_event for '{event_str}' failed - {err}");
            return false;
        }
    };

    let now = proxmox_time::epoch_i64();
    next <= now
}

// Rate Limiter lookup
async fn run_traffic_control_updater() {
    loop {
        let delay_target = Instant::now() + Duration::from_secs(1);

        {
            let mut cache = TRAFFIC_CONTROL_CACHE.lock().unwrap();
            cache.compute_current_rates();
        }

        tokio::time::sleep_until(tokio::time::Instant::from_std(delay_target)).await;
    }
}

fn lookup_rate_limiter(
    peer: std::net::SocketAddr,
) -> (Option<SharedRateLimit>, Option<SharedRateLimit>) {
    let mut cache = TRAFFIC_CONTROL_CACHE.lock().unwrap();

    let now = proxmox_time::epoch_i64();

    cache.reload(now);

    let (_rule_name, read_limiter, write_limiter) = cache.lookup_rate_limiter(peer, now);

    (read_limiter, write_limiter)
}
