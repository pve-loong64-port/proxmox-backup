use pbs_api_types::Authid;

pub fn term_aad(auth_id: &Authid, path: &str, port: u16) -> String {
    format!("{auth_id}{path}{port}")
}
