//! Raw pkt-line wire helpers for tests that speak the smart-HTTP protocol
//! directly (framing a `fetch` command, POSTing it, and decoding the packed
//! response) instead of driving the `git` CLI.

#![allow(dead_code)]

use std::collections::HashSet;

use super::*;

/// Pkt-line delimiter (`0001`), separating a v2 command from its arguments.
pub const DELIM: &[u8] = b"0001";
/// Pkt-line flush (`0000`), ending a section.
pub const FLUSH: &[u8] = b"0000";

/// Build a data pkt-line: a 4-hex length prefix (covering itself) then the
/// payload verbatim.
pub fn pkt(data: &[u8]) -> Vec<u8> {
    let mut out = format!("{:04x}", data.len() + 4).into_bytes();
    out.extend_from_slice(data);
    out
}

/// Frame a protocol-v2 `fetch` command request: the command, the
/// `object-format` capability, a delimiter, then one pkt-line per argument,
/// then a flush.
pub fn fetch_body(args: &[&str]) -> Vec<u8> {
    let mut body = pkt(b"command=fetch\n");
    body.extend(pkt(b"object-format=sha1\n"));
    body.extend_from_slice(DELIM);
    for arg in args {
        body.extend(pkt(format!("{arg}\n").as_bytes()));
    }
    body.extend_from_slice(FLUSH);
    body
}

/// POST a protocol-v2 command request to a repo's upload-pack endpoint.
pub async fn post_upload_pack(base_url: &str, repo: &str, body: Vec<u8>) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{base_url}/{repo}/git-upload-pack"))
        .header("Git-Protocol", "version=2")
        .body(body)
        .send()
        .await
        .expect("send request")
}

/// One decoded pkt-line of a response body.
#[derive(Debug, PartialEq, Eq)]
pub enum Pkt {
    Flush,
    Delim,
    Data(Vec<u8>),
}

/// Split a raw pkt-line stream into its packets.
pub fn parse_pkts(mut bytes: &[u8]) -> Vec<Pkt> {
    let mut pkts = Vec::new();
    while !bytes.is_empty() {
        let len_hex = std::str::from_utf8(&bytes[..4]).expect("pkt length prefix");
        let len = usize::from_str_radix(len_hex, 16).expect("hex pkt length");
        match len {
            0 => {
                pkts.push(Pkt::Flush);
                bytes = &bytes[4..];
            }
            1 => {
                pkts.push(Pkt::Delim);
                bytes = &bytes[4..];
            }
            _ => {
                pkts.push(Pkt::Data(bytes[4..len].to_vec()));
                bytes = &bytes[len..];
            }
        }
    }
    pkts
}

/// Pull the raw pack bytes out of a v2 fetch response: the side-band channel-1
/// payload of every packet after the `packfile` section header (channels 2
/// and 3 carry progress and errors and are dropped).
pub fn extract_pack(pkts: &[Pkt]) -> Vec<u8> {
    let mut pack = Vec::new();
    let mut in_packfile = false;
    for p in pkts {
        match p {
            Pkt::Data(data) if data.as_slice() == b"packfile\n" => in_packfile = true,
            Pkt::Data(data) if in_packfile && data.first() == Some(&1) => {
                pack.extend_from_slice(&data[1..]);
            }
            _ => {}
        }
    }
    pack
}

/// The object count a pack's v2 header declares (bytes 8..12, big-endian).
pub fn pack_object_count(pack: &[u8]) -> u32 {
    assert!(pack.starts_with(b"PACK"), "not a pack: {:?}", pack.get(..4));
    u32::from_be_bytes(pack[8..12].try_into().expect("pack count field"))
}

/// Unpack `pack` into a fresh repository under `server`'s tempdir and return
/// the hex ids of the objects it contained. `unpack-objects` performs no
/// connectivity check, so a pack whose objects reference absent ones unpacks
/// fine.
pub fn unpack_oids(server: &TestServer, pack: &[u8], name: &str) -> HashSet<String> {
    let dir = server.tempdir().join(name);
    init_repo(&dir);
    git_ok_with_input(&dir, &["unpack-objects", "-q"], pack);
    let listing = git_ok(
        &dir,
        &[
            "cat-file",
            "--batch-all-objects",
            "--batch-check=%(objectname)",
        ],
    );
    String::from_utf8(listing)
        .expect("utf-8 object listing")
        .lines()
        .map(|line| line.trim().to_owned())
        .filter(|line| !line.is_empty())
        .collect()
}
