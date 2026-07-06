//! Storage key encoding and decoding.
//!
//! Every key in every segment begins with the same fixed-width preamble
//! (`version | repo_id | segment`) followed by a segment-specific suffix. See
//! `docs/0001-init.md` §Storage (Key Preamble and the per-segment Key Layout
//! subsections) for the exact layout.

use gix_hash::ObjectId;

/// The current key format version.
pub const KEY_VERSION: u8 = 1;

/// Length in bytes of the fixed `version | repo_id | segment` preamble.
const PREAMBLE_LEN: usize = 1 + 8 + 1;

/// Opaque, fixed-width repository id assigned at repo creation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RepoId(pub u64);

impl RepoId {
    /// Reserved id for server-global metadata, readable before any specific
    /// repository has been resolved.
    pub const GLOBAL: RepoId = RepoId(0);
}

/// The four SlateDB segments every repository is namespaced into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Segment {
    /// Server-global and per-repo settings.
    Meta = 0x00,
    /// Git objects (blobs, trees, commits, tags).
    Object = 0x01,
    /// References (branches, tags, `HEAD`).
    Ref = 0x02,
    /// Derived, rebuildable commit ancestry metadata.
    CommitGraph = 0x03,
}

impl Segment {
    /// The single byte identifying this segment in a key preamble.
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

impl TryFrom<u8> for Segment {
    type Error = KeyError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x00 => Ok(Segment::Meta),
            0x01 => Ok(Segment::Object),
            0x02 => Ok(Segment::Ref),
            0x03 => Ok(Segment::CommitGraph),
            other => Err(KeyError::UnknownSegment(other)),
        }
    }
}

/// Errors returned while decoding a storage key.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum KeyError {
    /// The key was shorter than the fixed preamble.
    #[error("key too short: expected at least {expected} bytes, got {actual}")]
    TooShort {
        /// Minimum number of bytes required.
        expected: usize,
        /// Number of bytes actually present.
        actual: usize,
    },
    /// The key's version byte did not match [`KEY_VERSION`].
    #[error("unsupported key version: {0}")]
    UnsupportedVersion(u8),
    /// The key's segment byte did not match a known [`Segment`].
    #[error("unknown segment byte: {0:#04x}")]
    UnknownSegment(u8),
    /// The key's segment did not match the segment expected by the decoder
    /// that was called.
    #[error("unexpected segment: expected {expected:?}, got {actual:?}")]
    WrongSegment {
        /// The segment the caller expected.
        expected: Segment,
        /// The segment actually encoded in the key.
        actual: Segment,
    },
    /// A UTF-8 segment-specific suffix (meta key or ref name) was not valid
    /// UTF-8.
    #[error("invalid utf-8 in key")]
    InvalidUtf8,
    /// An object/commit-graph key's suffix was not exactly as long as the
    /// configured SHA width.
    #[error("invalid object id length: expected {expected}, got {actual}")]
    InvalidObjectIdLen {
        /// Expected length in bytes (20 for SHA-1, 32 for SHA-256).
        expected: usize,
        /// Length actually present.
        actual: usize,
    },
}

fn encode_preamble(buf: &mut Vec<u8>, repo: RepoId, segment: Segment) {
    buf.push(KEY_VERSION);
    buf.extend_from_slice(&repo.0.to_be_bytes());
    buf.push(segment.as_u8());
}

/// Split a key into its preamble fields and segment-specific suffix.
fn decode_preamble(bytes: &[u8]) -> Result<(RepoId, Segment, &[u8]), KeyError> {
    if bytes.len() < PREAMBLE_LEN {
        return Err(KeyError::TooShort {
            expected: PREAMBLE_LEN,
            actual: bytes.len(),
        });
    }
    let version = bytes[0];
    if version != KEY_VERSION {
        return Err(KeyError::UnsupportedVersion(version));
    }
    let repo_id = u64::from_be_bytes([
        bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8],
    ]);
    let segment = Segment::try_from(bytes[9])?;
    Ok((RepoId(repo_id), segment, &bytes[PREAMBLE_LEN..]))
}

fn expect_segment(actual: Segment, expected: Segment) -> Result<(), KeyError> {
    if actual == expected {
        Ok(())
    } else {
        Err(KeyError::WrongSegment { expected, actual })
    }
}

fn decode_oid(bytes: &[u8], hash_kind: gix_hash::Kind) -> Result<ObjectId, KeyError> {
    let expected = hash_kind.len_in_bytes();
    if bytes.len() != expected {
        return Err(KeyError::InvalidObjectIdLen {
            expected,
            actual: bytes.len(),
        });
    }
    ObjectId::try_from(bytes).map_err(|_| KeyError::InvalidObjectIdLen {
        expected,
        actual: bytes.len(),
    })
}

/// Build a metadata-segment key for `meta_key` scoped to `repo` (use
/// [`RepoId::GLOBAL`] for server-global metadata).
pub fn meta_key(repo: RepoId, meta_key: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(PREAMBLE_LEN + meta_key.len());
    encode_preamble(&mut buf, repo, Segment::Meta);
    buf.extend_from_slice(meta_key.as_bytes());
    buf
}

/// Decode a metadata-segment key back into its repo id and meta key name.
pub fn decode_meta_key(bytes: &[u8]) -> Result<(RepoId, String), KeyError> {
    let (repo, segment, rest) = decode_preamble(bytes)?;
    expect_segment(segment, Segment::Meta)?;
    let name = std::str::from_utf8(rest)
        .map_err(|_| KeyError::InvalidUtf8)?
        .to_owned();
    Ok((repo, name))
}

/// Build an object-segment key for `oid` scoped to `repo`.
pub fn object_key(repo: RepoId, oid: &gix_hash::oid) -> Vec<u8> {
    let mut buf = Vec::with_capacity(PREAMBLE_LEN + oid.as_bytes().len());
    encode_preamble(&mut buf, repo, Segment::Object);
    buf.extend_from_slice(oid.as_bytes());
    buf
}

/// Decode an object-segment key back into its repo id and object id.
/// `hash_kind` must be the repo's configured SHA width (resolved from the
/// repo's `object-format` metadata, per §Key Preamble's resolution order).
pub fn decode_object_key(
    bytes: &[u8],
    hash_kind: gix_hash::Kind,
) -> Result<(RepoId, ObjectId), KeyError> {
    let (repo, segment, rest) = decode_preamble(bytes)?;
    expect_segment(segment, Segment::Object)?;
    let oid = decode_oid(rest, hash_kind)?;
    Ok((repo, oid))
}

/// Build a ref-segment key for `name` scoped to `repo`.
pub fn ref_key(repo: RepoId, name: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(PREAMBLE_LEN + name.len());
    encode_preamble(&mut buf, repo, Segment::Ref);
    buf.extend_from_slice(name.as_bytes());
    buf
}

/// Decode a ref-segment key back into its repo id and ref name.
pub fn decode_ref_key(bytes: &[u8]) -> Result<(RepoId, String), KeyError> {
    let (repo, segment, rest) = decode_preamble(bytes)?;
    expect_segment(segment, Segment::Ref)?;
    let name = std::str::from_utf8(rest)
        .map_err(|_| KeyError::InvalidUtf8)?
        .to_owned();
    Ok((repo, name))
}

/// Build a commit-graph-segment key for `oid` scoped to `repo`.
pub fn commit_key(repo: RepoId, oid: &gix_hash::oid) -> Vec<u8> {
    let mut buf = Vec::with_capacity(PREAMBLE_LEN + oid.as_bytes().len());
    encode_preamble(&mut buf, repo, Segment::CommitGraph);
    buf.extend_from_slice(oid.as_bytes());
    buf
}

/// Decode a commit-graph-segment key back into its repo id and commit id.
/// `hash_kind` must be the repo's configured SHA width.
pub fn decode_commit_key(
    bytes: &[u8],
    hash_kind: gix_hash::Kind,
) -> Result<(RepoId, ObjectId), KeyError> {
    let (repo, segment, rest) = decode_preamble(bytes)?;
    expect_segment(segment, Segment::CommitGraph)?;
    let oid = decode_oid(rest, hash_kind)?;
    Ok((repo, oid))
}

/// Build the shared prefix of every key in `segment` for `repo`, for use in
/// full-segment prefix scans.
pub fn segment_prefix(repo: RepoId, segment: Segment) -> Vec<u8> {
    let mut buf = Vec::with_capacity(PREAMBLE_LEN);
    encode_preamble(&mut buf, repo, segment);
    buf
}

/// Build a prefix covering every ref key in `repo` whose name starts with
/// `utf8_prefix`, for use in ref-name prefix scans (e.g. `refs/heads/`).
pub fn ref_prefix(repo: RepoId, utf8_prefix: &str) -> Vec<u8> {
    let mut buf = segment_prefix(repo, Segment::Ref);
    buf.extend_from_slice(utf8_prefix.as_bytes());
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sha1_oid() -> ObjectId {
        // sha1 of the empty blob: e69de29bb2d1d6434b8b29ae775ad8c2e48c5391
        ObjectId::from_hex(b"e69de29bb2d1d6434b8b29ae775ad8c2e48c5391").expect("valid hex")
    }

    fn sha256_oid() -> ObjectId {
        // sha256 of the empty blob.
        ObjectId::from_hex(b"473a0f4c3be8a93681a267e3b1e9a7dcda1185436fe141f7749120a303721813")
            .expect("valid hex")
    }

    #[test]
    fn meta_key_golden_bytes() {
        let key = meta_key(RepoId(0), "repo:acme/widgets");
        let mut expected = vec![0x01u8]; // version
        expected.extend_from_slice(&0u64.to_be_bytes()); // repo_id = 0 (global)
        expected.push(0x00); // segment = meta
        expected.extend_from_slice(b"repo:acme/widgets");
        assert_eq!(key, expected);
    }

    #[test]
    fn meta_key_round_trip() {
        let key = meta_key(RepoId(7), "object-format");
        let (repo, name) = decode_meta_key(&key).expect("decodes");
        assert_eq!(repo, RepoId(7));
        assert_eq!(name, "object-format");
    }

    #[test]
    fn object_key_golden_bytes_sha1() {
        let oid = sha1_oid();
        let key = object_key(RepoId(42), &oid);
        let mut expected = vec![0x01u8];
        expected.extend_from_slice(&42u64.to_be_bytes());
        expected.push(0x01); // segment = object
        expected.extend_from_slice(oid.as_bytes());
        assert_eq!(key, expected);
        assert_eq!(key.len(), 10 + 20);
    }

    #[test]
    fn object_key_golden_bytes_sha256() {
        let oid = sha256_oid();
        let key = object_key(RepoId(42), &oid);
        let mut expected = vec![0x01u8];
        expected.extend_from_slice(&42u64.to_be_bytes());
        expected.push(0x01);
        expected.extend_from_slice(oid.as_bytes());
        assert_eq!(key, expected);
        assert_eq!(key.len(), 10 + 32);
    }

    #[test]
    fn object_key_round_trip_both_widths() {
        for oid in [sha1_oid(), sha256_oid()] {
            let key = object_key(RepoId(3), &oid);
            let (repo, decoded) = decode_object_key(&key, oid.kind()).expect("decodes");
            assert_eq!(repo, RepoId(3));
            assert_eq!(decoded, oid);
        }
    }

    #[test]
    fn commit_key_golden_bytes() {
        let oid = sha1_oid();
        let key = commit_key(RepoId(1), &oid);
        let mut expected = vec![0x01u8];
        expected.extend_from_slice(&1u64.to_be_bytes());
        expected.push(0x03); // segment = commit-graph
        expected.extend_from_slice(oid.as_bytes());
        assert_eq!(key, expected);
    }

    #[test]
    fn commit_key_round_trip_both_widths() {
        for oid in [sha1_oid(), sha256_oid()] {
            let key = commit_key(RepoId(9), &oid);
            let (repo, decoded) = decode_commit_key(&key, oid.kind()).expect("decodes");
            assert_eq!(repo, RepoId(9));
            assert_eq!(decoded, oid);
        }
    }

    #[test]
    fn ref_key_golden_bytes() {
        let key = ref_key(RepoId(2), "refs/heads/main");
        let mut expected = vec![0x01u8];
        expected.extend_from_slice(&2u64.to_be_bytes());
        expected.push(0x02); // segment = ref
        expected.extend_from_slice(b"refs/heads/main");
        assert_eq!(key, expected);
    }

    #[test]
    fn ref_key_round_trip() {
        let key = ref_key(RepoId(2), "HEAD");
        let (repo, name) = decode_ref_key(&key).expect("decodes");
        assert_eq!(repo, RepoId(2));
        assert_eq!(name, "HEAD");
    }

    #[test]
    fn ref_key_ordering() {
        let a = ref_key(RepoId(1), "refs/heads/a");
        let b = ref_key(RepoId(1), "refs/heads/b");
        assert!(a < b);
    }

    #[test]
    fn ref_prefix_scan_covers_exactly_the_prefix() {
        let prefix = ref_prefix(RepoId(1), "refs/heads/");
        let matching = ref_key(RepoId(1), "refs/heads/main");
        let non_matching = ref_key(RepoId(1), "refs/tags/v1");
        assert!(matching.starts_with(&prefix));
        assert!(!non_matching.starts_with(&prefix));
    }

    #[test]
    fn ref_prefix_scan_does_not_leak_into_other_repos() {
        let prefix = ref_prefix(RepoId(1), "refs/heads/");
        let other_repo = ref_key(RepoId(2), "refs/heads/main");
        assert!(!other_repo.starts_with(&prefix));
    }

    #[test]
    fn segment_prefix_covers_only_its_segment() {
        let prefix = segment_prefix(RepoId(5), Segment::Object);
        let oid_key = object_key(RepoId(5), &sha1_oid());
        let ref_key_bytes = ref_key(RepoId(5), "refs/heads/main");
        assert!(oid_key.starts_with(&prefix));
        assert!(!ref_key_bytes.starts_with(&prefix));
    }

    #[test]
    fn decode_rejects_bad_version_byte() {
        let mut key = meta_key(RepoId(0), "schema-version");
        key[0] = 0x02;
        assert_eq!(
            decode_meta_key(&key),
            Err(KeyError::UnsupportedVersion(0x02))
        );
    }

    #[test]
    fn decode_rejects_unknown_segment_byte() {
        let mut key = meta_key(RepoId(0), "schema-version");
        key[9] = 0xff;
        assert_eq!(decode_meta_key(&key), Err(KeyError::UnknownSegment(0xff)));
    }

    #[test]
    fn decode_rejects_wrong_segment() {
        let key = ref_key(RepoId(0), "HEAD");
        assert_eq!(
            decode_meta_key(&key),
            Err(KeyError::WrongSegment {
                expected: Segment::Meta,
                actual: Segment::Ref,
            })
        );
    }

    #[test]
    fn decode_rejects_truncated_preamble() {
        let key = vec![0x01u8, 0x00, 0x00, 0x00];
        assert_eq!(
            decode_meta_key(&key),
            Err(KeyError::TooShort {
                expected: 10,
                actual: 4,
            })
        );
    }

    #[test]
    fn decode_object_key_rejects_truncated_oid() {
        let mut key = object_key(RepoId(0), &sha1_oid());
        key.truncate(key.len() - 1);
        assert_eq!(
            decode_object_key(&key, gix_hash::Kind::Sha1),
            Err(KeyError::InvalidObjectIdLen {
                expected: 20,
                actual: 19,
            })
        );
    }

    #[test]
    fn decode_object_key_rejects_wrong_width_for_configured_hash() {
        // A valid SHA-256 key decoded against a repo configured for SHA-1
        // must be rejected rather than silently reinterpreted.
        let key = object_key(RepoId(0), &sha256_oid());
        assert_eq!(
            decode_object_key(&key, gix_hash::Kind::Sha1),
            Err(KeyError::InvalidObjectIdLen {
                expected: 20,
                actual: 32,
            })
        );
    }

    #[test]
    fn decode_meta_key_rejects_invalid_utf8() {
        let mut key = meta_key(RepoId(0), "x");
        key.truncate(key.len() - 1);
        key.push(0xff); // invalid utf-8 byte
        assert_eq!(decode_meta_key(&key), Err(KeyError::InvalidUtf8));
    }
}
