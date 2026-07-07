//! Storage key encoding and decoding.
//!
//! Every key in every segment begins with the same fixed-width preamble
//! (`segment | version | repo_id`) followed by a segment-specific suffix.
//! The segment byte leads so each segment is a single key prefix shared by
//! all repositories. See `docs/0001-init.md` §Storage (Key Preamble and the
//! per-segment Key Layout subsections) for the exact layout.

use gix_hash::ObjectId;

/// The current key format version.
pub const KEY_VERSION: u8 = 1;

/// Length in bytes of the fixed `segment | version | repo_id` preamble.
const PREAMBLE_LEN: usize = 1 + 1 + 8;

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
    buf.push(segment.as_u8());
    buf.push(KEY_VERSION);
    buf.extend_from_slice(&repo.0.to_be_bytes());
}

/// Split a key into its preamble fields and segment-specific suffix.
fn decode_preamble(bytes: &[u8]) -> Result<(RepoId, Segment, &[u8]), KeyError> {
    if bytes.len() < PREAMBLE_LEN {
        return Err(KeyError::TooShort {
            expected: PREAMBLE_LEN,
            actual: bytes.len(),
        });
    }
    let version = bytes[1];
    if version != KEY_VERSION {
        return Err(KeyError::UnsupportedVersion(version));
    }
    let segment = Segment::try_from(bytes[0])?;
    let repo_id = u64::from_be_bytes([
        bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8], bytes[9],
    ]);
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
    fn should_encode_meta_key_bytes_exactly() {
        // given/when
        let key = meta_key(RepoId(0), "repo:acme/widgets");

        // then
        let mut expected = vec![0x00u8, 0x01]; // segment = meta, version
        expected.extend_from_slice(&0u64.to_be_bytes()); // repo_id = 0 (global)
        expected.extend_from_slice(b"repo:acme/widgets");
        assert_eq!(key, expected);
    }

    #[test]
    fn should_round_trip_meta_key() {
        // given
        let key = meta_key(RepoId(7), "object-format");

        // when
        let (repo, name) = decode_meta_key(&key).expect("decodes");

        // then
        assert_eq!(repo, RepoId(7));
        assert_eq!(name, "object-format");
    }

    #[test]
    fn should_encode_sha1_object_key_bytes_exactly() {
        // given
        let oid = sha1_oid();

        // when
        let key = object_key(RepoId(42), &oid);

        // then
        let mut expected = vec![0x01u8, 0x01]; // segment = object, version
        expected.extend_from_slice(&42u64.to_be_bytes());
        expected.extend_from_slice(oid.as_bytes());
        assert_eq!(key, expected);
        assert_eq!(key.len(), 10 + 20);
    }

    #[test]
    fn should_encode_sha256_object_key_bytes_exactly() {
        // given
        let oid = sha256_oid();

        // when
        let key = object_key(RepoId(42), &oid);

        // then
        let mut expected = vec![0x01u8, 0x01]; // segment = object, version
        expected.extend_from_slice(&42u64.to_be_bytes());
        expected.extend_from_slice(oid.as_bytes());
        assert_eq!(key, expected);
        assert_eq!(key.len(), 10 + 32);
    }

    #[test]
    fn should_round_trip_object_key_at_both_hash_widths() {
        // given/when/then: each width is its own given -> when -> then cycle.
        for oid in [sha1_oid(), sha256_oid()] {
            // given
            let key = object_key(RepoId(3), &oid);

            // when
            let (repo, decoded) = decode_object_key(&key, oid.kind()).expect("decodes");

            // then
            assert_eq!(repo, RepoId(3));
            assert_eq!(decoded, oid);
        }
    }

    #[test]
    fn should_encode_commit_key_bytes_exactly() {
        // given
        let oid = sha1_oid();

        // when
        let key = commit_key(RepoId(1), &oid);

        // then
        let mut expected = vec![0x03u8, 0x01]; // segment = commit-graph, version
        expected.extend_from_slice(&1u64.to_be_bytes());
        expected.extend_from_slice(oid.as_bytes());
        assert_eq!(key, expected);
    }

    #[test]
    fn should_round_trip_commit_key_at_both_hash_widths() {
        // given/when/then: each width is its own given -> when -> then cycle.
        for oid in [sha1_oid(), sha256_oid()] {
            // given
            let key = commit_key(RepoId(9), &oid);

            // when
            let (repo, decoded) = decode_commit_key(&key, oid.kind()).expect("decodes");

            // then
            assert_eq!(repo, RepoId(9));
            assert_eq!(decoded, oid);
        }
    }

    #[test]
    fn should_encode_ref_key_bytes_exactly() {
        // given/when
        let key = ref_key(RepoId(2), "refs/heads/main");

        // then
        let mut expected = vec![0x02u8, 0x01]; // segment = ref, version
        expected.extend_from_slice(&2u64.to_be_bytes());
        expected.extend_from_slice(b"refs/heads/main");
        assert_eq!(key, expected);
    }

    #[test]
    fn should_round_trip_ref_key() {
        // given
        let key = ref_key(RepoId(2), "HEAD");

        // when
        let (repo, name) = decode_ref_key(&key).expect("decodes");

        // then
        assert_eq!(repo, RepoId(2));
        assert_eq!(name, "HEAD");
    }

    #[test]
    fn should_order_ref_keys_by_name() {
        // given
        let a = ref_key(RepoId(1), "refs/heads/a");
        let b = ref_key(RepoId(1), "refs/heads/b");

        // when/then
        assert!(a < b);
    }

    #[test]
    fn should_match_ref_prefix_scan_to_exactly_the_prefix() {
        // given
        let prefix = ref_prefix(RepoId(1), "refs/heads/");
        let matching = ref_key(RepoId(1), "refs/heads/main");
        let non_matching = ref_key(RepoId(1), "refs/tags/v1");

        // when/then
        assert!(matching.starts_with(&prefix));
        assert!(!non_matching.starts_with(&prefix));
    }

    #[test]
    fn should_not_leak_ref_prefix_scan_across_repos() {
        // given
        let prefix = ref_prefix(RepoId(1), "refs/heads/");
        let other_repo = ref_key(RepoId(2), "refs/heads/main");

        // when/then
        assert!(!other_repo.starts_with(&prefix));
    }

    #[test]
    fn should_match_segment_prefix_to_only_its_segment() {
        // given
        let prefix = segment_prefix(RepoId(5), Segment::Object);
        let oid_key = object_key(RepoId(5), &sha1_oid());
        let ref_key_bytes = ref_key(RepoId(5), "refs/heads/main");

        // when/then
        assert!(oid_key.starts_with(&prefix));
        assert!(!ref_key_bytes.starts_with(&prefix));
    }

    #[test]
    fn should_share_leading_segment_byte_across_repos() {
        // given
        // The segment byte leads the key, so a segment is one contiguous
        // keyspace prefix regardless of how many repositories exist.
        let a = object_key(RepoId(1), &sha1_oid());
        let b = object_key(RepoId(2), &sha256_oid());
        let other_segment = ref_key(RepoId(1), "refs/heads/main");

        // when/then
        assert_eq!(a[..2], b[..2]); // segment byte + version byte
        assert_ne!(a[0], other_segment[0]);
    }

    #[test]
    fn should_reject_unsupported_version_byte() {
        // given
        let mut key = meta_key(RepoId(0), "schema-version");
        key[1] = 0x02;

        // when
        let result = decode_meta_key(&key);

        // then
        assert_eq!(result, Err(KeyError::UnsupportedVersion(0x02)));
    }

    #[test]
    fn should_reject_unknown_segment_byte() {
        // given
        let mut key = meta_key(RepoId(0), "schema-version");
        key[0] = 0xff;

        // when
        let result = decode_meta_key(&key);

        // then
        assert_eq!(result, Err(KeyError::UnknownSegment(0xff)));
    }

    #[test]
    fn should_reject_wrong_segment_on_decode() {
        // given
        let key = ref_key(RepoId(0), "HEAD");

        // when
        let result = decode_meta_key(&key);

        // then
        assert_eq!(
            result,
            Err(KeyError::WrongSegment {
                expected: Segment::Meta,
                actual: Segment::Ref,
            })
        );
    }

    #[test]
    fn should_reject_truncated_preamble() {
        // given
        let key = vec![0x00u8, 0x01, 0x00, 0x00];

        // when
        let result = decode_meta_key(&key);

        // then
        assert_eq!(
            result,
            Err(KeyError::TooShort {
                expected: 10,
                actual: 4,
            })
        );
    }

    #[test]
    fn should_reject_truncated_object_id() {
        // given
        let mut key = object_key(RepoId(0), &sha1_oid());
        key.truncate(key.len() - 1);

        // when
        let result = decode_object_key(&key, gix_hash::Kind::Sha1);

        // then
        assert_eq!(
            result,
            Err(KeyError::InvalidObjectIdLen {
                expected: 20,
                actual: 19,
            })
        );
    }

    #[test]
    fn should_reject_object_key_with_wrong_configured_hash_width() {
        // given
        // A valid SHA-256 key decoded against a repo configured for SHA-1
        // must be rejected rather than silently reinterpreted.
        let key = object_key(RepoId(0), &sha256_oid());

        // when
        let result = decode_object_key(&key, gix_hash::Kind::Sha1);

        // then
        assert_eq!(
            result,
            Err(KeyError::InvalidObjectIdLen {
                expected: 20,
                actual: 32,
            })
        );
    }

    #[test]
    fn should_reject_invalid_utf8_in_meta_key() {
        // given
        let mut key = meta_key(RepoId(0), "x");
        key.truncate(key.len() - 1);
        key.push(0xff); // invalid utf-8 byte

        // when
        let result = decode_meta_key(&key);

        // then
        assert_eq!(result, Err(KeyError::InvalidUtf8));
    }
}
