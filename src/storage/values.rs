//! Storage value encoding and decoding.
//!
//! Every value begins with a one-byte tag identifying how to interpret the
//! remaining bytes. See `docs/0001-init.md` §Storage for the per-segment
//! Value Layout tables, and this repo's "Fixed decisions" for the
//! clarifications binding here (big-endian integers throughout; blob-inline
//! keeps its canonical git header, tree/commit/tag records store body-only
//! bytes).

use bytes::Bytes;
use gix_hash::ObjectId;
use gix_object::Kind as ObjectKind;

/// Errors returned while decoding a storage value.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ValueError {
    /// The value had no bytes at all (not even a tag).
    #[error("value is empty")]
    Empty,
    /// The value's tag byte did not match a known variant.
    #[error("unknown value tag: {0:#04x}")]
    UnknownTag(u8),
    /// The value did not contain enough bytes for its tag's fixed-width
    /// payload.
    #[error("value is truncated")]
    Truncated,
    /// The value contained more bytes than its tag's fixed-width payload
    /// requires.
    #[error("value has trailing bytes")]
    TrailingBytes,
    /// A UTF-8 payload (string value, ref name) was not valid UTF-8.
    #[error("invalid utf-8 in value")]
    InvalidUtf8,
    /// A binary object id payload was not a valid 20- or 32-byte hash.
    #[error("invalid object id")]
    InvalidObjectId,
}

fn read_u32_be(bytes: &[u8]) -> u32 {
    u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

fn read_u64_be(bytes: &[u8]) -> u64 {
    u64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ])
}

/// A metadata-segment value: server-global or per-repo settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetaValue {
    /// A UTF-8 string setting.
    Utf8(String),
    /// A single-byte setting.
    U8(u8),
    /// A big-endian `u32` setting.
    U32(u32),
    /// An opaque raw-bytes setting.
    Raw(Vec<u8>),
}

impl MetaValue {
    /// Encode this value as `value_type | value`.
    pub fn encode(&self) -> Vec<u8> {
        match self {
            MetaValue::Utf8(s) => {
                let mut buf = Vec::with_capacity(1 + s.len());
                buf.push(0x01);
                buf.extend_from_slice(s.as_bytes());
                buf
            }
            MetaValue::U8(v) => vec![0x02, *v],
            MetaValue::U32(v) => {
                let mut buf = Vec::with_capacity(1 + 4);
                buf.push(0x03);
                buf.extend_from_slice(&v.to_be_bytes());
                buf
            }
            MetaValue::Raw(bytes) => {
                let mut buf = Vec::with_capacity(1 + bytes.len());
                buf.push(0x04);
                buf.extend_from_slice(bytes);
                buf
            }
        }
    }

    /// Decode a `value_type | value` byte string.
    pub fn decode(bytes: &[u8]) -> Result<Self, ValueError> {
        let (&tag, rest) = bytes.split_first().ok_or(ValueError::Empty)?;
        match tag {
            0x01 => {
                let s = std::str::from_utf8(rest).map_err(|_| ValueError::InvalidUtf8)?;
                Ok(MetaValue::Utf8(s.to_owned()))
            }
            0x02 => match rest.len() {
                0 => Err(ValueError::Truncated),
                1 => Ok(MetaValue::U8(rest[0])),
                _ => Err(ValueError::TrailingBytes),
            },
            0x03 => match rest.len().cmp(&4) {
                std::cmp::Ordering::Less => Err(ValueError::Truncated),
                std::cmp::Ordering::Equal => Ok(MetaValue::U32(read_u32_be(rest))),
                std::cmp::Ordering::Greater => Err(ValueError::TrailingBytes),
            },
            0x04 => Ok(MetaValue::Raw(rest.to_vec())),
            other => Err(ValueError::UnknownTag(other)),
        }
    }
}

/// An object-segment value: a git object's storage record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectRecord {
    /// A blob stored inline, as the canonical git encoding `<type>
    /// <size>\0<content>` (header included).
    BlobInline(Bytes),
    /// A blob offloaded to object storage under `blobs/<xx>/<rest>`; the
    /// payload carries only its content size, so size queries never need an
    /// object-storage round trip.
    BlobPointer {
        /// The blob's content size in bytes.
        size: u64,
    },
    /// A tree object's body (no `<type> <size>\0` header).
    Tree(Bytes),
    /// A commit object's body (no `<type> <size>\0` header).
    Commit(Bytes),
    /// A tag object's body (no `<type> <size>\0` header).
    Tag(Bytes),
}

impl ObjectRecord {
    /// The `gix_object::Kind` this record represents.
    pub fn kind(&self) -> ObjectKind {
        match self {
            ObjectRecord::BlobInline(_) | ObjectRecord::BlobPointer { .. } => ObjectKind::Blob,
            ObjectRecord::Tree(_) => ObjectKind::Tree,
            ObjectRecord::Commit(_) => ObjectKind::Commit,
            ObjectRecord::Tag(_) => ObjectKind::Tag,
        }
    }

    /// The object's content size in bytes. Never touches the blob content
    /// store: pointer records carry their size in the payload, and
    /// tree/commit/tag records store body-only bytes whose length *is* the
    /// size.
    pub fn size(&self) -> u64 {
        match self {
            ObjectRecord::BlobInline(bytes) => gix_object::decode::loose_header(bytes)
                .map(|(_, size, _)| size)
                .unwrap_or(0),
            ObjectRecord::BlobPointer { size } => *size,
            ObjectRecord::Tree(bytes) | ObjectRecord::Commit(bytes) | ObjectRecord::Tag(bytes) => {
                bytes.len() as u64
            }
        }
    }

    /// Encode this record as `object_tag | payload`.
    pub fn encode(&self) -> Vec<u8> {
        match self {
            ObjectRecord::BlobInline(bytes) => {
                let mut buf = Vec::with_capacity(1 + bytes.len());
                buf.push(0x01);
                buf.extend_from_slice(bytes);
                buf
            }
            ObjectRecord::BlobPointer { size } => {
                let mut buf = Vec::with_capacity(1 + 8);
                buf.push(0x02);
                buf.extend_from_slice(&size.to_be_bytes());
                buf
            }
            ObjectRecord::Tree(bytes) => {
                let mut buf = Vec::with_capacity(1 + bytes.len());
                buf.push(0x03);
                buf.extend_from_slice(bytes);
                buf
            }
            ObjectRecord::Commit(bytes) => {
                let mut buf = Vec::with_capacity(1 + bytes.len());
                buf.push(0x04);
                buf.extend_from_slice(bytes);
                buf
            }
            ObjectRecord::Tag(bytes) => {
                let mut buf = Vec::with_capacity(1 + bytes.len());
                buf.push(0x05);
                buf.extend_from_slice(bytes);
                buf
            }
        }
    }

    /// Decode an `object_tag | payload` byte string.
    pub fn decode(bytes: &[u8]) -> Result<Self, ValueError> {
        let (&tag, rest) = bytes.split_first().ok_or(ValueError::Empty)?;
        match tag {
            0x01 => Ok(ObjectRecord::BlobInline(Bytes::copy_from_slice(rest))),
            0x02 => match rest.len().cmp(&8) {
                std::cmp::Ordering::Less => Err(ValueError::Truncated),
                std::cmp::Ordering::Equal => Ok(ObjectRecord::BlobPointer {
                    size: read_u64_be(rest),
                }),
                std::cmp::Ordering::Greater => Err(ValueError::TrailingBytes),
            },
            0x03 => Ok(ObjectRecord::Tree(Bytes::copy_from_slice(rest))),
            0x04 => Ok(ObjectRecord::Commit(Bytes::copy_from_slice(rest))),
            0x05 => Ok(ObjectRecord::Tag(Bytes::copy_from_slice(rest))),
            other => Err(ValueError::UnknownTag(other)),
        }
    }
}

/// A ref-segment value: what a ref name points at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefTarget {
    /// Points directly at an object id.
    Direct(ObjectId),
    /// Points at another ref by name (a symref, e.g. `HEAD`).
    Reference(String),
}

impl RefTarget {
    /// Encode this target as `ref_tag | target`.
    pub fn encode(&self) -> Vec<u8> {
        match self {
            RefTarget::Direct(oid) => {
                let mut buf = Vec::with_capacity(1 + oid.as_bytes().len());
                buf.push(0x01);
                buf.extend_from_slice(oid.as_bytes());
                buf
            }
            RefTarget::Reference(name) => {
                let mut buf = Vec::with_capacity(1 + name.len());
                buf.push(0x02);
                buf.extend_from_slice(name.as_bytes());
                buf
            }
        }
    }

    /// Decode a `ref_tag | target` byte string. The SHA width of a `Direct`
    /// target is inferred from its length (20 or 32 bytes), so no hash kind
    /// parameter is needed.
    pub fn decode(bytes: &[u8]) -> Result<Self, ValueError> {
        let (&tag, rest) = bytes.split_first().ok_or(ValueError::Empty)?;
        match tag {
            0x01 => {
                let oid = ObjectId::try_from(rest).map_err(|_| ValueError::InvalidObjectId)?;
                Ok(RefTarget::Direct(oid))
            }
            0x02 => {
                let s = std::str::from_utf8(rest).map_err(|_| ValueError::InvalidUtf8)?;
                Ok(RefTarget::Reference(s.to_owned()))
            }
            other => Err(ValueError::UnknownTag(other)),
        }
    }
}

/// A commit-graph-segment value: derived ancestry metadata for one commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitGraphRecord {
    /// `1 + max(parent generations)`, or `1` for a root commit.
    pub generation: u32,
    /// The commit's root tree (Phase II walk entry point).
    pub root_tree: ObjectId,
    /// The commit's parents, in the commit's parent order.
    pub parents: Vec<ObjectId>,
}

impl CommitGraphRecord {
    /// Encode this record as `generation | root_tree | parent_count |
    /// parents`.
    pub fn encode(&self) -> Vec<u8> {
        let width = self.root_tree.as_slice().len();
        let mut buf = Vec::with_capacity(4 + width + 1 + self.parents.len() * width);
        buf.extend_from_slice(&self.generation.to_be_bytes());
        buf.extend_from_slice(self.root_tree.as_slice());
        buf.push(self.parents.len() as u8);
        for parent in &self.parents {
            buf.extend_from_slice(parent.as_slice());
        }
        buf
    }

    /// Decode a `generation | root_tree | parent_count | parents` byte
    /// string. `hash_kind` must be the repo's configured SHA width, since
    /// object ids in the payload have no independent length marker.
    pub fn decode(bytes: &[u8], hash_kind: gix_hash::Kind) -> Result<Self, ValueError> {
        let width = hash_kind.len_in_bytes();
        let header_len = 4 + width + 1;
        if bytes.len() < header_len {
            return Err(ValueError::Truncated);
        }
        let generation = read_u32_be(&bytes[0..4]);
        let root_tree =
            ObjectId::try_from(&bytes[4..4 + width]).map_err(|_| ValueError::InvalidObjectId)?;
        let parent_count = bytes[4 + width] as usize;
        let expected_len = header_len + parent_count * width;
        match bytes.len().cmp(&expected_len) {
            std::cmp::Ordering::Less => return Err(ValueError::Truncated),
            std::cmp::Ordering::Greater => return Err(ValueError::TrailingBytes),
            std::cmp::Ordering::Equal => {}
        }
        let mut parents = Vec::with_capacity(parent_count);
        for i in 0..parent_count {
            let start = header_len + i * width;
            let oid = ObjectId::try_from(&bytes[start..start + width])
                .map_err(|_| ValueError::InvalidObjectId)?;
            parents.push(oid);
        }
        Ok(CommitGraphRecord {
            generation,
            root_tree,
            parents,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sha1_oid(hex: &[u8]) -> ObjectId {
        ObjectId::from_hex(hex).expect("valid hex")
    }

    fn sha256_oid(hex: &[u8]) -> ObjectId {
        ObjectId::from_hex(hex).expect("valid hex")
    }

    const SHA1_A: &[u8] = b"e69de29bb2d1d6434b8b29ae775ad8c2e48c5391";
    const SHA1_B: &[u8] = b"4b825dc642cb6eb9a060e54bf8d69288fbee4904";
    const SHA256_A: &[u8] = b"473a0f4c3be8a93681a267e3b1e9a7dcda1185436fe141f7749120a303721813";
    const SHA256_B: &[u8] = b"6ef19b41225c5369f1c104d45d8d85efa9b057b53b14b4b9b939dd74decc5321";

    // --- MetaValue ---

    #[test]
    fn meta_value_utf8_golden_bytes() {
        let v = MetaValue::Utf8("sha1".to_owned());
        let mut expected = vec![0x01u8];
        expected.extend_from_slice(b"sha1");
        assert_eq!(v.encode(), expected);
    }

    #[test]
    fn meta_value_u8_golden_bytes() {
        let v = MetaValue::U8(7);
        assert_eq!(v.encode(), vec![0x02, 0x07]);
    }

    #[test]
    fn meta_value_u32_golden_bytes() {
        let v = MetaValue::U32(1);
        assert_eq!(v.encode(), vec![0x03, 0x00, 0x00, 0x00, 0x01]);
    }

    #[test]
    fn meta_value_raw_golden_bytes() {
        // e.g. next-repo-id / repo:<name> per Fixed decisions: raw u64 BE.
        let v = MetaValue::Raw(1u64.to_be_bytes().to_vec());
        let mut expected = vec![0x04u8];
        expected.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 1]);
        assert_eq!(v.encode(), expected);
    }

    #[test]
    fn meta_value_round_trips() {
        for v in [
            MetaValue::Utf8("refs/heads/main".to_owned()),
            MetaValue::U8(3),
            MetaValue::U32(42),
            MetaValue::Raw(vec![1, 2, 3, 4]),
        ] {
            assert_eq!(MetaValue::decode(&v.encode()).expect("decodes"), v);
        }
    }

    #[test]
    fn meta_value_rejects_unknown_tag() {
        assert_eq!(
            MetaValue::decode(&[0xff]),
            Err(ValueError::UnknownTag(0xff))
        );
    }

    #[test]
    fn meta_value_rejects_empty() {
        assert_eq!(MetaValue::decode(&[]), Err(ValueError::Empty));
    }

    #[test]
    fn meta_value_rejects_truncated_u32() {
        assert_eq!(
            MetaValue::decode(&[0x03, 0x00, 0x00]),
            Err(ValueError::Truncated)
        );
    }

    #[test]
    fn meta_value_rejects_trailing_bytes_u8() {
        assert_eq!(
            MetaValue::decode(&[0x02, 0x01, 0x02]),
            Err(ValueError::TrailingBytes)
        );
    }

    #[test]
    fn meta_value_rejects_invalid_utf8() {
        assert_eq!(
            MetaValue::decode(&[0x01, 0xff]),
            Err(ValueError::InvalidUtf8)
        );
    }

    // --- ObjectRecord ---

    #[test]
    fn object_record_blob_inline_golden_bytes() {
        let record = ObjectRecord::BlobInline(Bytes::from_static(b"blob 5\0hello"));
        let mut expected = vec![0x01u8];
        expected.extend_from_slice(b"blob 5\0hello");
        assert_eq!(record.encode(), expected);
        assert_eq!(record.kind(), ObjectKind::Blob);
        assert_eq!(record.size(), 5);
    }

    #[test]
    fn object_record_blob_pointer_golden_bytes() {
        let record = ObjectRecord::BlobPointer { size: 131_072 };
        let mut expected = vec![0x02u8];
        expected.extend_from_slice(&131_072u64.to_be_bytes());
        assert_eq!(record.encode(), expected);
        assert_eq!(record.kind(), ObjectKind::Blob);
        assert_eq!(record.size(), 131_072);
    }

    #[test]
    fn object_record_tree_golden_bytes() {
        let body = Bytes::from_static(b"tree-body-bytes");
        let record = ObjectRecord::Tree(body.clone());
        let mut expected = vec![0x03u8];
        expected.extend_from_slice(&body);
        assert_eq!(record.encode(), expected);
        assert_eq!(record.kind(), ObjectKind::Tree);
        assert_eq!(record.size(), body.len() as u64);
    }

    #[test]
    fn object_record_commit_golden_bytes() {
        let body = Bytes::from_static(b"commit-body-bytes");
        let record = ObjectRecord::Commit(body.clone());
        let mut expected = vec![0x04u8];
        expected.extend_from_slice(&body);
        assert_eq!(record.encode(), expected);
        assert_eq!(record.kind(), ObjectKind::Commit);
    }

    #[test]
    fn object_record_tag_golden_bytes() {
        let body = Bytes::from_static(b"tag-body-bytes");
        let record = ObjectRecord::Tag(body.clone());
        let mut expected = vec![0x05u8];
        expected.extend_from_slice(&body);
        assert_eq!(record.encode(), expected);
        assert_eq!(record.kind(), ObjectKind::Tag);
    }

    #[test]
    fn object_record_round_trips() {
        for record in [
            ObjectRecord::BlobInline(Bytes::from_static(b"blob 5\0hello")),
            ObjectRecord::BlobPointer { size: 999 },
            ObjectRecord::Tree(Bytes::from_static(b"tree-body")),
            ObjectRecord::Commit(Bytes::from_static(b"commit-body")),
            ObjectRecord::Tag(Bytes::from_static(b"tag-body")),
        ] {
            assert_eq!(
                ObjectRecord::decode(&record.encode()).expect("decodes"),
                record
            );
        }
    }

    #[test]
    fn object_record_rejects_unknown_tag() {
        assert_eq!(
            ObjectRecord::decode(&[0x00]),
            Err(ValueError::UnknownTag(0x00))
        );
    }

    #[test]
    fn object_record_rejects_empty() {
        assert_eq!(ObjectRecord::decode(&[]), Err(ValueError::Empty));
    }

    #[test]
    fn object_record_rejects_truncated_pointer() {
        assert_eq!(
            ObjectRecord::decode(&[0x02, 0x00, 0x00]),
            Err(ValueError::Truncated)
        );
    }

    #[test]
    fn object_record_rejects_trailing_bytes_on_pointer() {
        let mut bytes = vec![0x02u8];
        bytes.extend_from_slice(&99u64.to_be_bytes());
        bytes.push(0xAA);
        assert_eq!(ObjectRecord::decode(&bytes), Err(ValueError::TrailingBytes));
    }

    // --- RefTarget ---

    #[test]
    fn ref_target_direct_golden_bytes_sha1() {
        let oid = sha1_oid(SHA1_A);
        let target = RefTarget::Direct(oid);
        let mut expected = vec![0x01u8];
        expected.extend_from_slice(oid.as_bytes());
        assert_eq!(target.encode(), expected);
    }

    #[test]
    fn ref_target_direct_golden_bytes_sha256() {
        let oid = sha256_oid(SHA256_A);
        let target = RefTarget::Direct(oid);
        let mut expected = vec![0x01u8];
        expected.extend_from_slice(oid.as_bytes());
        assert_eq!(target.encode(), expected);
    }

    #[test]
    fn ref_target_reference_golden_bytes() {
        let target = RefTarget::Reference("refs/heads/main".to_owned());
        let mut expected = vec![0x02u8];
        expected.extend_from_slice(b"refs/heads/main");
        assert_eq!(target.encode(), expected);
    }

    #[test]
    fn ref_target_round_trips_both_widths() {
        for oid in [sha1_oid(SHA1_A), sha256_oid(SHA256_A)] {
            let target = RefTarget::Direct(oid);
            assert_eq!(
                RefTarget::decode(&target.encode()).expect("decodes"),
                target
            );
        }
        let symref = RefTarget::Reference("HEAD-target".to_owned());
        assert_eq!(
            RefTarget::decode(&symref.encode()).expect("decodes"),
            symref
        );
    }

    #[test]
    fn ref_target_rejects_unknown_tag() {
        assert_eq!(
            RefTarget::decode(&[0x03]),
            Err(ValueError::UnknownTag(0x03))
        );
    }

    #[test]
    fn ref_target_rejects_truncated_direct() {
        let oid = sha1_oid(SHA1_A);
        let mut bytes = vec![0x01u8];
        bytes.extend_from_slice(&oid.as_bytes()[..19]);
        assert_eq!(RefTarget::decode(&bytes), Err(ValueError::InvalidObjectId));
    }

    #[test]
    fn ref_target_rejects_invalid_utf8_reference() {
        assert_eq!(
            RefTarget::decode(&[0x02, 0xff]),
            Err(ValueError::InvalidUtf8)
        );
    }

    // --- CommitGraphRecord ---

    #[test]
    fn commit_graph_record_golden_bytes_root_commit_sha1() {
        let root_tree = sha1_oid(SHA1_A);
        let record = CommitGraphRecord {
            generation: 1,
            root_tree,
            parents: vec![],
        };
        let mut expected = vec![0x00, 0x00, 0x00, 0x01]; // generation = 1
        expected.extend_from_slice(root_tree.as_bytes());
        expected.push(0x00); // parent_count = 0
        assert_eq!(record.encode(), expected);
    }

    #[test]
    fn commit_graph_record_golden_bytes_merge_sha1() {
        let root_tree = sha1_oid(SHA1_A);
        let parent_a = sha1_oid(SHA1_B);
        let parent_b = sha1_oid(SHA1_A);
        let record = CommitGraphRecord {
            generation: 3,
            root_tree,
            parents: vec![parent_a, parent_b],
        };
        let mut expected = vec![0x00, 0x00, 0x00, 0x03]; // generation = 3
        expected.extend_from_slice(root_tree.as_bytes());
        expected.push(0x02); // parent_count = 2
        expected.extend_from_slice(parent_a.as_bytes());
        expected.extend_from_slice(parent_b.as_bytes());
        assert_eq!(record.encode(), expected);
    }

    #[test]
    fn commit_graph_record_golden_bytes_sha256() {
        let root_tree = sha256_oid(SHA256_A);
        let parent = sha256_oid(SHA256_B);
        let record = CommitGraphRecord {
            generation: 2,
            root_tree,
            parents: vec![parent],
        };
        let mut expected = vec![0x00, 0x00, 0x00, 0x02];
        expected.extend_from_slice(root_tree.as_bytes());
        expected.push(0x01);
        expected.extend_from_slice(parent.as_bytes());
        assert_eq!(record.encode(), expected);
    }

    #[test]
    fn commit_graph_record_round_trips_both_widths() {
        let sha1_record = CommitGraphRecord {
            generation: 5,
            root_tree: sha1_oid(SHA1_A),
            parents: vec![sha1_oid(SHA1_B)],
        };
        assert_eq!(
            CommitGraphRecord::decode(&sha1_record.encode(), gix_hash::Kind::Sha1)
                .expect("decodes"),
            sha1_record
        );

        let sha256_record = CommitGraphRecord {
            generation: 6,
            root_tree: sha256_oid(SHA256_A),
            parents: vec![sha256_oid(SHA256_B), sha256_oid(SHA256_A)],
        };
        assert_eq!(
            CommitGraphRecord::decode(&sha256_record.encode(), gix_hash::Kind::Sha256)
                .expect("decodes"),
            sha256_record
        );
    }

    #[test]
    fn commit_graph_record_rejects_truncated_header() {
        assert_eq!(
            CommitGraphRecord::decode(&[0x00, 0x00, 0x00, 0x01], gix_hash::Kind::Sha1),
            Err(ValueError::Truncated)
        );
    }

    #[test]
    fn commit_graph_record_rejects_truncated_parents() {
        let record = CommitGraphRecord {
            generation: 2,
            root_tree: sha1_oid(SHA1_A),
            parents: vec![sha1_oid(SHA1_B)],
        };
        let mut bytes = record.encode();
        bytes.truncate(bytes.len() - 5);
        assert_eq!(
            CommitGraphRecord::decode(&bytes, gix_hash::Kind::Sha1),
            Err(ValueError::Truncated)
        );
    }

    #[test]
    fn commit_graph_record_rejects_trailing_bytes() {
        let record = CommitGraphRecord {
            generation: 1,
            root_tree: sha1_oid(SHA1_A),
            parents: vec![],
        };
        let mut bytes = record.encode();
        bytes.push(0xAA);
        assert_eq!(
            CommitGraphRecord::decode(&bytes, gix_hash::Kind::Sha1),
            Err(ValueError::TrailingBytes)
        );
    }
}
