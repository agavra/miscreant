//! Storage value encoding and decoding.
//!
//! Every value begins with a one-byte tag identifying how to interpret the
//! remaining bytes. See `docs/0001-init.md` §Storage for the per-segment
//! Value Layout tables, and this repo's "Fixed decisions" for the
//! clarifications binding here (big-endian integers throughout).
//!
//! An object record's tag carries the git object kind, so kind and size are
//! readable without inflating anything. Every inline object payload is the
//! object's content-length (a big-endian `u64`) followed by the zlib stream of
//! its bare content — no git `<type> <size>\0` header, matching what a pack
//! entry carries. This module only frames those bytes; the zlib codec lives in
//! [`crate::storage::zlib`].

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

/// An object-segment value: a git object's storage record. Every inline
/// variant carries the object's uncompressed content length plus the zlib
/// stream of its bare content (no `<type> <size>\0` header); the tag names the
/// kind, so both kind and size are available without inflating.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectRecord {
    /// A blob stored inline.
    BlobInline {
        /// The blob's content size in bytes.
        uncompressed_len: u64,
        /// The zlib stream of the blob's content.
        zlib: Bytes,
    },
    /// A blob offloaded to object storage under `blobs/<xx>/<rest>`, where its
    /// content lives as a zlib stream. The payload carries only its content
    /// size, so size queries never need an object-storage round trip.
    BlobPointer {
        /// The blob's content size in bytes.
        size: u64,
    },
    /// A tree object.
    Tree {
        /// The tree's content size in bytes.
        uncompressed_len: u64,
        /// The zlib stream of the tree's body.
        zlib: Bytes,
    },
    /// A commit object.
    Commit {
        /// The commit's content size in bytes.
        uncompressed_len: u64,
        /// The zlib stream of the commit's body.
        zlib: Bytes,
    },
    /// A tag object.
    Tag {
        /// The tag's content size in bytes.
        uncompressed_len: u64,
        /// The zlib stream of the tag's body.
        zlib: Bytes,
    },
}

impl ObjectRecord {
    /// The `gix_object::Kind` this record represents.
    pub fn kind(&self) -> ObjectKind {
        match self {
            ObjectRecord::BlobInline { .. } | ObjectRecord::BlobPointer { .. } => ObjectKind::Blob,
            ObjectRecord::Tree { .. } => ObjectKind::Tree,
            ObjectRecord::Commit { .. } => ObjectKind::Commit,
            ObjectRecord::Tag { .. } => ObjectKind::Tag,
        }
    }

    /// The object's content size in bytes. Never touches the blob content
    /// store and never inflates: every variant carries its uncompressed
    /// content length in the record itself.
    pub fn size(&self) -> u64 {
        match self {
            ObjectRecord::BlobPointer { size } => *size,
            ObjectRecord::BlobInline {
                uncompressed_len, ..
            }
            | ObjectRecord::Tree {
                uncompressed_len, ..
            }
            | ObjectRecord::Commit {
                uncompressed_len, ..
            }
            | ObjectRecord::Tag {
                uncompressed_len, ..
            } => *uncompressed_len,
        }
    }

    /// Encode this record as `object_tag | payload`.
    pub fn encode(&self) -> Vec<u8> {
        match self {
            ObjectRecord::BlobInline {
                uncompressed_len,
                zlib,
            } => encode_compressed(0x01, *uncompressed_len, zlib),
            ObjectRecord::BlobPointer { size } => {
                let mut buf = Vec::with_capacity(1 + 8);
                buf.push(0x02);
                buf.extend_from_slice(&size.to_be_bytes());
                buf
            }
            ObjectRecord::Tree {
                uncompressed_len,
                zlib,
            } => encode_compressed(0x03, *uncompressed_len, zlib),
            ObjectRecord::Commit {
                uncompressed_len,
                zlib,
            } => encode_compressed(0x04, *uncompressed_len, zlib),
            ObjectRecord::Tag {
                uncompressed_len,
                zlib,
            } => encode_compressed(0x05, *uncompressed_len, zlib),
        }
    }

    /// Decode an `object_tag | payload` byte string.
    pub fn decode(bytes: &[u8]) -> Result<Self, ValueError> {
        let (&tag, rest) = bytes.split_first().ok_or(ValueError::Empty)?;
        match tag {
            0x01 => {
                let (uncompressed_len, zlib) = decode_compressed(rest)?;
                Ok(ObjectRecord::BlobInline {
                    uncompressed_len,
                    zlib,
                })
            }
            0x02 => match rest.len().cmp(&8) {
                std::cmp::Ordering::Less => Err(ValueError::Truncated),
                std::cmp::Ordering::Equal => Ok(ObjectRecord::BlobPointer {
                    size: read_u64_be(rest),
                }),
                std::cmp::Ordering::Greater => Err(ValueError::TrailingBytes),
            },
            0x03 => {
                let (uncompressed_len, zlib) = decode_compressed(rest)?;
                Ok(ObjectRecord::Tree {
                    uncompressed_len,
                    zlib,
                })
            }
            0x04 => {
                let (uncompressed_len, zlib) = decode_compressed(rest)?;
                Ok(ObjectRecord::Commit {
                    uncompressed_len,
                    zlib,
                })
            }
            0x05 => {
                let (uncompressed_len, zlib) = decode_compressed(rest)?;
                Ok(ObjectRecord::Tag {
                    uncompressed_len,
                    zlib,
                })
            }
            other => Err(ValueError::UnknownTag(other)),
        }
    }
}

/// Frame an inline object payload: `tag | uncompressed_len (u64 BE) | zlib`.
fn encode_compressed(tag: u8, uncompressed_len: u64, zlib: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 8 + zlib.len());
    buf.push(tag);
    buf.extend_from_slice(&uncompressed_len.to_be_bytes());
    buf.extend_from_slice(zlib);
    buf
}

/// Split an inline object payload (the bytes after the tag) into its
/// uncompressed length and zlib stream. The stream itself may be empty (an
/// empty object still deflates to a non-empty stream, but a truncated record
/// might not), so only the fixed-width length prefix is length-checked.
fn decode_compressed(rest: &[u8]) -> Result<(u64, Bytes), ValueError> {
    if rest.len() < 8 {
        return Err(ValueError::Truncated);
    }
    let uncompressed_len = read_u64_be(&rest[..8]);
    Ok((uncompressed_len, Bytes::copy_from_slice(&rest[8..])))
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
    fn should_encode_meta_value_utf8_bytes_exactly() {
        // given/when
        let v = MetaValue::Utf8("sha1".to_owned());

        // then
        let mut expected = vec![0x01u8];
        expected.extend_from_slice(b"sha1");
        assert_eq!(v.encode(), expected);
    }

    #[test]
    fn should_encode_meta_value_u8_bytes_exactly() {
        // given/when
        let v = MetaValue::U8(7);

        // then
        assert_eq!(v.encode(), vec![0x02, 0x07]);
    }

    #[test]
    fn should_encode_meta_value_u32_bytes_exactly() {
        // given/when
        let v = MetaValue::U32(1);

        // then
        assert_eq!(v.encode(), vec![0x03, 0x00, 0x00, 0x00, 0x01]);
    }

    #[test]
    fn should_encode_meta_value_raw_bytes_exactly() {
        // given/when
        // e.g. next-repo-id / repo:<name> per Fixed decisions: raw u64 BE.
        let v = MetaValue::Raw(1u64.to_be_bytes().to_vec());

        // then
        let mut expected = vec![0x04u8];
        expected.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 1]);
        assert_eq!(v.encode(), expected);
    }

    #[test]
    fn should_round_trip_every_meta_value_variant() {
        // given/when/then: each variant is its own encode -> decode -> compare cycle.
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
    fn should_reject_unknown_meta_value_tag() {
        // given
        let bytes = [0xffu8];

        // when
        let result = MetaValue::decode(&bytes);

        // then
        assert_eq!(result, Err(ValueError::UnknownTag(0xff)));
    }

    #[test]
    fn should_reject_empty_meta_value() {
        // given/when
        let result = MetaValue::decode(&[]);

        // then
        assert_eq!(result, Err(ValueError::Empty));
    }

    #[test]
    fn should_reject_truncated_meta_value_u32() {
        // given
        let bytes = [0x03, 0x00, 0x00];

        // when
        let result = MetaValue::decode(&bytes);

        // then
        assert_eq!(result, Err(ValueError::Truncated));
    }

    #[test]
    fn should_reject_meta_value_u8_with_trailing_bytes() {
        // given
        let bytes = [0x02, 0x01, 0x02];

        // when
        let result = MetaValue::decode(&bytes);

        // then
        assert_eq!(result, Err(ValueError::TrailingBytes));
    }

    #[test]
    fn should_reject_invalid_utf8_meta_value() {
        // given
        let bytes = [0x01, 0xff];

        // when
        let result = MetaValue::decode(&bytes);

        // then
        assert_eq!(result, Err(ValueError::InvalidUtf8));
    }

    // --- ObjectRecord ---

    /// The zlib stream of `content` at the default level, for building record
    /// fixtures whose payload is a real (inflatable) stream.
    fn zlib(content: &[u8]) -> Bytes {
        Bytes::from(crate::storage::zlib::deflate(content, 6))
    }

    #[test]
    fn should_encode_blob_inline_record_bytes_exactly() {
        // given/when
        let stream = zlib(b"hello");
        let record = ObjectRecord::BlobInline {
            uncompressed_len: 5,
            zlib: stream.clone(),
        };

        // then: tag, the big-endian content length, then the zlib stream
        let mut expected = vec![0x01u8];
        expected.extend_from_slice(&5u64.to_be_bytes());
        expected.extend_from_slice(&stream);
        assert_eq!(record.encode(), expected);
        assert_eq!(record.kind(), ObjectKind::Blob);
        assert_eq!(record.size(), 5);
    }

    #[test]
    fn should_encode_blob_pointer_record_bytes_exactly() {
        // given/when
        let record = ObjectRecord::BlobPointer { size: 131_072 };

        // then
        let mut expected = vec![0x02u8];
        expected.extend_from_slice(&131_072u64.to_be_bytes());
        assert_eq!(record.encode(), expected);
        assert_eq!(record.kind(), ObjectKind::Blob);
        assert_eq!(record.size(), 131_072);
    }

    #[test]
    fn should_encode_tree_record_bytes_exactly() {
        // given/when
        let stream = zlib(b"tree-body-bytes");
        let record = ObjectRecord::Tree {
            uncompressed_len: 15,
            zlib: stream.clone(),
        };

        // then
        let mut expected = vec![0x03u8];
        expected.extend_from_slice(&15u64.to_be_bytes());
        expected.extend_from_slice(&stream);
        assert_eq!(record.encode(), expected);
        assert_eq!(record.kind(), ObjectKind::Tree);
        assert_eq!(record.size(), 15);
    }

    #[test]
    fn should_encode_commit_record_bytes_exactly() {
        // given/when
        let stream = zlib(b"commit-body-bytes");
        let record = ObjectRecord::Commit {
            uncompressed_len: 17,
            zlib: stream.clone(),
        };

        // then
        let mut expected = vec![0x04u8];
        expected.extend_from_slice(&17u64.to_be_bytes());
        expected.extend_from_slice(&stream);
        assert_eq!(record.encode(), expected);
        assert_eq!(record.kind(), ObjectKind::Commit);
    }

    #[test]
    fn should_encode_tag_record_bytes_exactly() {
        // given/when
        let stream = zlib(b"tag-body-bytes");
        let record = ObjectRecord::Tag {
            uncompressed_len: 14,
            zlib: stream.clone(),
        };

        // then
        let mut expected = vec![0x05u8];
        expected.extend_from_slice(&14u64.to_be_bytes());
        expected.extend_from_slice(&stream);
        assert_eq!(record.encode(), expected);
        assert_eq!(record.kind(), ObjectKind::Tag);
    }

    #[test]
    fn should_round_trip_every_object_record_variant() {
        // given/when/then: each variant is its own encode -> decode -> compare cycle.
        for record in [
            ObjectRecord::BlobInline {
                uncompressed_len: 5,
                zlib: zlib(b"hello"),
            },
            ObjectRecord::BlobPointer { size: 999 },
            ObjectRecord::Tree {
                uncompressed_len: 9,
                zlib: zlib(b"tree-body"),
            },
            ObjectRecord::Commit {
                uncompressed_len: 11,
                zlib: zlib(b"commit-body"),
            },
            ObjectRecord::Tag {
                uncompressed_len: 8,
                zlib: zlib(b"tag-body"),
            },
        ] {
            assert_eq!(
                ObjectRecord::decode(&record.encode()).expect("decodes"),
                record
            );
        }
    }

    #[test]
    fn should_round_trip_inline_record_through_encode_decode_inflate_to_original_content() {
        // given: real content, deflated into an inline commit record
        let content = b"tree cafebabe\nauthor A U Thor <a@example.com> 1 +0000\n\nhi\n";
        let record = ObjectRecord::Commit {
            uncompressed_len: content.len() as u64,
            zlib: zlib(content),
        };

        // when: the record is serialized, read back, and its stream inflated
        let decoded = ObjectRecord::decode(&record.encode()).expect("decodes");
        let ObjectRecord::Commit {
            uncompressed_len,
            zlib,
        } = &decoded
        else {
            panic!("decoded to the wrong variant: {decoded:?}");
        };
        let restored =
            crate::storage::zlib::inflate(zlib, *uncompressed_len).expect("inflates cleanly");

        // then: the original content comes back, kind/size read without inflating
        assert_eq!(restored.as_ref(), content.as_slice());
        assert_eq!(decoded.kind(), ObjectKind::Commit);
        assert_eq!(decoded.size(), content.len() as u64);
    }

    #[test]
    fn should_reject_truncated_inline_object_record() {
        // given: a tag byte with fewer than the 8 length-prefix bytes
        let bytes = [0x01, 0x00, 0x00, 0x00];

        // when
        let result = ObjectRecord::decode(&bytes);

        // then
        assert_eq!(result, Err(ValueError::Truncated));
    }

    #[test]
    fn should_reject_unknown_object_record_tag() {
        // given
        let bytes = [0x00u8];

        // when
        let result = ObjectRecord::decode(&bytes);

        // then
        assert_eq!(result, Err(ValueError::UnknownTag(0x00)));
    }

    #[test]
    fn should_reject_empty_object_record() {
        // given/when
        let result = ObjectRecord::decode(&[]);

        // then
        assert_eq!(result, Err(ValueError::Empty));
    }

    #[test]
    fn should_reject_truncated_blob_pointer_record() {
        // given
        let bytes = [0x02, 0x00, 0x00];

        // when
        let result = ObjectRecord::decode(&bytes);

        // then
        assert_eq!(result, Err(ValueError::Truncated));
    }

    #[test]
    fn should_reject_blob_pointer_record_with_trailing_bytes() {
        // given
        let mut bytes = vec![0x02u8];
        bytes.extend_from_slice(&99u64.to_be_bytes());
        bytes.push(0xAA);

        // when
        let result = ObjectRecord::decode(&bytes);

        // then
        assert_eq!(result, Err(ValueError::TrailingBytes));
    }

    // --- RefTarget ---

    #[test]
    fn should_encode_direct_ref_target_bytes_exactly_sha1() {
        // given
        let oid = sha1_oid(SHA1_A);
        let target = RefTarget::Direct(oid);

        // when/then
        let mut expected = vec![0x01u8];
        expected.extend_from_slice(oid.as_bytes());
        assert_eq!(target.encode(), expected);
    }

    #[test]
    fn should_encode_direct_ref_target_bytes_exactly_sha256() {
        // given
        let oid = sha256_oid(SHA256_A);
        let target = RefTarget::Direct(oid);

        // when/then
        let mut expected = vec![0x01u8];
        expected.extend_from_slice(oid.as_bytes());
        assert_eq!(target.encode(), expected);
    }

    #[test]
    fn should_encode_reference_ref_target_bytes_exactly() {
        // given/when
        let target = RefTarget::Reference("refs/heads/main".to_owned());

        // then
        let mut expected = vec![0x02u8];
        expected.extend_from_slice(b"refs/heads/main");
        assert_eq!(target.encode(), expected);
    }

    #[test]
    fn should_round_trip_ref_target_at_both_hash_widths() {
        // given/when/then: each direct oid is its own encode -> decode -> compare cycle.
        for oid in [sha1_oid(SHA1_A), sha256_oid(SHA256_A)] {
            let target = RefTarget::Direct(oid);
            assert_eq!(
                RefTarget::decode(&target.encode()).expect("decodes"),
                target
            );
        }

        // given/when/then: same cycle for a symbolic reference.
        let symref = RefTarget::Reference("HEAD-target".to_owned());
        assert_eq!(
            RefTarget::decode(&symref.encode()).expect("decodes"),
            symref
        );
    }

    #[test]
    fn should_reject_unknown_ref_target_tag() {
        // given
        let bytes = [0x03u8];

        // when
        let result = RefTarget::decode(&bytes);

        // then
        assert_eq!(result, Err(ValueError::UnknownTag(0x03)));
    }

    #[test]
    fn should_reject_truncated_direct_ref_target() {
        // given
        let oid = sha1_oid(SHA1_A);
        let mut bytes = vec![0x01u8];
        bytes.extend_from_slice(&oid.as_bytes()[..19]);

        // when
        let result = RefTarget::decode(&bytes);

        // then
        assert_eq!(result, Err(ValueError::InvalidObjectId));
    }

    #[test]
    fn should_reject_invalid_utf8_reference_target() {
        // given
        let bytes = [0x02, 0xff];

        // when
        let result = RefTarget::decode(&bytes);

        // then
        assert_eq!(result, Err(ValueError::InvalidUtf8));
    }

    // --- CommitGraphRecord ---

    #[test]
    fn should_encode_root_commit_graph_record_bytes_exactly_sha1() {
        // given
        let root_tree = sha1_oid(SHA1_A);
        let record = CommitGraphRecord {
            generation: 1,
            root_tree,
            parents: vec![],
        };

        // when/then
        let mut expected = vec![0x00, 0x00, 0x00, 0x01]; // generation = 1
        expected.extend_from_slice(root_tree.as_bytes());
        expected.push(0x00); // parent_count = 0
        assert_eq!(record.encode(), expected);
    }

    #[test]
    fn should_encode_merge_commit_graph_record_bytes_exactly_sha1() {
        // given
        let root_tree = sha1_oid(SHA1_A);
        let parent_a = sha1_oid(SHA1_B);
        let parent_b = sha1_oid(SHA1_A);
        let record = CommitGraphRecord {
            generation: 3,
            root_tree,
            parents: vec![parent_a, parent_b],
        };

        // when/then
        let mut expected = vec![0x00, 0x00, 0x00, 0x03]; // generation = 3
        expected.extend_from_slice(root_tree.as_bytes());
        expected.push(0x02); // parent_count = 2
        expected.extend_from_slice(parent_a.as_bytes());
        expected.extend_from_slice(parent_b.as_bytes());
        assert_eq!(record.encode(), expected);
    }

    #[test]
    fn should_encode_commit_graph_record_bytes_exactly_sha256() {
        // given
        let root_tree = sha256_oid(SHA256_A);
        let parent = sha256_oid(SHA256_B);
        let record = CommitGraphRecord {
            generation: 2,
            root_tree,
            parents: vec![parent],
        };

        // when/then
        let mut expected = vec![0x00, 0x00, 0x00, 0x02];
        expected.extend_from_slice(root_tree.as_bytes());
        expected.push(0x01);
        expected.extend_from_slice(parent.as_bytes());
        assert_eq!(record.encode(), expected);
    }

    #[test]
    fn should_round_trip_commit_graph_record_at_both_hash_widths() {
        // given
        let sha1_record = CommitGraphRecord {
            generation: 5,
            root_tree: sha1_oid(SHA1_A),
            parents: vec![sha1_oid(SHA1_B)],
        };

        // when/then
        assert_eq!(
            CommitGraphRecord::decode(&sha1_record.encode(), gix_hash::Kind::Sha1)
                .expect("decodes"),
            sha1_record
        );

        // given
        let sha256_record = CommitGraphRecord {
            generation: 6,
            root_tree: sha256_oid(SHA256_A),
            parents: vec![sha256_oid(SHA256_B), sha256_oid(SHA256_A)],
        };

        // when/then
        assert_eq!(
            CommitGraphRecord::decode(&sha256_record.encode(), gix_hash::Kind::Sha256)
                .expect("decodes"),
            sha256_record
        );
    }

    #[test]
    fn should_reject_truncated_commit_graph_header() {
        // given
        let bytes = [0x00, 0x00, 0x00, 0x01];

        // when
        let result = CommitGraphRecord::decode(&bytes, gix_hash::Kind::Sha1);

        // then
        assert_eq!(result, Err(ValueError::Truncated));
    }

    #[test]
    fn should_reject_truncated_commit_graph_parents() {
        // given
        let record = CommitGraphRecord {
            generation: 2,
            root_tree: sha1_oid(SHA1_A),
            parents: vec![sha1_oid(SHA1_B)],
        };
        let mut bytes = record.encode();
        bytes.truncate(bytes.len() - 5);

        // when
        let result = CommitGraphRecord::decode(&bytes, gix_hash::Kind::Sha1);

        // then
        assert_eq!(result, Err(ValueError::Truncated));
    }

    #[test]
    fn should_reject_commit_graph_record_with_trailing_bytes() {
        // given
        let record = CommitGraphRecord {
            generation: 1,
            root_tree: sha1_oid(SHA1_A),
            parents: vec![],
        };
        let mut bytes = record.encode();
        bytes.push(0xAA);

        // when
        let result = CommitGraphRecord::decode(&bytes, gix_hash::Kind::Sha1);

        // then
        assert_eq!(result, Err(ValueError::TrailingBytes));
    }
}
