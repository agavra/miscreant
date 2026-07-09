//! Pack serialization for the fetch response.
//!
//! See `docs/0001-init.md` §Packfiles: objects are packed on the send side
//! after the walk has chosen them. Every entry is a full (non-delta) base
//! object — the server never produces deltas — so the pack's correctness does
//! not depend on entry order, and the stream is a version-2 pack header, one
//! entry per object, and a trailing checksum over the whole pack.
//!
//! Each object's zlib stream was computed once at write time and is stored
//! ready to pack, so entries are copied out verbatim behind their type/size
//! header — the pack writer never recompresses.

use std::cell::Cell;

use bytes::Bytes;
use gix_hash::ObjectId;
use gix_object::Kind;
use gix_pack::data::Version;
use gix_pack::data::output::{self, bytes::FromEntriesIter};

/// A ready-to-pack object: its id and kind, the size its content decompresses
/// to, and the zlib stream copied verbatim into the pack body.
#[derive(Debug, Clone)]
pub struct PackEntry {
    /// The object's id.
    pub id: ObjectId,
    /// The object's git kind.
    pub kind: Kind,
    /// The object's uncompressed content size, written into the entry header.
    pub decompressed_size: u64,
    /// The zlib stream of the object's content.
    pub zlib: Bytes,
}

/// A failure while assembling or writing a pack stream.
#[derive(Debug, thiserror::Error)]
pub enum PackOutError<E: std::error::Error + 'static> {
    /// The input stream failed to produce an object.
    #[error(transparent)]
    Input(E),
    /// Writing pack bytes to the output (or hashing them for the trailing
    /// checksum) failed.
    #[error("cannot write pack stream")]
    Write(#[source] gix_hash::io::Error),
    /// The input ended before the promised number of objects arrived. The
    /// already-written header names `expected`, so the emitted bytes are not
    /// a usable pack.
    #[error("pack input ended after {written} of {expected} objects")]
    Truncated {
        /// How many entries were actually written.
        written: u32,
        /// How many entries the pack header promised.
        expected: u32,
    },
}

/// Write a complete pack to `out`: a version-2 header declaring
/// `num_objects` entries, one full (non-delta) entry per [`PackEntry`] drawn
/// from `objects` in iteration order, and the trailing `object_hash`
/// checksum. Each entry's stored zlib stream is copied straight into the pack
/// behind its type/size header — no compression happens here — so the pack
/// streams out without ever being held in memory whole. Returns the pack's
/// trailing checksum.
///
/// `objects` must yield exactly `num_objects` items; ending early fails with
/// [`PackOutError::Truncated`] (the output is already unusable at that point
/// because the header's count is wrong).
pub fn write_pack<I, W, E>(
    objects: I,
    num_objects: u32,
    object_hash: gix_hash::Kind,
    out: W,
) -> Result<ObjectId, PackOutError<E>>
where
    I: Iterator<Item = Result<PackEntry, E>>,
    W: std::io::Write,
    E: std::error::Error + 'static,
{
    let written = Cell::new(0u32);
    let entries = objects.map(|item| {
        let PackEntry {
            id,
            kind,
            decompressed_size,
            zlib,
        } = item.map_err(PackOutError::Input)?;
        // gix-pack's writer emits the type/size header from `kind` and
        // `decompressed_size`, then copies `compressed_data` verbatim.
        let entry = output::Entry {
            id,
            kind: output::entry::Kind::Base(kind),
            decompressed_size: decompressed_size as usize,
            compressed_data: zlib.to_vec(),
        };
        written.set(written.get() + 1);
        Ok(vec![entry])
    });

    let mut writer = FromEntriesIter::new(entries, out, num_objects, Version::V2, object_hash);
    for step in &mut writer {
        step.map_err(|err| match err {
            output::bytes::Error::Io(source) => PackOutError::Write(source),
            output::bytes::Error::Input(inner) => inner,
        })?;
    }

    if written.get() != num_objects {
        return Err(PackOutError::Truncated {
            written: written.get(),
            expected: num_objects,
        });
    }
    // The iterator only finishes after writing the trailing checksum, so a
    // missing digest cannot be reached from here; fail loudly regardless.
    writer.digest().ok_or(PackOutError::Truncated {
        written: written.get(),
        expected: num_objects,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use std::io::BufReader;

    use gix_features::zlib::Inflate;
    use gix_pack::data::entry::Header;
    use gix_pack::data::input::{BytesToEntriesIter, EntryDataMode, Mode};

    use crate::storage::zlib::deflate;

    /// Hash `body` as a real git object of `kind`.
    fn oid_of(kind: Kind, body: &[u8]) -> ObjectId {
        gix_object::compute_hash(gix_hash::Kind::Sha1, kind, body).expect("hash object")
    }

    /// An infallible input item for [`write_pack`], carrying `body`'s zlib
    /// stream as a stored object would.
    fn item(kind: Kind, body: &'static [u8]) -> Result<PackEntry, Infallible> {
        Ok(PackEntry {
            id: oid_of(kind, body),
            kind,
            decompressed_size: body.len() as u64,
            zlib: Bytes::from(deflate(body, 6)),
        })
    }

    #[test]
    fn should_write_an_empty_pack_with_header_and_trailer_only() {
        // given: no objects at all
        let mut out = Vec::new();

        // when
        let digest = write_pack(
            std::iter::empty::<Result<_, Infallible>>(),
            0,
            gix_hash::Kind::Sha1,
            &mut out,
        )
        .expect("write empty pack");

        // then: a 12-byte v2 header declaring zero objects, then the 20-byte
        // SHA-1 trailer over the header, and nothing else
        assert_eq!(out.len(), 32);
        assert_eq!(&out[..4], b"PACK");
        assert_eq!(&out[4..8], &2u32.to_be_bytes());
        assert_eq!(&out[8..12], &0u32.to_be_bytes());
        assert_eq!(&out[12..], digest.as_slice());
    }

    #[test]
    fn should_write_full_entries_that_a_pack_reader_round_trips() {
        // given: three objects of distinct kinds, each carrying its zlib stream
        let bodies: [(Kind, &'static [u8]); 3] = [
            (Kind::Blob, b"some file content\n"),
            (
                Kind::Commit,
                b"tree 0000000000000000000000000000000000000000\n",
            ),
            (
                Kind::Tag,
                b"object 0000000000000000000000000000000000000000\n",
            ),
        ];
        let inputs: Vec<_> = bodies
            .iter()
            .map(|(kind, body)| item(*kind, body))
            .collect();
        let mut out = Vec::new();

        // when
        write_pack(inputs.into_iter(), 3, gix_hash::Kind::Sha1, &mut out).expect("write pack");

        // then: a verifying pack reader accepts the stream and every entry is
        // a full (non-delta) object whose stored stream inflates back to the
        // input content
        let entries: Vec<_> = BytesToEntriesIter::new_from_header(
            BufReader::new(out.as_slice()),
            Mode::Verify,
            EntryDataMode::Keep,
            gix_hash::Kind::Sha1,
        )
        .expect("read pack header")
        .collect::<Result<_, _>>()
        .expect("verify pack entries");
        assert_eq!(entries.len(), 3);
        let expected_kinds = [Header::Blob, Header::Commit, Header::Tag];
        for ((entry, expected_kind), (_, body)) in entries.iter().zip(expected_kinds).zip(bodies) {
            assert_eq!(entry.header, expected_kind);
            assert_eq!(entry.decompressed_size, body.len() as u64);
            let compressed = entry.compressed.as_ref().expect("kept entry bytes");
            let mut inflated = vec![0u8; body.len()];
            let (_, _, written) = Inflate::default()
                .once(compressed, &mut inflated)
                .expect("inflate entry");
            assert_eq!(&inflated[..written], body);
        }
    }

    #[test]
    fn should_report_the_declared_count_in_the_header() {
        // given: two objects promised and provided
        let inputs = [item(Kind::Blob, b"a"), item(Kind::Blob, b"bb")];
        let mut out = Vec::new();

        // when
        write_pack(inputs.into_iter(), 2, gix_hash::Kind::Sha1, &mut out).expect("write pack");

        // then
        assert_eq!(&out[8..12], &2u32.to_be_bytes());
    }

    #[test]
    fn should_fail_when_input_ends_before_the_promised_count() {
        // given: a header promising three objects but only one available
        let inputs = [item(Kind::Blob, b"only one")];
        let mut out = Vec::new();

        // when
        let result = write_pack(inputs.into_iter(), 3, gix_hash::Kind::Sha1, &mut out);

        // then: the shortfall is a hard error, not a silently short pack
        assert!(matches!(
            result,
            Err(PackOutError::Truncated {
                written: 1,
                expected: 3
            })
        ));
    }

    #[test]
    fn should_surface_input_errors() {
        // given: an input stream that fails on its second item
        #[derive(Debug, thiserror::Error)]
        #[error("lookup failed")]
        struct LookupFailed;
        let inputs = vec![
            item(Kind::Blob, b"fine").map_err(|_| LookupFailed),
            Err(LookupFailed),
        ];
        let mut out = Vec::new();

        // when
        let result = write_pack(inputs.into_iter(), 2, gix_hash::Kind::Sha1, &mut out);

        // then
        assert!(matches!(result, Err(PackOutError::Input(LookupFailed))));
    }
}
