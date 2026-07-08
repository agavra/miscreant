//! Pack serialization for the fetch response.
//!
//! See `docs/0001-init.md` §Packfiles: objects are packed on the send side
//! after the walk has chosen them. Every entry is a full (non-delta)
//! zlib-compressed base object — the server never produces deltas — so the
//! pack's correctness does not depend on entry order, and the stream is a
//! version-2 pack header, one entry per object, and a trailing checksum over
//! the whole pack.

use std::cell::Cell;

use bytes::Bytes;
use gix_hash::ObjectId;
use gix_object::Kind;
use gix_pack::data::Version;
use gix_pack::data::output::{self, bytes::FromEntriesIter};

/// A failure while assembling or writing a pack stream.
#[derive(Debug, thiserror::Error)]
pub enum PackOutError<E: std::error::Error + 'static> {
    /// The input stream failed to produce an object.
    #[error(transparent)]
    Input(E),
    /// An object body could not be encoded as a pack entry.
    #[error("cannot build pack entry for {oid}")]
    Entry {
        /// The object whose entry failed to encode.
        oid: ObjectId,
        /// The underlying compression/encoding failure.
        #[source]
        source: output::entry::Error,
    },
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
/// `num_objects` entries, one full (non-delta) zlib-compressed entry per
/// object drawn from `objects` in iteration order, and the trailing
/// `object_hash` checksum. Compression happens on the calling thread as each
/// object is consumed, so the pack streams without ever being held in memory
/// whole. Returns the pack's trailing checksum.
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
    I: Iterator<Item = Result<(ObjectId, Kind, Bytes), E>>,
    W: std::io::Write,
    E: std::error::Error + 'static,
{
    let written = Cell::new(0u32);
    let entries = objects.map(|item| {
        let (id, kind, body) = item.map_err(PackOutError::Input)?;
        let count = output::Count::from_data(id, None);
        let data = gix_object::Data {
            kind,
            object_hash: id.kind(),
            data: &body,
        };
        let entry = output::Entry::from_data(&count, &data)
            .map_err(|source| PackOutError::Entry { oid: id, source })?;
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

    /// Hash `body` as a real git object of `kind`.
    fn oid_of(kind: Kind, body: &[u8]) -> ObjectId {
        gix_object::compute_hash(gix_hash::Kind::Sha1, kind, body).expect("hash object")
    }

    /// An infallible input item for [`write_pack`].
    fn item(kind: Kind, body: &'static [u8]) -> Result<(ObjectId, Kind, Bytes), Infallible> {
        Ok((oid_of(kind, body), kind, Bytes::from_static(body)))
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
        // given: three objects of distinct kinds
        let inputs = [
            item(Kind::Blob, b"some file content\n"),
            item(
                Kind::Commit,
                b"tree 0000000000000000000000000000000000000000\n",
            ),
            item(
                Kind::Tag,
                b"object 0000000000000000000000000000000000000000\n",
            ),
        ];
        let mut out = Vec::new();

        // when
        write_pack(inputs.iter().cloned(), 3, gix_hash::Kind::Sha1, &mut out).expect("write pack");

        // then: a verifying pack reader accepts the stream and every entry is
        // a full (non-delta) object whose bytes inflate back to the input
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
        for ((entry, expected_kind), input) in entries.iter().zip(expected_kinds).zip(&inputs) {
            let (_, _, body) = input.as_ref().expect("infallible input");
            assert_eq!(entry.header, expected_kind);
            assert_eq!(entry.decompressed_size, body.len() as u64);
            let compressed = entry.compressed.as_ref().expect("kept entry bytes");
            let mut inflated = vec![0u8; body.len()];
            let (_, _, written) = Inflate::default()
                .once(compressed, &mut inflated)
                .expect("inflate entry");
            assert_eq!(&inflated[..written], body.as_ref());
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
            Ok((
                oid_of(Kind::Blob, b"fine"),
                Kind::Blob,
                Bytes::from_static(b"fine"),
            )),
            Err(LookupFailed),
        ];
        let mut out = Vec::new();

        // when
        let result = write_pack(inputs.into_iter(), 2, gix_hash::Kind::Sha1, &mut out);

        // then
        assert!(matches!(result, Err(PackOutError::Input(LookupFailed))));
    }
}
