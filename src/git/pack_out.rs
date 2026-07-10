//! Pack serialization for the fetch response.
//!
//! See `docs/0001-init.md` §Packfiles: objects are packed on the send side
//! after the walk has chosen them. Full entries copy their stored zlib streams
//! verbatim; scheduled blobs may instead be emitted as in-pack `OFS_DELTA`
//! entries against an earlier full blob. The stream is a version-2 pack
//! header, one entry per object, and a trailing checksum over the whole pack.
//!
//! Each object's zlib stream was computed once at write time and is stored
//! ready to pack, so entries are copied out verbatim behind their type/size
//! header — the pack writer never recompresses.

use std::cell::Cell;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

use bytes::Bytes;
use gix_hash::ObjectId;
use gix_object::Kind;
use gix_pack::data::Version;
use gix_pack::data::output::{self, bytes::FromEntriesIter};

use crate::storage::zlib;

/// Only blobs in this range take part in delta planning. The upper bound
/// limits both the temporary inflated target and the retained base cache.
const MIN_DELTA_BLOB_SIZE: u64 = 512;
const MAX_DELTA_BLOB_SIZE: u64 = 2 * 1024 * 1024;
/// At most this many inflated full blobs are considered as prior bases.
const DELTA_WINDOW: usize = 8;
/// The cache is byte-bounded as well as entry-bounded so a sequence of large
/// blobs never makes a request retain an unbounded amount of content.
const MAX_DELTA_CACHE_BYTES: usize = 8 * 1024 * 1024;
/// Avoid changing the entry form for negligible savings once its pack header
/// and offset are included.
const MIN_DELTA_SAVINGS: usize = 32;
/// Minimum exact match that is worth encoding as a copy instruction. This is
/// also the block-hash window over which both sides of a comparison agree.
const MIN_COPY: usize = 16;
/// At most this many base offsets are indexed per block tag.
const MAX_BLOCK_MATCHES: usize = 4;
/// A block index entry lives within this many slots of its home slot, and a
/// lookup examines exactly this many.
const PROBE_WINDOW: usize = 8;
/// Odd multiplier for the multiplicative Rabin-Karp block fingerprint; large
/// so windows spread across index slots.
const HASH_BASE: u64 = 0x9e37_79b9_7f4a_7c15;
/// `HASH_BASE` raised to the leading window position, the coefficient of the
/// byte that leaves the window when the hash rolls forward.
const HASH_TOP: u64 = wrapping_pow(HASH_BASE, (MIN_COPY - 1) as u32);

/// Wrapping exponentiation usable in a `const` initializer.
const fn wrapping_pow(base: u64, exp: u32) -> u64 {
    let mut acc = 1u64;
    let mut n = 0;
    while n < exp {
        acc = acc.wrapping_mul(base);
        n += 1;
    }
    acc
}

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
    /// The planner's path-name hash for a blob eligible for `OFS_DELTA`.
    /// `None` keeps the entry full; direct oid wants and all non-blobs use
    /// this form.
    pub delta_group: Option<u32>,
}

/// Pack-assembly behavior selected by the negotiated fetch capabilities.
#[derive(Debug, Clone, Copy, Default)]
pub struct PackOptions {
    /// Emit in-pack offset deltas for scheduled blobs. Only set when the
    /// client explicitly requested `ofs-delta`.
    pub ofs_deltas: bool,
}

/// Delta-planning work performed while writing one pack. Every field stays
/// zero unless `ofs_deltas` is set and at least one scheduled blob is
/// size-eligible; the full-entry fast path touches none of it.
#[derive(Debug, Clone, Copy, Default)]
pub struct DeltaStats {
    /// Size-eligible scheduled blobs inflated as delta targets.
    pub eligible: u64,
    /// Prior-base comparisons attempted across all eligible blobs.
    pub comparisons: u64,
    /// Entries emitted as offset deltas.
    pub emitted: u64,
    /// Blob bodies retained in the candidate window as future bases.
    pub inserted: u64,
    /// Stored bytes saved across emitted deltas: each entry's full zlib length
    /// minus its emitted delta's compressed length, summed.
    pub bytes_saved: u64,
    /// Time spent inflating eligible blob targets.
    pub inflate: Duration,
    /// Time spent building candidate base block indexes.
    pub index: Duration,
    /// Time spent scanning targets and building raw delta instructions.
    pub scan: Duration,
    /// Time spent deflating the smallest raw delta selected for each target.
    pub deflate: Duration,
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
    /// An object selected for delta planning had a stored zlib stream that
    /// did not inflate to its recorded length.
    #[error("cannot inflate stored blob {oid} for delta planning")]
    CorruptZlib {
        /// The selected blob id.
        oid: ObjectId,
    },
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
    let mut stats = DeltaStats::default();
    write_pack_with_options(
        objects,
        num_objects,
        object_hash,
        out,
        PackOptions::default(),
        &mut stats,
    )
}

/// Like [`write_pack`], with the negotiated pack representation options. Full
/// entries always preserve their stored zlib stream. With `ofs_deltas`, blobs
/// in the same planner group are considered against a bounded cache of
/// already-written full blobs; an `OFS_DELTA` is emitted only when its
/// compressed instruction stream wins materially over the full stream.
///
/// `stats` accumulates the delta-planning work done along the way, populated
/// as far as the write progressed even when it fails partway.
pub fn write_pack_with_options<I, W, E>(
    objects: I,
    num_objects: u32,
    object_hash: gix_hash::Kind,
    out: W,
    options: PackOptions,
    stats: &mut DeltaStats,
) -> Result<ObjectId, PackOutError<E>>
where
    I: Iterator<Item = Result<PackEntry, E>>,
    W: std::io::Write,
    E: std::error::Error + 'static,
{
    let written = Cell::new(0u32);
    let mut candidates = DeltaCandidates::default();
    let entries = objects.map(|item| {
        let object = item.map_err(PackOutError::Input)?;
        let entry_index = written.get() as usize;
        let entry = if options.ofs_deltas {
            delta_or_full(&object, entry_index, &mut candidates, stats)?
        } else {
            full_entry(&object)
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

/// A full entry copies the object-storage zlib stream verbatim.
fn full_entry(object: &PackEntry) -> output::Entry {
    output::Entry {
        id: object.id,
        kind: output::entry::Kind::Base(object.kind),
        decompressed_size: object.decompressed_size as usize,
        compressed_data: object.zlib.to_vec(),
    }
}

/// A prior full blob retained for delta planning. `entry_index` is the
/// zero-based index the pack writer uses to calculate the offset distance.
struct DeltaCandidate {
    entry_index: usize,
    group: u32,
    body: Bytes,
}

/// Bounded LRU-like candidate cache. The schedule groups similar names next
/// to each other, so simple oldest-first eviction keeps the relevant bases
/// while enforcing the request memory budget.
#[derive(Default)]
struct DeltaCandidates {
    entries: VecDeque<DeltaCandidate>,
    bytes: usize,
}

impl DeltaCandidates {
    fn matching(&self, group: u32) -> impl Iterator<Item = &DeltaCandidate> {
        self.entries
            .iter()
            .filter(move |candidate| candidate.group == group)
    }

    fn insert(&mut self, entry_index: usize, group: u32, body: Bytes) -> bool {
        if body.len() > MAX_DELTA_CACHE_BYTES {
            return false;
        }
        while self.bytes + body.len() > MAX_DELTA_CACHE_BYTES || self.entries.len() >= DELTA_WINDOW
        {
            let Some(evicted) = self.entries.pop_front() else {
                break;
            };
            self.bytes -= evicted.body.len();
        }
        self.bytes += body.len();
        self.entries.push_back(DeltaCandidate {
            entry_index,
            group,
            body,
        });
        true
    }
}

/// Create the smallest useful delta against a prior full blob in the same
/// name-hash group, or return the full entry. Candidates and targets are
/// inflated only when their scheduled blob is eligible; all other entries
/// retain the zero-inflate full-entry fast path.
fn delta_or_full<E: std::error::Error + 'static>(
    object: &PackEntry,
    entry_index: usize,
    candidates: &mut DeltaCandidates,
    stats: &mut DeltaStats,
) -> Result<output::Entry, PackOutError<E>> {
    let Some(group) = object.delta_group else {
        return Ok(full_entry(object));
    };
    if object.kind != Kind::Blob
        || !(MIN_DELTA_BLOB_SIZE..=MAX_DELTA_BLOB_SIZE).contains(&object.decompressed_size)
    {
        return Ok(full_entry(object));
    }

    stats.eligible += 1;
    let inflate_start = Instant::now();
    let body = zlib::inflate(&object.zlib, object.decompressed_size)
        .ok_or_else(|| PackOutError::CorruptZlib { oid: object.id })?;
    stats.inflate += inflate_start.elapsed();

    // A depth-1 delta larger than half the target is not worth keeping, and
    // each accepted candidate tightens the budget so a smaller raw delta is
    // required to displace it.
    let mut budget = body.len() / 2;
    let mut best: Option<(usize, Vec<u8>)> = None;
    for candidate in candidates.matching(group) {
        stats.comparisons += 1;

        let index_start = Instant::now();
        let index = block_index(&candidate.body);
        stats.index += index_start.elapsed();

        let scan_start = Instant::now();
        let delta = make_delta(&candidate.body, &body, &index, budget);
        stats.scan += scan_start.elapsed();

        if let Some(delta) = delta {
            budget = delta.len().saturating_sub(1);
            best = Some((candidate.entry_index, delta));
        }
    }

    if let Some((base_index, delta)) = best {
        let deflate_start = Instant::now();
        let compressed = zlib::deflate(&delta, 6);
        stats.deflate += deflate_start.elapsed();

        if compressed.len() + MIN_DELTA_SAVINGS < object.zlib.len() {
            stats.emitted += 1;
            stats.bytes_saved += (object.zlib.len() - compressed.len()) as u64;
            return Ok(output::Entry {
                id: object.id,
                kind: output::entry::Kind::DeltaRef {
                    object_index: base_index,
                },
                decompressed_size: delta.len(),
                compressed_data: compressed,
            });
        }
    }

    // A delta target is not a future base in this first implementation. This
    // keeps every chain at depth one and makes reconstruction cheap.
    if candidates.insert(entry_index, group, body) {
        stats.inserted += 1;
    }
    Ok(full_entry(object))
}

/// Produce Git's delta instruction stream for `target` relative to `base`,
/// given `index`, `base`'s fixed-block hash index, or `None` once the raw
/// output would exceed `max_size`. A rolling window hash walks the target: on
/// a miss the window slides one byte in O(1), on a hit the matched run is
/// copied and the window is re-primed just past it. Every hit is verified
/// byte-for-byte, unmatched bytes are encoded as inserts, and the final
/// `MIN_COPY`-byte tail that can no longer form a window is always an insert.
fn make_delta(base: &[u8], target: &[u8], index: &BlockIndex, max_size: usize) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    encode_size(base.len() as u64, &mut out);
    encode_size(target.len() as u64, &mut out);

    let mut cursor = 0;
    let mut insert_start = 0;
    let mut hash = 0u64;
    let mut primed = false;
    while cursor + MIN_COPY <= target.len() {
        // The committed output plus the pending literal run bounds the raw
        // delta so far; a comparison that has already lost is dropped here.
        if out.len() + (cursor - insert_start) > max_size {
            return None;
        }
        if !primed {
            hash = block_hash(&target[cursor..cursor + MIN_COPY]);
            primed = true;
        }

        let (window, tag) = index.window_of(hash);
        let mut nominated = false;
        for &entry in window {
            nominated |= entry >> 32 == tag;
        }

        let mut best = None;
        if nominated {
            for &entry in window {
                if entry >> 32 != tag || entry as u32 == 0 {
                    continue;
                }
                let offset = (entry as u32 - 1) as usize;
                let len = common_prefix(&base[offset..], &target[cursor..]);
                if len >= MIN_COPY && best.is_none_or(|(_, best_len)| len > best_len) {
                    best = Some((offset, len));
                }
            }
        }

        if let Some((offset, len)) = best {
            append_insert(&mut out, &target[insert_start..cursor]);
            append_copy(&mut out, offset, len);
            cursor += len;
            insert_start = cursor;
            primed = false;
        } else {
            let arriving = cursor + MIN_COPY;
            if arriving < target.len() {
                hash = roll_hash(hash, target[cursor], target[arriving]);
            }
            cursor += 1;
        }
    }
    append_insert(&mut out, &target[insert_start..]);
    (out.len() <= max_size).then_some(out)
}

/// Base block offsets keyed by their window fingerprint. Every entry lives
/// within [`PROBE_WINDOW`] slots of its fingerprint's home slot, so a lookup
/// examines exactly that many slots with branch-free tag compares — the
/// target scan probes once per position, and a per-slot branch that depends
/// on occupancy would stall it on mispredictions.
struct BlockIndex {
    /// Packed entries, `tag << 32 | offset + 1`; zero marks a free slot. The
    /// length is a power of two plus [`PROBE_WINDOW`] trailing slots so a
    /// probe window never wraps.
    slots: Vec<u64>,
    /// Right shift that turns a mixed fingerprint into its home slot.
    shift: u32,
}

impl BlockIndex {
    fn home_of(&self, fingerprint: u64) -> usize {
        (fingerprint.wrapping_mul(HASH_BASE) >> self.shift) as usize
    }

    /// The probe window holding `fingerprint`'s entries, and the tag they
    /// carry. A tag match only nominates an offset — the caller verifies the
    /// bytes — so a colliding tag costs one failed verification and nothing
    /// more.
    fn window_of(&self, fingerprint: u64) -> (&[u64; PROBE_WINDOW], u64) {
        let home = self.home_of(fingerprint);
        let window = self.slots[home..home + PROBE_WINDOW]
            .try_into()
            .expect("windowful of slots");
        (window, fingerprint >> 32)
    }

    /// Record a block, keeping at most [`MAX_BLOCK_MATCHES`] offsets per tag
    /// (in insertion order). A block whose probe window is full goes
    /// unindexed; a run through it can still be matched starting from a
    /// neighboring block's offset.
    fn insert(&mut self, fingerprint: u64, offset: usize) {
        let home = self.home_of(fingerprint);
        let tag = fingerprint >> 32;
        let mut duplicates = 0;
        for slot in home..home + PROBE_WINDOW {
            let entry = self.slots[slot];
            if entry == 0 {
                self.slots[slot] = (tag << 32) | (offset as u64 + 1);
                return;
            }
            if entry >> 32 == tag {
                duplicates += 1;
                if duplicates >= MAX_BLOCK_MATCHES {
                    return;
                }
            }
        }
    }
}

/// Index fixed-size base blocks by their window fingerprint. A stride equal
/// to the match size bounds index memory while target scanning still finds
/// shifted runs after an insertion or deletion.
fn block_index(base: &[u8]) -> BlockIndex {
    let blocks = base.len() / MIN_COPY;
    let homes = (blocks * 4).next_power_of_two().max(PROBE_WINDOW);
    let mut index = BlockIndex {
        slots: vec![0; homes + PROBE_WINDOW],
        shift: 64 - homes.trailing_zeros(),
    };
    if base.len() < MIN_COPY {
        return index;
    }
    for offset in (0..=base.len() - MIN_COPY).step_by(MIN_COPY) {
        index.insert(block_hash(&base[offset..offset + MIN_COPY]), offset);
    }
    index
}

/// Length of the common prefix of `a` and `b`, compared a word at a time so
/// verifying and extending a fingerprint hit costs far less than a byte loop
/// over the matched run.
fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    let limit = a.len().min(b.len());
    let mut len = 0;
    while len + 8 <= limit {
        let x = u64::from_le_bytes(a[len..len + 8].try_into().expect("8-byte chunk"));
        let y = u64::from_le_bytes(b[len..len + 8].try_into().expect("8-byte chunk"));
        if x != y {
            return len + ((x ^ y).trailing_zeros() / 8) as usize;
        }
        len += 8;
    }
    while len < limit && a[len] == b[len] {
        len += 1;
    }
    len
}

/// A multiplicative Rabin-Karp block fingerprint over a fixed window, computed
/// in Horner form so it agrees with [`roll_hash`]'s incremental update. It only
/// selects candidates; every hit is verified byte-for-byte before it becomes a
/// copy instruction.
fn block_hash(window: &[u8]) -> u64 {
    window.iter().fold(0u64, |hash, byte| {
        hash.wrapping_mul(HASH_BASE).wrapping_add(u64::from(*byte))
    })
}

/// Slide the block fingerprint one byte: drop `out_byte` from the front of the
/// window and admit `in_byte` at the back, yielding what [`block_hash`] would
/// compute for the shifted window.
fn roll_hash(hash: u64, out_byte: u8, in_byte: u8) -> u64 {
    hash.wrapping_sub(u64::from(out_byte).wrapping_mul(HASH_TOP))
        .wrapping_mul(HASH_BASE)
        .wrapping_add(u64::from(in_byte))
}

/// Encode a Git delta size varint.
fn encode_size(mut size: u64, out: &mut Vec<u8>) {
    loop {
        let mut byte = (size & 0x7f) as u8;
        size >>= 7;
        if size != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if size == 0 {
            return;
        }
    }
}

/// Append Git's literal-insert instructions, each carrying at most 127 bytes.
fn append_insert(out: &mut Vec<u8>, bytes: &[u8]) {
    for chunk in bytes.chunks(127) {
        if chunk.is_empty() {
            continue;
        }
        out.push(chunk.len() as u8);
        out.extend_from_slice(chunk);
    }
}

/// Append one or more Git copy instructions. A zero encoded copy size means
/// 64KiB, so larger copies are split at that boundary.
fn append_copy(out: &mut Vec<u8>, mut offset: usize, mut len: usize) {
    while len != 0 {
        let chunk_len = len.min(0x10000);
        let mut command = 0x80u8;
        let mut payload = Vec::with_capacity(7);
        for (bit, shift) in [(0x01, 0), (0x02, 8), (0x04, 16), (0x08, 24)] {
            let byte = ((offset >> shift) & 0xff) as u8;
            if byte != 0 {
                command |= bit;
                payload.push(byte);
            }
        }
        if chunk_len != 0x10000 {
            for (bit, shift) in [(0x10, 0), (0x20, 8), (0x40, 16)] {
                let byte = ((chunk_len >> shift) & 0xff) as u8;
                if byte != 0 {
                    command |= bit;
                    payload.push(byte);
                }
            }
        }
        out.push(command);
        out.extend_from_slice(&payload);
        offset += chunk_len;
        len -= chunk_len;
    }
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
            delta_group: None,
        })
    }

    fn random_body(len: usize) -> Vec<u8> {
        let mut state = 0x1234_5678u32;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                state as u8
            })
            .collect()
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

    #[test]
    fn should_roll_the_block_hash_to_match_a_fresh_computation() {
        // given: a sample buffer wider than the hash window
        let buffer = random_body(4 * 1024);

        // when: the hash rolls one byte at a time from the first window on
        let mut rolled = block_hash(&buffer[..MIN_COPY]);

        // then: every rolled value equals a freshly computed window hash
        assert_eq!(rolled, block_hash(&buffer[..MIN_COPY]));
        for start in 1..=buffer.len() - MIN_COPY {
            rolled = roll_hash(rolled, buffer[start - 1], buffer[start - 1 + MIN_COPY]);
            assert_eq!(
                rolled,
                block_hash(&buffer[start..start + MIN_COPY]),
                "rolled hash diverged at position {start}"
            );
        }
    }

    #[test]
    fn should_emit_an_offset_delta_for_similar_scheduled_blobs() {
        // given: two same-path revisions whose zlib streams are intentionally
        // hard to compress on their own, but differ by one byte.
        let base = random_body(32 * 1024);
        let mut target = base.clone();
        target[16 * 1024] ^= 0xff;
        let inputs = [
            PackEntry {
                id: oid_of(Kind::Blob, &base),
                kind: Kind::Blob,
                decompressed_size: base.len() as u64,
                zlib: Bytes::from(deflate(&base, 6)),
                delta_group: Some(7),
            },
            PackEntry {
                id: oid_of(Kind::Blob, &target),
                kind: Kind::Blob,
                decompressed_size: target.len() as u64,
                zlib: Bytes::from(deflate(&target, 6)),
                delta_group: Some(7),
            },
        ];
        let mut out = Vec::new();
        let mut stats = DeltaStats::default();

        // when
        write_pack_with_options(
            inputs.into_iter().map(Ok::<_, std::convert::Infallible>),
            2,
            gix_hash::Kind::Sha1,
            &mut out,
            PackOptions { ofs_deltas: true },
            &mut stats,
        )
        .expect("write pack");

        // then: both blobs were inflated as delta targets and the second was
        // emitted as an offset delta against the first, which was the only body
        // retained as a base and whose delta saved stored bytes
        assert_eq!(stats.eligible, 2);
        assert_eq!(stats.comparisons, 1);
        assert_eq!(stats.emitted, 1);
        assert_eq!(stats.inserted, 1);
        assert!(stats.bytes_saved > 0);

        // then: the first blob is a full base and the second is an in-pack
        // offset delta. Pack checksum verification still succeeds.
        let entries: Vec<_> = BytesToEntriesIter::new_from_header(
            BufReader::new(out.as_slice()),
            Mode::Verify,
            EntryDataMode::Keep,
            gix_hash::Kind::Sha1,
        )
        .expect("read pack header")
        .collect::<Result<_, _>>()
        .expect("verify pack entries");
        assert!(matches!(entries[0].header, Header::Blob));
        assert!(matches!(entries[1].header, Header::OfsDelta { .. }));
    }
}
