//! zlib deflate/inflate for stored object content.
//!
//! A stored object's payload (and the content of an offloaded blob) is the
//! zlib stream of the object's bare content — the exact bytes a git pack entry
//! carries after its type/size header — so serving a pack copies the stream
//! out verbatim, never recompressing. Deflation happens once, when an object
//! is written; inflation happens only on the read paths that need the content
//! itself. The compression level is a write-time choice: zlib inflate is
//! level-agnostic and the git wire format pins zlib, so nothing records which
//! level (or algorithm) produced a stream, and streams of mixed levels are all
//! valid in the same pack.

use bytes::Bytes;
use zlib_rs::{DeflateConfig, InflateConfig, ReturnCode};

/// Deflate `content` into a standard-window zlib stream at `level` (0–9). The
/// output is sized to zlib's worst-case bound, so compression never fails for
/// space — even level 0, whose stored blocks are larger than the input.
pub fn deflate(content: &[u8], level: u32) -> Vec<u8> {
    let mut out = vec![0u8; zlib_rs::compress_bound(content.len())];
    let (compressed, code) =
        zlib_rs::compress_slice(&mut out, content, DeflateConfig::new(level as i32));
    debug_assert_eq!(
        code,
        ReturnCode::Ok,
        "deflate into a worst-case-sized buffer cannot fail"
    );
    let len = compressed.len();
    out.truncate(len);
    out
}

/// Inflate `zlib` back to its content, allocating the output exactly once at
/// `uncompressed_len` (the size recorded alongside the stream). Returns `None`
/// for a stream that does not inflate cleanly to exactly that many bytes,
/// which the caller treats as storage corruption.
pub fn inflate(zlib: &[u8], uncompressed_len: u64) -> Option<Bytes> {
    let mut out = vec![0u8; uncompressed_len as usize];
    let (content, code) = zlib_rs::decompress_slice(&mut out, zlib, InflateConfig::default());
    let ok = code == ReturnCode::Ok && content.len() as u64 == uncompressed_len;
    ok.then(|| Bytes::from(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_round_trip_content_through_deflate_and_inflate() {
        // given: content of a size that actually compresses
        let content = b"the quick brown fox jumps over the lazy dog".repeat(16);

        // when: deflated then inflated back at its known length
        let zlib = deflate(&content, 6);
        let restored = inflate(&zlib, content.len() as u64).expect("inflate");

        // then: the original bytes come back, and the stream was smaller
        assert_eq!(restored.as_ref(), content.as_slice());
        assert!(zlib.len() < content.len());
    }

    #[test]
    fn should_round_trip_empty_content() {
        // given/when: an empty object still deflates to a valid (non-empty)
        // zlib stream that inflates back to nothing
        let zlib = deflate(b"", 6);
        let restored = inflate(&zlib, 0).expect("inflate empty");

        // then
        assert!(restored.is_empty());
    }

    #[test]
    fn should_round_trip_at_every_valid_level() {
        // given: one payload deflated at each valid level
        let content = b"level-agnostic inflate over mixed compression levels".repeat(4);

        // when/then: every level's stream inflates back to the same content,
        // proving inflate needs no knowledge of the level that produced it
        for level in 0..=9 {
            let zlib = deflate(&content, level);
            let restored = inflate(&zlib, content.len() as u64).expect("inflate");
            assert_eq!(restored.as_ref(), content.as_slice(), "level {level}");
        }
    }

    #[test]
    fn should_reject_a_corrupt_stream() {
        // given: a valid stream with a flipped byte
        let mut zlib = deflate(b"some content", 6);
        let last = zlib.len() - 1;
        zlib[last] ^= 0xff;

        // when/then: it fails to inflate rather than returning garbage
        assert_eq!(inflate(&zlib, 12), None);
    }
}
