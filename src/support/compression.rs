//  mq-bridge
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge
//
//! Shared batch-compression codecs for the file and object_store endpoints.
//!
//! Every algorithm here is *member-concatenable*: each compressed batch is a
//! self-contained member (gzip member / lz4 frame), and members appended
//! back-to-back decode sequentially as one stream. That keeps the file sink's
//! append model and the standard CLI tools (`zcat`, `lz4 -d`) working.

use crate::models::Compression;
use crate::support::compression_pool::{gzip_default, lz4_pooled, zstd_pooled};
use std::io::{BufRead, Read};

/// Compresses one batch's serialized bytes into a single self-contained member.
/// `Compression::None` is a caller bug — the uncompressed path never gets here.
pub(crate) fn compress_member(algo: Compression, data: &[u8]) -> std::io::Result<Vec<u8>> {
    match algo {
        Compression::None => unreachable!("compress_member called with Compression::None"),
        Compression::Gzip => gzip_default(data),
        Compression::Lz4 => lz4_pooled(data),
        Compression::Zstd => zstd_pooled(data, zstd::DEFAULT_COMPRESSION_LEVEL),
    }
}

/// Streaming decoder over concatenated members. Reading past the end of one
/// member continues into the next until the underlying reader is exhausted.
pub(crate) fn decompress_reader<R: BufRead + 'static>(
    algo: Compression,
    reader: R,
) -> Box<dyn Read> {
    match algo {
        Compression::None => Box::new(reader),
        Compression::Gzip => Box::new(flate2::read::MultiGzDecoder::new(reader)),
        Compression::Lz4 => Box::new(MultiLz4FrameDecoder::new(reader)),
        // zstd's Decoder reads concatenated frames to EOF, matching the member model. Construction
        // is fallible, so defer any init error through this infallible boundary via `ErrReader`.
        Compression::Zstd => match zstd::stream::read::Decoder::with_buffer(reader) {
            Ok(dec) => Box::new(dec),
            Err(e) => Box::new(ErrReader(Some(e))),
        },
    }
}

/// A reader that surfaces a single stored error on first read. Lets a fallible decoder constructor
/// be reported through [`decompress_reader`]'s infallible `Box<dyn Read>` return.
struct ErrReader(Option<std::io::Error>);
impl Read for ErrReader {
    fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
        Err(self
            .0
            .take()
            .unwrap_or_else(|| std::io::Error::other("zstd decoder already failed")))
    }
}

/// Decompresses a whole buffer of concatenated members, optionally rejecting
/// output larger than `max_bytes` (guards against a decompression bomb).
/// Used by the object_store endpoint and the encrypted-file reader.
#[cfg_attr(
    not(any(feature = "object-store", feature = "encryption")),
    allow(dead_code)
)]
pub(crate) fn decompress_all(
    algo: Compression,
    data: &[u8],
    max_bytes: Option<u64>,
) -> std::io::Result<Vec<u8>> {
    // Decode straight over the borrowed slice — no owning copy. (`decompress_reader`
    // returns a `'static` box and would force a `data.to_vec()`; matching here keeps
    // the whole compressed object from being cloned on every read.)
    let cursor = std::io::Cursor::new(data);
    let mut decoder: Box<dyn Read + '_> = match algo {
        Compression::None => Box::new(cursor),
        Compression::Gzip => Box::new(flate2::read::MultiGzDecoder::new(cursor)),
        Compression::Lz4 => Box::new(MultiLz4FrameDecoder::new(cursor)),
        Compression::Zstd => Box::new(zstd::stream::read::Decoder::with_buffer(cursor)?),
    };
    let mut out = Vec::new();
    match max_bytes {
        // Read one byte past the limit; a decompressed payload longer than that is
        // rejected rather than allocated whole.
        Some(limit) => {
            decoder
                .by_ref()
                .take(limit.saturating_add(1))
                .read_to_end(&mut out)?;
            if out.len() as u64 > limit {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("decompressed data exceeds the decode limit of {limit} bytes"),
                ));
            }
        }
        None => {
            decoder.read_to_end(&mut out)?;
        }
    }
    Ok(out)
}

/// lz4 counterpart of `MultiGzDecoder`: decodes concatenated lz4 frames as one
/// stream. When one frame ends, the next is started as long as the underlying
/// reader has bytes left.
struct MultiLz4FrameDecoder<R: BufRead> {
    inner: Option<lz4_flex::frame::FrameDecoder<R>>,
}

impl<R: BufRead> MultiLz4FrameDecoder<R> {
    fn new(reader: R) -> Self {
        Self {
            inner: Some(lz4_flex::frame::FrameDecoder::new(reader)),
        }
    }
}

impl<R: BufRead> Read for MultiLz4FrameDecoder<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        loop {
            let decoder = match self.inner.as_mut() {
                Some(d) => d,
                None => return Ok(0),
            };
            let n = decoder.read(buf)?;
            if n > 0 {
                return Ok(n);
            }
            // Frame finished: continue with a fresh decoder if more bytes follow.
            let mut reader = self.inner.take().expect("decoder present").into_inner();
            if reader.fill_buf()?.is_empty() {
                return Ok(0);
            }
            self.inner = Some(lz4_flex::frame::FrameDecoder::new(reader));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concatenated_members_round_trip() {
        for algo in [Compression::Gzip, Compression::Lz4, Compression::Zstd] {
            let a = compress_member(algo, b"first batch\n").unwrap();
            let b = compress_member(algo, b"second batch\n").unwrap();
            let mut joined = a;
            joined.extend_from_slice(&b);
            let out = decompress_all(algo, &joined, None).unwrap();
            assert_eq!(out, b"first batch\nsecond batch\n", "algo {algo:?}");
        }
    }

    #[test]
    fn repeated_members_reuse_pooled_encoders() {
        // Same thread, many members: the pooled gzip/zstd state is exercised on a reset,
        // and incompressible input forces the gzip output buffer past its initial size.
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let random: Vec<u8> = (0..64 * 1024)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect();

        for algo in [Compression::Gzip, Compression::Lz4, Compression::Zstd] {
            for _ in 0..3 {
                for data in [b"".as_slice(), b"tiny", &vec![b'a'; 200_000], &random] {
                    let member = compress_member(algo, data).unwrap();
                    assert_eq!(
                        decompress_all(algo, &member, None).unwrap(),
                        data,
                        "{algo:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn pooled_gzip_matches_gzencoder_bytes() {
        // Members already on disk (and `zcat`) must keep reading the same as before.
        use std::io::Write;
        for data in [b"tiny".as_slice(), &vec![b'x'; 100_000]] {
            let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            e.write_all(data).unwrap();
            let legacy = e.finish().unwrap();
            let pooled = crate::support::compression_pool::gzip_default(data).unwrap();
            assert_eq!(legacy, pooled, "byte-identical gzip member expected");
        }
    }

    #[test]
    fn pooled_lz4_matches_fresh_frame_encoder_bytes() {
        // Sizes either side of the pool cut-off, each repeated so the reused encoder is
        // compared too: frames already written must keep decoding the same way.
        use std::io::Write;
        for len in [0usize, 1, 4096, 64 * 1024, 64 * 1024 + 1, 300_000] {
            let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            let mut fresh = lz4_flex::frame::FrameEncoder::new(Vec::new());
            fresh.write_all(&data).unwrap();
            let expected = fresh.finish().unwrap();
            for _ in 0..3 {
                let pooled = compress_member(Compression::Lz4, &data).unwrap();
                assert_eq!(pooled, expected, "len {len}");
            }
        }
    }

    #[test]
    fn decompress_all_enforces_limit() {
        for algo in [Compression::Gzip, Compression::Lz4, Compression::Zstd] {
            let big = vec![b'a'; 10 * 1024];
            let member = compress_member(algo, &big).unwrap();
            assert!(member.len() < big.len());
            assert!(decompress_all(algo, &member, Some(1024)).is_err());
            assert_eq!(decompress_all(algo, &member, Some(64 * 1024)).unwrap(), big);
        }
    }
}
