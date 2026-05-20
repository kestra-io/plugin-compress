# How to use the Compress plugin

Compress, decompress, archive, and extract files from Kestra flows.

## Tasks

`ArchiveCompress` creates an archive from a `from` source — set `algorithm` to the archive format (`TAR`, `ZIP`, `JAR`, `AR`, or `CPIO`). Optionally set `compression` to also compress the archive (e.g. `GZIP` for `.tar.gz`). Note: `ARJ` and `DUMP` are supported for extraction only.

`ArchiveDecompress` extracts an archive — set `from` (a `kestra://` URI) and `algorithm`. Optionally set `compression` if the archive is also compressed. `ARJ` and `DUMP` are supported here but not for compression.

`FileCompress` compresses a single file — set `from` (a `kestra://` URI) and `compression` (required). Supported algorithms: `GZIP`, `BZIP2`, `XZ`, `ZSTD`, `LZMA`, `DEFLATE`, `LZ4FRAME`, `LZ4BLOCK`, `SNAPPYFRAME`, `Z`, and others. Note: `BROTLI`, `DEFLATE64`, and `SNAPPY` variants are decode-only.

`FileDecompress` decompresses a single file — set `from` and `compression`. Supports all algorithms including the decode-only ones.
