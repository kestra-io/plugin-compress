# How to use the Compress plugin

Compress, decompress, archive, extract, encrypt, and decrypt files from Kestra flows.

## Tasks

`ArchiveCompress` creates an archive from a `from` source — set `algorithm` to the archive format (`TAR`, `ZIP`, `JAR`, `AR`, or `CPIO`). Optionally set `compression` to also compress the archive (e.g. `GZIP` for `.tar.gz`). Note: `ARJ` and `DUMP` are supported for extraction only.

`ArchiveDecompress` extracts an archive — set `from` (a `kestra://` URI) and `algorithm`. Optionally set `compression` if the archive is also compressed. `ARJ` and `DUMP` are supported here but not for compression.

`FileCompress` compresses a single file — set `from` (a `kestra://` URI) and `compression` (required). Supported algorithms: `GZIP`, `BZIP2`, `XZ`, `ZSTD`, `LZMA`, `DEFLATE`, `LZ4FRAME`, `LZ4BLOCK`, `SNAPPYFRAME`, `Z`, and others. Note: `BROTLI`, `DEFLATE64`, and `SNAPPY` variants are decode-only.

`FileDecompress` decompresses a single file — set `from` and `compression`. Supports all algorithms including the decode-only ones.

`FileEncrypt` encrypts a file with AES-256 — set `from` (a `kestra://` URI) and `password` (required; store it as a [secret](https://kestra.io/docs/concepts/secret)). The default `keyDerivation` (`PBKDF2_SHA256`) produces OpenSSL-compatible AES-256-CBC output (`openssl enc -aes-256-cbc -pbkdf2`); `PBKDF2_SHA512`, `ARGON2ID`, and `SCRYPT` use authenticated AES-256-GCM with a self-describing KESTRAENC file format. Tune the cost with `iterations` (PBKDF2), `argon2TimeCost`/`memory`/`parallelism` (Argon2id), or `memory`/`parallelism` (Scrypt).

`FileDecrypt` decrypts a file produced by `FileEncrypt` — set `from` and `password`. The file format (OpenSSL or KESTRAENC) is detected automatically; for OpenSSL files, set `iterations` to match the value used at encrypt time.
