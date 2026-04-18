# Kestra Compress Plugin

## What

- Provides plugin components under `io.kestra.plugin.compress`.
- Includes classes such as `ArchiveCompress`, `ArchiveDecompress`, `FileCompress`, `FileDecompress`.

## Why

- This plugin integrates Kestra with Compression.
- It provides tasks that compress and decompress files and archives for Kestra flows.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `compress`

### Key Plugin Classes

- `io.kestra.plugin.compress.ArchiveCompress`
- `io.kestra.plugin.compress.ArchiveDecompress`
- `io.kestra.plugin.compress.FileCompress`
- `io.kestra.plugin.compress.FileDecompress`

### Project Structure

```
plugin-compress/
├── src/main/java/io/kestra/plugin/compress/
├── src/test/java/io/kestra/plugin/compress/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
