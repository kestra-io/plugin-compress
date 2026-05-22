# Kestra Compress Plugin

## What

- Provides plugin components under `io.kestra.plugin.compress`.
- Includes classes such as `ArchiveCompress`, `ArchiveDecompress`, `FileCompress`, `FileDecompress`, `FileEncrypt`, and `FileDecrypt`.

## Why

- What user problem does this solve? Teams need to compress and decompress files and archives for Kestra flows from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Compression steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Compression.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `compress`

### Key Plugin Classes

- `io.kestra.plugin.compress.ArchiveCompress`
- `io.kestra.plugin.compress.ArchiveDecompress`
- `io.kestra.plugin.compress.FileCompress`
- `io.kestra.plugin.compress.FileDecompress`
- `io.kestra.plugin.compress.FileEncrypt`
- `io.kestra.plugin.compress.FileDecrypt`

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
