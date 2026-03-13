# Kestra Compress Plugin

## What

Manage file compression and decompression within Kestra workflows. Exposes 4 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with Compression, allowing orchestration of Compression-based operations as part of data pipelines and automation workflows.

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

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
