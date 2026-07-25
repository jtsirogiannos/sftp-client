# Changelog

Notable changes to this project. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Design decisions behind these entries are recorded in [docs/adr](docs/adr/).

## [Unreleased]

Nothing has been published to Maven Central. The API is still free to change.

### Added

- Framework-agnostic `sftp-client-api` with a zero-dependency SPI: `SftpProvider`, `SftpSession`,
  `SftpCapabilities`, `SftpConnectionConfig`.
- `sftp-client-core`: `SftpClients` bootstrap over `ServiceLoader`, blocking and reactive client
  facades in JDK types only, and `BoundedSftpSessionPool` with validation-on-borrow, LIFO reuse, idle
  eviction and a bounded acquire wait.
- Providers: `sftp-provider-mina`, `sftp-provider-jsch`, and `sftp-provider-inmemory` for consumers'
  unit tests.
- `sftp-client-tck`: a conformance suite every provider passes, run against an embedded MINA server
  and an OpenSSH container.
- Quarkus extension and three Spring Boot starters (core, blocking, reactive).
- Kotlin coroutine support in `sftp-client-kotlin`.
- Streaming directory listing, symlink and hard-link operations, `statvfs`, and the full set of
  filexfer operations the drafts define.
- `withSession` unit of work, so a sequence of operations travels over one connection.
- Optional async SPI — `AsyncSftpSession`, `AsyncSftpProvider`, `asyncStreaming()` — with
  `AsyncReactiveSftpClient` in the core. See [ADR 0003](docs/adr/0003-async-spi-and-the-mina-seam.md).
- `SftpSessionPool.borrowAsync(Executor)`, satisfied inline by a warm pool.
  See [ADR 0002](docs/adr/0002-sibling-session-contracts.md).
- `uploadAtomically` on both the blocking and reactive facades, publishing by rename.
  See [ADR 0004](docs/adr/0004-publish-by-rename.md).
- Reactive facade parity: `lstat`, `realpath`, `symlink`, `readlink`, `hardlink`, `setAttributes`
  and `statvfs` on `ReactiveSftpClient` and `AsyncSftpSession`, plus `mkdirs`,
  `deleteRecursively`, `chmod`, `findFirst` and filtered `list` as default methods composed from
  them. `visit` and the `Path` transfers remain blocking-only.

### Changed

- `SftpConnectionConfig` is a builder-only class rather than a public record, so new fields stop
  being breaking changes; timeouts and passphrase-without-key are now validated.
- `RemoteFile` reports numeric `ownerId`/`groupId` where the server supplies them.
- The reactive facade gained `WriteMode` uploads and offset downloads; the Kotlin, Mutiny and
  Reactor wrappers are at full parity, each guarded by a reflection tripwire.
- Pool tuning (`max-size`, `max-idle`, `acquire-timeout`) is reachable from `SftpClients`, Spring
  properties and Quarkus config; `PoolSettings` carries it.
- OpenTelemetry spans accept a path redactor; the BOM covers the observability modules; builds are
  reproducible and dependency convergence is enforced.

- Quarkus extension packages moved from `io.quarkiverse.sftp.client.*` to
  `io.github.jtsirogiannos.sftp.client.quarkus.*`, and the extension now ships a
  `quarkus-extension.yaml` descriptor.
  See [ADR 0006](docs/adr/0006-own-namespace-and-one-repository.md).

- `SftpSession` and `AsyncSftpSession` are siblings over `SftpSessionLifecycle`, and
  `SftpSessionPool` is generic in the session type.
  See [ADR 0002](docs/adr/0002-sibling-session-contracts.md).

### Fixed

- `ByteArrayPublisher` (in-memory provider) delivered nested `onNext` signals and could overflow the
  stack under a request-one-at-a-time subscriber; its drain loop is now serialised by a
  work-in-progress counter.
- `BoundedSftpSessionPool` invoked listeners while holding the pool lock on the timeout and
  interrupt paths — a lock-order deadlock risk on exactly the exhaustion path where listeners alert.
- The JSch provider routed passwords and passphrases through an intermediate `String`, defeating the
  `char[]` wipe discipline its own comment claimed; credentials are now encoded via
  `SftpConnectionConfig.encodeSecret`.
- Reactive `uploadAtomically` threw synchronously for an invalid path instead of failing the stage.
- MINA sessions on SFTP v6 reported files as owned by uid 0; ownership is now `UNKNOWN_ID` where the
  wire carries names rather than numeric ids.
- `WriteSubscriber` could leave an upstream producer requested-from but never cancelled when a
  cancellation raced `onSubscribe`.

- `WriteSubscriber` raced on chained writes, truncating uploads while reporting success.
- `InputStreamPublisher` violated Reactive Streams rule 1.3 when a subscriber requested from inside
  `onSubscribe`.
- `remove()` could not delete directories in either network provider; the fakes hid it.
- `deleteRecursively` followed symlinks because it used `stat` rather than `lstat`.
- MINA offered ambient `~/.ssh` identities, causing intermittent authentication failures.
- MINA also read the developer's `~/.ssh/config`, so a client addressed to one host could be
  silently rewritten to another, and a `Match` section crashed `connect()` outright
  (`StreamCorruptedException: Currently not able to process Match sections`) — which had been
  misread as a Quarkus/SmallRye incompatibility. The provider now installs `HostConfigEntryResolver.EMPTY`
  alongside the empty identity provider, so a session connects to exactly what `SftpConnectionConfig`
  says and nothing the local environment supplies.
- The embedded test server bound to a port that another process could already hold on loopback,
  making the conformance suite fail roughly one connect in eight hundred.
  See [ADR 0005](docs/adr/0005-test-server-binds-below-ephemeral-range.md).
