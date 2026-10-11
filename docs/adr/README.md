# Architecture decision records

One file per decision that would otherwise have to be reconstructed from the code. Each records the
context that forced the choice, the choice itself, and what it cost — including, where it applies,
the reasoning that turned out to be wrong, because a decision whose rejected alternatives are lost
gets re-litigated by the next person to read the code.

Format is [Nygard's](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions):
Status, Context, Decision, Consequences. Superseded records stay in place and say what replaced them
rather than being deleted.

`ROADMAP.md` is the complement and holds only what is still ahead. `CHANGELOG.md` records what
shipped.

| ADR | Decision |
|---|---|
| [0001](0001-native-image-configuration.md) | Native-image configuration lives in the neutral extension, and is verified in CI only |
| [0002](0002-sibling-session-contracts.md) | `SftpSession` and `AsyncSftpSession` are siblings over `SftpSessionLifecycle`, and the pool is generic |
| [0003](0003-async-spi-and-the-mina-seam.md) | Ship the async SPI without an async network provider; the MINA seam that would build one |
| [0004](0004-publish-by-rename.md) | `uploadAtomically` publishes by rename, and refuses rather than degrading on SFTP v3 |
| [0005](0005-test-server-binds-below-ephemeral-range.md) | The embedded test server binds to loopback below the ephemeral port range |
| [0006](0006-own-namespace-and-one-repository.md) | Publish every module under one namespace from one repository; drop the Quarkiverse namespace |
| [0007](0007-contextual-sessions.md) | Sessions are current through a scoped context SPI, and recycled on the connection's health |
| [0008](0008-multi-tenancy.md) | Tenants are isolated by connection, by root directory, or both; a root directory is a namespace |
