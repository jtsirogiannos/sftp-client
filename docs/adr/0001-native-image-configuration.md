# 0001 — Native-image configuration lives in the neutral extension, and is verified in CI only

Status: **Accepted** (partially open — see Consequences)

## Context

The Quarkus extension is meant to follow the JDBC shape: a neutral extension plus a code-free
provider bundle that inherits native-image registration from the underlying library's own extension
(`quarkus-sshd`, `quarkus-jsch`). Building the first real native image showed that plan does not
survive contact with the client side, and produced three findings worth not rediscovering.

**`--initialize-at-run-time` is per class, and the culprit is a nested one.** MINA's
`JceRandom$Cache` holds a `SecureRandom` in a static field, so image generation bakes a seeded
`NativePRNG` into the binary and GraalVM aborts. Registering the *outer* `JceRandom` is a silent
no-op — no error, no warning, an identical failure. Only
`-Dquarkus.native.additional-build-args=--trace-object-instantiation=java.security.SecureRandom`
names the real holder.

**`quarkus-sshd` targets the server side** and does not register the above, so
`quarkus-sftp-client-mina` cannot both stay code-free and work natively.

**The runtime jar needs a Jandex index.** Without `META-INF/jandex.idx`, ARC cannot resolve the
`@NamedSftp` qualifier when attaching it to a synthetic bean, and a real application fails to
augment. It does not reproduce under `QuarkusUnitTest`, where the runtime is a classes directory and
is indexed for free — so the extension's own tests passed while every real consumer would have
broken.

## Decision

Keep the `SecureRandom` registration in the neutral extension behind a class-presence check, rather
than pushing it into the bundle. Ship `META-INF/jandex.idx` in the runtime jar. Verify native
execution in CI only.

CI-only verification is deliberate: a container build on macOS emits a Linux binary — correct
architecture, wrong OS — so `@QuarkusIntegrationTest` fails it with exit code 126. Building that way
locally is still worth doing, since it is what surfaced `JceRandom`; *running* it needs a Linux host
or a local GraalVM/Mandrel producing a Mach-O binary.

## Consequences

- Registration sits in the neutral extension, which is not where the bring-your-own-driver design
  says it belongs. Whether to add a bundle deployment module instead is **still open**.
- Reach for `--trace-object-instantiation` on the *first* image-heap failure, not the third.
- Native coverage is incomplete: the integration-tests application hardcodes the mina bundle, so
  `jsch` is unverified natively. Covering both means parameterising which bundle it declares.
