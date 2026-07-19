# Roadmap: neutral SFTP client library + framework starters

Goal: one framework-agnostic SFTP client library, usable from plain Java, Quarkus, or Spring Boot,
with swappable JSch / Apache MINA SSHD backends — the JDBC "bring your own driver" model.

## The model, mapped

| JDBC | Here |
|---|---|
| `java.sql.Driver` (in the JDK, no framework) | `SftpProvider` / `SftpSession` in `sftp-client-api` |
| `postgresql.jar`, `mysql-connector.jar` | `sftp-provider-jsch`, `sftp-provider-mina` |
| `DriverManager` + `ServiceLoader` discovery | `SftpClients` bootstrap/registry |
| HikariCP (pooling, framework-neutral) | `SftpSessionPool` in `sftp-client-core` |
| Quarkus Agroal / `spring-boot-starter-jdbc` | `quarkus-sftp-client` / `sftp-client-spring-boot-starter` |
| JDBC TCK | `sftp-client-tck` — the shared conformance suite |

The load-bearing insight: JDBC pluggability works because the *contract jar has no framework in it*.
Anything a driver author has to import is part of your public API. Keep that surface tiny and neutral
and everything downstream is easy.

---

## Phase 0 — Make the core actually neutral (blocker for everything else)

Right now `core/` imports `io.smallrye.mutiny.Multi`, `io.smallrye.mutiny.Uni`, and
`io.vertx.core.buffer.Buffer`. Those are Quarkus-flavoured types sitting in what is supposed to be
the neutral library. A Spring Boot user pulling this in gets SmallRye and Vert.x on their classpath
to download a file — that kills the whole premise.

**Decide the neutral streaming currency.** Three options:

1. **Blocking-only core; each framework module writes its own reactive facade.**
   Simple, zero deps — but the demand-driven streaming logic (the actual hard part) gets written
   twice, once in Mutiny and once in Reactor, and they will drift.
2. **`java.util.concurrent.Flow.Publisher<ByteBuffer>`** — JDK built-in, literally zero dependencies.
3. **`org.reactivestreams:Publisher<ByteBuffer>`** — a ~10KB API-only jar that is the universal
   currency; Reactor and Mutiny both consume it natively.

**Recommendation: 2 or 3.** Mutiny's `Multi` *is* a `Publisher`, so the Quarkus side becomes
`Multi.createFrom().publisher(corePublisher).map(Buffer::buffer)` — a one-line edge adapter. Reactor
is `Flux.from(corePublisher)`. You write the backpressured streaming logic once, in the core, and
each framework module is a thin type-conversion shim. Pick (3) if you want the widest interop today,
(2) if you want a genuinely zero-dependency core and are willing to write a correct `Flow.Publisher`
by hand (it's fiddly — get it right once, cover it with the TCK).

Also in this phase:
- **Rename packages.** `io.github.jtsirogiannos.sftp.client.*` → something neutral (`io.github.<you>.sftp.*`).
  `io.quarkiverse` in the coordinates of a library you want Spring users to adopt is a non-starter.
- **Split `api` from `core`.** Providers must depend only on the contract, never on the pool:
  - `sftp-client-api` — `SftpProvider`, `SftpSession`, `SftpCapabilities`, `SftpConnectionConfig`,
    `RemoteFile`, `SftpException`, `SftpClient`, `ReactiveSftpClient`. Dependencies: none (or the
    reactive-streams API jar).
  - `sftp-client-core` — `DefaultSftpClient`, `DefaultReactiveSftpClient`, `SftpSessionPool`,
    bootstrap. Depends on `-api`.
  This is exactly why a JDBC driver depends on the JDK and not on Hikari.
- **Set the Java baseline at 17.** The draft already uses records, sealed interfaces, and switch
  expressions. 17 also matches Quarkus and Spring Boot 3.

**Exit criteria:** `sftp-client-api` builds with an empty (or one-line) dependency list, and
`mvn dependency:tree` on `sftp-client-core` shows nothing framework-shaped.

---

## Phase 1 — Harden the standalone library

Make it good to use with *no* framework at all. If it's only pleasant via a starter, it isn't really
a library.

- **Bootstrap/registry API.** A `SftpClients` entry point doing `ServiceLoader` lookup over
  `SftpProvider`, plus a builder: `SftpClients.provider("mina").connect(config)`. This is your
  `DriverManager`. Fail loudly and clearly when a named provider isn't on the classpath — that error
  message is the single most-read line of your docs.
- **Config without a config framework.** `SftpConnectionConfig` gets a proper builder. Framework
  modules map *their* config (`@ConfigMapping`, `@ConfigurationProperties`) onto this record; the
  library never learns what a Quarkus config property is.
- **Replace the naive pool.** Real bounded pool with validation-on-borrow, idle eviction, and
  max-size accounting. Decide the cancellation/discard contract properly (already flagged in the
  README) — a mid-transfer session must not silently consume a pool slot forever.
- **Logging via SLF4J API only**, or no logging at all. Never a logging implementation.

**Exit criteria:** a plain `public static void main` sample that connects, lists, and streams a file
with only `sftp-client-core` + one provider on the classpath.

---

## Phase 2 — Provider parity and the conformance kit (the real "bring your own driver" work)

This is the phase that decides whether the SPI is genuine or decorative.

- **`sftp-client-tck`** — a shared test suite any provider must pass. Same tests, run twice, once per
  provider. Cover: metadata ops, round-trip transfer of a large file, EOF handling, backpressure
  (slow consumer must not OOM), cancellation mid-transfer, error mapping to `SftpException`,
  session validity after failure, and correct `WriteMode` semantics (especially `CREATE_NEW` failing
  on an existing path).
- **Use MINA SSHD's server as the test fixture for *both* providers.** It runs in-process, so your
  JSch provider tests against a real SSH server with no Docker. (Optionally add a Testcontainers
  OpenSSH run as a second target — real-world servers behave differently from MINA's.)
- **Finish auth parity.** The JSch provider's `applyAuth` is still a `TODO`. Both providers need
  password + private key (with passphrase) at minimum, and identical behaviour for the three
  `HostKeyStrategy` values. Verify `ACCEPT_NEW` really is TOFU on both.
- **Nail down capability honesty.** Anything a provider can't do should be a capability flag or a
  documented `UnsupportedOperationException` — never a silent difference in behaviour.
- **Consider a third provider as a forcing function.** An in-memory fake (for users' unit tests) is
  cheap and immediately proves the SPI isn't secretly JSch-shaped. It's also a genuinely useful
  artifact for consumers.

**Exit criteria:** the TCK passes green against jsch, mina, and the in-memory fake, from a single
parameterised run.

---

## Phase 3 — Quarkus extension

Now the extension is a thin adapter, not the product.

- Lives in this repository under `quarkus-sftp-client/`, coordinates `io.github.jtsirogiannos:*`,
  standard `runtime` + `deployment` + `integration-tests` layout.
- `runtime`: the `@ConfigMapping` named-endpoint config and `@NamedSftp` qualifier already drafted,
  plus a recorder producing clients; and the Mutiny edge adapter wrapping the core's `Publisher`.
- `deployment`: synthetic beans per named endpoint, build-time provider resolution (fail the *build*,
  not the first request, when a configured provider is missing), dev services, health check.
- **Bring-your-own-driver split**, mirroring the JDBC starter shape — the user adds two artifacts:
  - `quarkus-sftp-client` (core adapter)
  - `quarkus-sftp-client-jsch` **or** `quarkus-sftp-client-mina`
  Each provider module depends on `quarkus-jsch` / `quarkus-sshd` respectively, so native-image
  registration is inherited rather than re-declared — and the MINA EdDSA classloader workaround stays
  their problem.
- Native image verification is a Phase 3 deliverable, not an afterthought: run the integration tests
  in `@QuarkusIntegrationTest` native mode for both providers.

### Native image

Native execution is verified in CI only, and native-image registration currently lives in the neutral
extension rather than the provider bundle. Both choices, and the three findings that forced them, are
recorded in [ADR 0001](docs/adr/0001-native-image-configuration.md).

**Exit criteria:** both provider combinations pass integration tests in JVM *and* native mode.
Currently: JVM yes for `mina`; native builds but is unexecuted locally, and `jsch` is not covered —
the integration-tests application hardcodes the mina bundle, so covering both means parameterising
which bundle it declares.

---

## Phase 4 — Spring Boot starter (this is what proves the claim)

Until a second framework consumes it, "framework-agnostic" is a hypothesis. This phase is the test.

- `sftp-client-spring-boot-starter`: `@AutoConfiguration`, `@ConfigurationProperties(prefix="sftp")`,
  conditional beans per named endpoint, `@ConditionalOnClass` per provider.
- Reactive facade via `Flux.from(corePublisher)` — if this is more than a few lines, Phase 0 was done
  wrong and it's worth going back rather than papering over it.
- Actuator health indicator to match the Quarkus health check.

**Exit criteria:** the same sample app, ported between the two frameworks, differs only in config and
wiring — no changes to transfer logic.

---

## Phase 5 — Release engineering

- **Semver, strictly.** Your SPI is public API for third-party provider authors; a breaking change to
  `SftpSession` breaks every provider ever written against it.
- **Evolve the SPI with `default` methods**, always. Adding an abstract method to `SftpSession` is a
  breaking change for every downstream provider — JDBC learned this painfully.
- **Optional sub-interfaces for optional power** (e.g. the `SeekableSftpSession` idea for MINA's
  ranged `FileChannel`): the core checks `instanceof`, the base contract stays minimal, and providers
  that can't do it simply don't implement it.
- Version-compatibility table in the README: library version × extension version × supported
  Quarkus/Spring versions.

---

## Repository strategy

**Not "monorepo vs multi-repo" — group by release cadence.** Things that must version together
belong in one repo; things that follow someone else's clock don't.

| Repo | Contents | Versions with |
|---|---|---|
| `sftp-client` (monorepo) | `-api`, `-core`, `-provider-jsch`, `-provider-mina`, `-tck` | itself |
| `quarkus-sftp-client` | Quarkus extension (runtime/deployment/its own provider modules) | **Quarkus** releases |
| `sftp-client-spring-boot-starter` | Spring auto-configuration | **Spring Boot** releases |

The library modules share a version because a TCK change can force provider changes — they must be
releasable in one shot. The extension does not: it needs to release when Quarkus 3.x+1 lands, and
forcing a library release for that is backwards.

**Decided: one repo, one version, one groupId — indefinitely.** The Quarkus extension and the Spring
starters are treated as permanent parts of this project rather than as things staged for donation.
Everything publishes under `io.github.jtsirogiannos`, and a release bumps every module together.

The cost is real and worth stating: a Spring-only fix bumps the version of the core too. That is the
monorepo trade, and it is the same one Spring Boot itself makes. What it buys is that an SPI change
never needs SNAPSHOT juggling across three builds, and the framework modules cannot quietly rot
against a core that moved without them.

A future split is not ruled out — Quarkiverse is one-repo-per-extension, so donating the extension
would force one — but it is a contingency rather than a plan, and nothing is now designed around it.

## Publishing order

One reactor, one version, one coordinate space, so there is no ordering problem to solve: a tagged
release publishes every module to Maven Central under `io.github.jtsirogiannos` in a single pass.

Moving the repository into a GitHub organisation later needs nothing from the build. A transfer
preserves history, issues and Actions; only `<scm>`, `<url>` and any badge links change.

### Maven Central setup (verify against current docs — this changed, and older guides are wrong)

OSSRH (`oss.sonatype.org`) reached end of life on 30 June 2025; everything now goes through the
Central Portal at `central.sonatype.com`. Any tutorial telling you to use `nexus-staging-maven-plugin`
predates this.

1. Sign in to `central.sonatype.com` **with GitHub** → you automatically get a verified
   `io.github.<your-github-username>` namespace. Gotcha: only the *username* you signed up with —
   `io.github.<org-name>` is not auto-granted, so hosting the project under a GitHub org doesn't
   change your namespace.
2. Own a domain? Verify it via DNS TXT instead and use it as the groupId. Better long-term identity
   for a library you want Spring shops to adopt than `io.github.*`.
3. Use `central-publishing-maven-plugin`, generate a portal user token, GPG-sign, and attach
   sources + javadoc jars (all required).
4. **Central is immutable** — a published version can never be deleted or overwritten. Stay on `0.x`
   while the SPI moves; cut `1.0` only when you're prepared to live with the contract.

### Quarkiverse onboarding

You don't transfer your repo — you request one and move the code in.

- Open a request naming the lead maintainers (GitHub accounts) and a URL describing the technology.
  The Quarkus core team decides core-vs-Quarkiverse; a PR is then raised in the Quarkiverse DevOps
  repo adding a `.tf` entry, and they initialise the repo with skeleton code.
- You can (and should) build the extension in your own repo first — they explicitly support starting
  from the `create-extension` skeleton before the repo exists. Request the repo once it *works and
  has tests*, not while it's empty.
- Your POM must use `io.quarkiverse:quarkiverse-parent` as parent; groupId becomes
  `io.quarkiverse.<repo-name>`.
- Releases are then a PR editing `.github/project.yml`; merging triggers the Action that publishes to
  Central. No manual Central credentials on your side.
- Extras worth taking: `docs.quarkiverse.io` hosting, a catalog YAML entry for discoverability in
  code.quarkus.io, and registering with `quarkus-ecosystem-ci` for cross-testing against Quarkus
  snapshots.

> **This section is a contingency, not the plan.** The extension now ships under
> `io.github.jtsirogiannos` in this repository, with packages to match — see
> [ADR 0006](docs/adr/0006-own-namespace-and-one-repository.md). If donation ever happens it forces a
> groupId change to `io.quarkiverse.<repo-name>`, which is breaking for anyone already depending on
> it; that break is the price of donating, and it is why the decision to donate has to be made before
> the extension has adopters rather than after.

### Spring side

There is no Quarkiverse equivalent for Spring; publish under your own groupId. Follow the naming
convention: `sftp-client-spring-boot-starter` (suffix). The `spring-boot-starter-*` **prefix** is
reserved for official starters.

---

## Backlog

Not scheduled. Each entry records the design question that has to be answered first, because for
most of these the question is the hard part and the code is not.

### Observability: Micrometer and OpenTelemetry

Today the only signal is SLF4J. Worth having: per-operation timers, transfer byte counters, and pool
gauges — live, idle, borrow-wait, discards, evictions. `BoundedSftpSessionPool` already exposes
`liveSessions()` and `idleSessions()`; the client does not currently expose its pool, which is the
one small change the metrics binder would need.

**Decorators, not instrumentation in the core.** Extracting `SftpOperations` made this cheap: a
metering or tracing decorator implements that interface, wraps another, and the core never learns
what a meter is — the same rule that keeps `sftp-client-api` at zero dependencies. Two modules,
`sftp-client-micrometer` and `sftp-client-opentelemetry`, each with its own dependency.

Open question for tracing: there is no stable OpenTelemetry semantic convention for file transfer.
Spans would use `server.address`, `server.port` and a `network.protocol.name` of `sftp` and invent
the rest, so the attribute names are a guess that may need to change later. Say so in the module
rather than implying conformance.

### Quarkus Dev Services

In dev and test mode, start an SFTP container automatically when an endpoint is declared but has no
host configured, and inject `host`/`port`/`username`/`password` — exactly what the datasource
extension does with Postgres. The pieces already exist: the TCK's `OpenSshSftpServer` runs
`atmoz/sftp` under Testcontainers, and that is the container this would start.

Two things to work through:

- The extension discovers endpoint **names** during augmentation. Dev Services supplies values at
  runtime, so a partially-configured endpoint has to survive the build with a placeholder host —
  the same wrinkle the integration-tests application works around today with a fixed port in
  `application.properties`.
- A container is not the only sensible default. `sftp-provider-inmemory` needs no Docker at all and
  starts in milliseconds; for many dev loops that is the better zero-config experience. Possibly
  both, with the in-memory one as the default and a container behind a flag.

### An async network provider

The optional async SPI has shipped — `AsyncSftpSession`, `AsyncSftpProvider`, an `asyncStreaming()`
capability, and `AsyncReactiveSftpClient` — with `sftp-provider-inmemory` as the only implementation,
so the path is exercised rather than hypothetical. What remains is a provider that is async **over a
network**, which no JVM library offers today.

The mechanism is proved and the seam in MINA that would build one is identified, along with the
coupling risk of relying on it: [ADR 0003](docs/adr/0003-async-spi-and-the-mina-seam.md). The SPI
shape it has to fit is [ADR 0002](docs/adr/0002-sibling-session-contracts.md).

Still open, and additive whenever it is wanted: a blocking view of an async session, so an async-only
provider stays usable from `SftpClient`, `withSession` and the conformance suite.

**Deliberately not planned:** a third real provider (SSHJ). Its SFTP client is blocking, so it moves
nothing toward the above.

## Sequencing note

Phases 0–2 are the actual project; 3 and 4 are adapters that get easy if 0–2 are done right. The
strong temptation is to build the Quarkus extension first because it's the visible, demo-able part —
resist it. Every framework-shaped decision made before the core is neutral is one you'll have to
undo, and the Spring starter in Phase 4 is where you'd find out.

If you want a shortcut to de-risk the whole plan: do Phase 0, then write the *Spring* starter as a
50-line throwaway spike before building anything else. If it's awkward, the core isn't neutral yet,
and you've learned that in an afternoon rather than after Phase 3.
