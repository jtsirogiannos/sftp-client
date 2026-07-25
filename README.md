# sftp-client

Framework-agnostic SFTP client for the JVM, with pluggable **JSch** and **Apache MINA SSHD**
backends — the JDBC "bring your own driver" model — plus a Quarkus extension and a Spring Boot
starter built on top.

> **Working, not released.** The whole reactor builds and 419 tests pass, including the conformance
> suite run against two independent SFTP servers. Versions in the root POM have been resolved and
> build-tested. It has never been published, never run against a production server, never run in CI,
> and the API is still free to change — see
> [Before the first public commit](#before-the-first-public-commit).

## First steps

```bash
./mvnw -B verify   # Maven 3.9.16 is pinned by the wrapper; no local install needed
```

Needs JDK 17+ (CI builds on 17 and 21). **Docker is optional**: without it the containerised OpenSSH
conformance run skips rather than fails, and you lose the second server's opinion.

## Why it's laid out this way

**One repo now, split at 1.0.** The library modules version together (a TCK change can force
provider changes); the Quarkus extension versions with *Quarkus* and the Spring starter with
*Spring Boot*. Different clocks eventually mean different repos — but until the SPI stops moving,
splitting means SNAPSHOT juggling across three builds for every contract tweak. See ROADMAP
"Repository strategy".

**`sftp-client-api` has zero dependencies, on purpose.** Anything added there becomes a dependency
of every third-party provider author. That's why the reactive facade speaks
`java.util.concurrent.Flow.Publisher` and `CompletionStage` rather than Mutiny or Reactor types.
If that POM's `<dependencies>` ever grows, Phase 0 has regressed. The convenience methods on
`SftpClient` are `default` methods for the same reason: anything expressible from the primitives
stays out of the SPI, so a driver author never implements it.

**Three Spring modules, because Spring applications are one thing or the other.** Spring Boot pairs
its own starters this way — `spring-boot-starter-data-redis` and `-reactive`, MVC and WebFlux — with
the code in a shared artifact and the reactive dependencies added on top. Ours follows that shape,
and gets one thing for free: because the Reactor facade ships in the reactive starter's own jar
rather than behind a `@ConditionalOnClass` in shared auto-configuration, a servlet application never
loads the class and there is no condition to get wrong. Quarkus needs no such split — one
application there serves blocking and reactive endpoints together.

## Modules

Artifact ids, which is what you depend on. On disk the framework adapters live under
`integrations/quarkus/` and `integrations/spring/`; everything else is a directory at the root.

| Artifact | What it is | Deps |
|---|---|---|
| `sftp-client-bom` | dependency management for consumers | — |
| `sftp-client-api` | `SftpProvider`/`SftpSession` and their optional async siblings, `SftpCapabilities`, both client facades | **none** |
| `sftp-client-core` | `BoundedSftpSessionPool`, `SftpClients` bootstrap, `InputStreamPublisher`, facade impls | api + slf4j-api |
| `sftp-provider-jsch` | JSch backend (SFTP v3, stop-and-wait) | api + jsch |
| `sftp-provider-mina` | MINA SSHD backend (SFTP v6 where the server agrees, pipelined) | api + sshd |
| `sftp-provider-inmemory` | in-memory backend for tests: no server, no network, no Docker | api |
| `sftp-client-tck` | conformance suite + both server fixtures | api + sshd, testcontainers *(optional)* |
| `sftp-client-micrometer` | Micrometer decorator for both facades: timers, transfer counters, pool gauges | core + micrometer |
| `sftp-client-opentelemetry` | OpenTelemetry decorator for both facades: a CLIENT span per operation | core + otel-api |
| `sftp-client-kotlin` | `SuspendingSftpClient`: suspend functions and kotlinx `Flow` | api + coroutines |
| `sftp-client-integration-tests` | facades × real providers × real server. Not published | everything |
| `quarkus-sftp-client` | `@ConfigMapping`, `@NamedSftp`, `MutinySftpClient` adapter | core + mutiny |
| `quarkus-sftp-client-deployment` | build steps, synthetic beans per endpoint | runtime |
| `quarkus-sftp-client-{jsch,mina}` | code-free BYO-driver bundles | + quarkus-jsch / quarkus-sshd |
| `quarkus-sftp-client-integration-tests` | a real Quarkus app, JVM and native. Not published | bundle + quarkus-rest |
| `sftp-client-spring-boot-autoconfigure` | `@AutoConfiguration`, `@ConfigurationProperties`, the registrar | core + spring |
| `sftp-client-spring-boot-starter` | aggregator for servlet apps. No code, **no Reactor** | autoconfigure |
| `sftp-client-spring-boot-starter-reactive` | aggregator for WebFlux + `ReactorSftpClient` | autoconfigure + reactor |

## Using it

Plain Java — no framework:

```java
try (var client = SftpClients.blocking("mina", config)) {
    client.list("/inbound").forEach(System.out::println);
}
```

Beyond the primitives, `SftpClient` carries the operations everyone otherwise hand-rolls:

```java
client.mkdirs("/outbound/2026/07/18");           // mkdir -p
client.upload(Path.of("report.csv"), "/outbound/2026/07/18/report.csv");
client.list("/inbound", f -> f.name().endsWith(".csv"));
client.deleteRecursively("/outbound/2026/06");   // symlinks removed, never followed
client.chmod("/outbound/report.csv", 0640);
client.findFirst("/inbound", f -> f.name().startsWith("SETTLE"));
```

Attributes read back as well as write: `RemoteFile` carries `permissions`, and `ownerId`/`groupId`
where the wire supplies them — numeric ids on SFTP v3, `UNKNOWN_ID` on v4+ servers that send owner
*names* instead. Reporting a guess there would mean claiming every file belongs to root.

**Publishing a file nobody can read half of.** `upload` writes straight to the destination, so a
consumer polling that directory can pick the file up a third written — the oldest bug in SFTP
integration. `uploadAtomically` writes to a scratch name beside it and renames into place:

```java
client.uploadAtomically("/inbound/orders.csv", data);
```

Available on the reactive facade too. Replacing an existing file needs a rename that overwrites,
which is SFTP v5+; against a v3 server the call throws rather than quietly doing
remove-then-rename, because that gap is the window it exists to close. Writing to a path that does
not exist yet is atomic everywhere. See [ADR 0004](docs/adr/0004-publish-by-rename.md).

**One connection for a block of work.** Every call above borrows a session and returns it, which is
right for one-off operations and wasteful for a sequence. `withSession` pins one:

```java
client.withSessionWithoutResult(work -> {          // withSession(...) if you need a return value
    work.mkdirs("/outbound/2026/07");
    work.uploadAtomically("/outbound/2026/07/report.csv", data);
    work.rename("/outbound/2026/07/old.csv", "/archive/old.csv");
});
```

The block gets the same `SftpOperations` surface, so nothing changes except how many times the pool
is asked. A directory sync that took fourteen borrows takes one.

**Sizing the pool, before production sizes it for you.** Every client owns a bounded pool: eight
sessions, five minutes idle, thirty seconds to acquire. The ninth concurrent operation waits and
then fails, which is a fine default and a poor surprise — so it is tunable at every entry point:

```java
PoolSettings pool = PoolSettings.defaults()
        .withMaxSize(24)                              // concurrent operations per endpoint
        .withAcquireTimeout(Duration.ofSeconds(5));   // fail fast rather than queue

try (var client = SftpClients.blocking("mina", config, pool)) { ... }
```

The same three settings exist in both frameworks — `sftp.endpoints.bank-a.pool.max-size`,
`.pool.max-idle`, `.pool.acquire-timeout` — and reach the same pool. If parked sessions keep dying
before reuse, `max-idle` is longer than the server's own idle timeout; align it rather than raising
`max-size`. `sftp-client-micrometer` exposes the gauges and counters that tell the two apart.

Quarkus — add `quarkus-sftp-client-mina` (or `-jsch`) and inject:

```java
@Inject @NamedSftp("bank-a") MutinySftpClient client;
```

**In dev and test, an endpoint with no host gets one.** Declare `sftp.endpoints.reports.username=…`
without a host and Dev Services starts an `atmoz/sftp` container, wiring its address and credentials
in. The endpoint keeps its real provider, so you develop against a real SFTP server rather than a
fake — the same reason the datasource extension starts a database. It needs Docker, never runs in
production (`%prod` skips it entirely), and switches off with
`quarkus.sftp-client.devservices.enabled=false`. Giving an endpoint a host disables it for that
endpoint.

Spring Boot — pick the starter that matches your application type, then configure
`sftp.endpoints.bank-a.*`:

| | starter | beans per endpoint |
|---|---|---|
| Spring MVC | `sftp-client-spring-boot-starter` | `SftpClient`, `ReactiveSftpClient` |
| WebFlux | `sftp-client-spring-boot-starter-reactive` | the above plus `ReactorSftpClient` |

```java
@Qualifier("sftpClient-bank-a")         SftpClient blocking;
@Qualifier("reactiveSftpClient-bank-a") ReactiveSftpClient async;      // JDK types, both starters
@Qualifier("reactorSftpClient-bank-a")  ReactorSftpClient reactive;    // reactive starter only
```

One endpoint gets `@Default` (Quarkus) / primary (Spring) beans, so the common case needs no
qualifier.

**The reactive facade carries the same operations as the blocking one**, bar two documented
exceptions (`visit`, and the `java.nio.file.Path` transfers). That includes the ones added late —
`lstat`, `realpath`, `symlink`/`readlink`/`hardlink`, `setAttributes`, `statvfs`, `mkdirs`,
`deleteRecursively`, `chmod`, filtered `list`, `findFirst`, `uploadAtomically` — plus explicit write
semantics and resumable reads:

```java
client.upload("/inbound/log.txt", chunks, WriteMode.APPEND);
client.download("/inbound/big.csv", resumeFrom);            // skip bytes already fetched
```

The Mutiny, Reactor and Kotlin wrappers mirror that surface. Each is hand-written, so each carries a
test that fails the build if the neutral client grows an operation the wrapper lacks — they had
silently drifted thirteen operations behind before those existed. Every wrapper also exposes the
neutral client (`unwrap()`, or `underlying` in Kotlin) so a gap is never a wall.

**The servlet starter pulls in no Reactor at all**, and it does not need to: `ReactiveSftpClient`
speaks `CompletionStage` and `Flow.Publisher`, both JDK types, and Spring MVC returns a
`CompletionStage` from a controller natively. Async endpoints work on a plain
`spring-boot-starter-web` classpath. The reactive starter adds `Mono`/`Flux` on top by wrapping the
same client — an endpoint keeps one connection pool however many facades you inject.

> **A `Mono` here is not non-blocking I/O.** Every transfer parks a thread from the client's worker
> executor for its duration, because neither backend offers a zero-thread SFTP path — the same
> reason the capability is called `pipelinedStreaming` and not "async". Nothing blocks the event
> loop (the work is dispatched to that pool), but concurrent transfers are capped by the pool size,
> eight by default, not by demand — see `PoolSettings` above, or
> `sftp.endpoints.<name>.pool.max-size`, and size it for the transfers you expect to overlap.

Kotlin — add `sftp-client-kotlin` and wrap the reactive client:

```kotlin
SuspendingSftpClient(SftpClients.reactive("mina", config, worker)).use { client ->
    client.mkdirs("/outbound/2026/07")
    client.uploadAtomically("/outbound/2026/07/report.csv", chunks)   // takes a kotlinx Flow
    client.download("/inbound/big.csv").collect { chunk -> ... }
    client.deleteRecursively("/outbound/2026/06")
}
```

That module is sugar, not plumbing — the adapters it wraps are two lines, and calling the neutral
client directly works just as well:

```kotlin
import kotlinx.coroutines.future.await     // kotlinx-coroutines-core
import kotlinx.coroutines.jdk9.asFlow      // kotlinx-coroutines-jdk9
import kotlinx.coroutines.jdk9.asPublisher

val files = client.list("/inbound").await()                       // CompletionStage -> suspend
client.download("/inbound/big.csv").asFlow().collect { chunk -> ... }
client.upload("/outbound/report.csv", chunks.asPublisher())       // kotlinx Flow -> Flow.Publisher
```

Two caveats, both worth reading before building on it:

- **`suspend` does not mean no thread here.** The transfer still occupies a worker thread for its
  duration, exactly as it does under Mutiny or Reactor. If that bothers you, the blocking client
  inside `withContext(Dispatchers.IO)` is the same cost with none of the pretence.
- **Cancellation reaches the transfer.** Cancelling the stage behind an upload cancels the upstream
  subscription, closes the remote file and discards the session, so a cancelled coroutine actually
  stops the work. One buffer may still land — demand is one chunk at a time and a write already
  handed to the stream cannot be recalled — but nothing further is requested.

Testing your own code — add `sftp-provider-inmemory` at test scope and ask for it by name. No
server, no Docker; the pool, both facades and the framework integrations all behave normally:

```java
try (var client = SftpClients.blocking("inmemory", config)) { ... }
```

It reports SFTP v6 with everything supported by default, and
`InMemorySftpProvider.withCapabilities(InMemorySftpProvider.VERSION_3)` gives a fake that claims v3
**and behaves like it** — rename refuses an occupied target, hard links and statvfs decline. That is
otherwise an awkward thing to test against, since it means finding a v3 server.

Logging is **SLF4J API only** — never an implementation. The pool logs an exhausted-pool WARN with
the endpoint and the likely cause; connects, discards and evictions are DEBUG. Credentials never
reach a log line, and a test asserts it across a whole client lifecycle.

## Observability

Both facades can be wrapped, and neither the core nor the API learns what a meter is:

```java
SftpClient metered = MicrometerSftpClient.wrap(client, registry, config);
SftpClient traced  = OpenTelemetrySftpClient.wrap(client, openTelemetry, config);
SftpPoolMetrics.bind(pool, registry, Tags.of("endpoint", "bank-a"));
```

Metrics give `sftp.client.operations` timers (tagged operation and outcome),
`sftp.client.transferred` byte counters, and — from `bind` — pool gauges plus borrow-wait, timeout,
discard, eviction and validation-failure meters. Waits climb before timeouts do, which is the point:
the gauges alone sample right past a pool that is quietly too small.

Tracing gives a CLIENT span per operation, with a `withSession` block becoming one `sftp.session`
span and its operations nested beneath. Note the deliberate asymmetry: metrics do **not** aggregate
convenience methods (`mkdirs` reports the three `mkdir` calls that crossed the wire), while tracing
does, because a trace view wants the tree and a counter wants the wire.

> **There is no OpenTelemetry semantic convention for file transfer** — searching the
> semantic-conventions repository for `sftp` returns nothing. Connection attributes here are stable
> and standard; the `file.*` ones are experimental and may move. `SftpAttributes` labels each one.
> Remote paths become span attributes, and filenames often embed business data, so `wrap` takes an
> optional redactor.

## The things worth reading first

`InputStreamPublisher` (core) is the only genuinely fiddly class: demand accounting, a serialising
drain loop, and cancellation. Get it right once and every framework module is a type shim —
`MutinySftpClient` is the proof (Mutiny 2 is natively `Flow`-based, so there's no bridge jar). It is
verified against the official Reactive Streams TCK, which is how a rule 1.3 violation surfaced: a
subscriber calling `request()` from inside `onSubscribe` used to get `onNext` before `onSubscribe`
had returned.

`SftpCapabilities` carries the honesty flags. `pipelinedStreaming` is deliberately not called
"async": MINA's public client API has no zero-thread SFTP path, it just keeps requests in flight so
the pulling worker rarely stalls. Callers are never told a transfer is more asynchronous than it is.

**There is a real `asyncStreaming` flag, and today almost nothing sets it.** The SPI has an optional
second half — `AsyncSftpSession` and `AsyncSftpProvider`, returning `CompletionStage` and
`Flow.Publisher` — for a backend that can complete operations without parking a thread. When a
provider implements it, `SftpClients.reactive` builds `AsyncReactiveSftpClient` over a pool typed to
those sessions, and the choice is made once per client rather than re-tested per call.

The two contracts are siblings, not subtypes: both extend a three-method `SftpSessionLifecycle`,
which is all a pool ever touches, so one generic `BoundedSftpSessionPool<S>` serves both. A provider
implements whichever it actually is.

Only `sftp-provider-inmemory` sets `asyncStreaming` today, because **no JVM SSH library exposes a
non-blocking SFTP client API** — MINA, SSHJ, JSch and Maverick are all an async transport with a
blocking client on top. The mechanism for building one has been proved against MINA's channel API
and the seam is written up, along with the coupling risk of relying on it:
[ADR 0003](docs/adr/0003-async-spi-and-the-mina-seam.md). Until then the flag stays false for both
network backends, which is the point of having it.

**Capabilities exist at two levels, and the difference is not cosmetic.**
`SftpProvider.capabilities()` is the backend's best case; `SftpSession.capabilities()` is what this
server actually agreed to. The MINA provider negotiates v6 against MINA's own server and **v3 against
OpenSSH** — no atomic rename, whatever the provider advertises. Anything behavioural must ask the
session. That distinction only became visible once the suite ran against a second server.

## Testing

Three layers, because each catches what the others structurally cannot:

- **Unit tests** (`sftp-client-core`) — pool accounting, demand, cancellation, logging, against
  in-memory fakes. TestNG, because the Reactive Streams TCK is TestNG-based and mixing frameworks in
  one module makes Surefire silently ignore one of them.
- **Conformance tests** (`sftp-client-tck`) — the SPI contract, run **six times**: each real provider
  against the embedded MINA server and against a containerised OpenSSH, plus the in-memory provider
  at full and at v3 capabilities. The second fixture is not
  redundant: it is what revealed that MINA's rename broke entirely against a v3 server, because
  MINA's own server always granted v6.
- **Integration tests** (`sftp-client-integration-tests`) — the facades, through real providers,
  against a real server. This layer exists because `core` may not depend on a provider and the TCK
  tests the SPI beneath the facades, so the combination had nowhere to live — and two bugs shipped
  through the gap.

A fake agrees with whatever the code believes. Both bugs above passed a green build against fakes
before a real server contradicted them; prefer to add coverage at the lowest layer that can actually
fail.

Quarkus native **builds** (`-Pnative`) — which is what caught MINA's `JceRandom` baking a seeded
`SecureRandom` into the image heap. The binary has not been *executed* anywhere yet: a container
build on macOS emits a Linux binary the host cannot run, and CI has not run. The `native` job is
configured to do it on Linux, where `@QuarkusIntegrationTest` runs the full suite against the
binary.

## Before the first public commit

- ~~Replace the placeholder groupId~~ and ~~`scm`~~ — done: `io.github.jtsirogiannos` throughout, and
  the `scm`/`url` blocks point at `github.com/jtsirogiannos/sftp-client`. Still open: `<developers>`
  has `<name>CHANGEME</name>`, which Maven Central wants filled in with a human name.
- ~~Verify every version in the root POM~~ — done, and three were wrong: `quarkus 3.33.4` never
  existed, `jsch 0.2.27` predates that fork's move to 2.x, and `sshd-sftp 2.15.0` was resolving
  against `sshd-core 2.17.1` pulled in by `quarkus-sshd`. All now build-tested. The Quarkus line is
  still worth confirming against quarkus.io before release — 3.33 is not necessarily the LTS.
- ~~Add `LICENSE`~~ — done: verbatim Apache 2.0, matching the root POM's `<licenses>` declaration.
  Still undecided: the copyright holder. There is no `NOTICE` file and no source headers, because
  naming an owner is your call (see the employment-agreement note below).
- Check your employment agreement's IP/moonlighting clause before going public.
- The Quarkus extension and Spring starters publish under `io.github.jtsirogiannos` alongside the
  library, on one version — see [ADR 0006](docs/adr/0006-own-namespace-and-one-repository.md).

## Project documents

- **[ROADMAP.md](ROADMAP.md)** — what is still ahead: the phases, publishing order, and the backlog.
- **[CHANGELOG.md](CHANGELOG.md)** — what has shipped.
- **[docs/adr](docs/adr/)** — why the load-bearing decisions were made, and what they cost.

## Known gaps

- The Quarkus native run covers the **mina** bundle only; the integration-tests app hardcodes it, so
  ROADMAP Phase 3's "both provider combinations in native" is not met.
- MINA's `JceRandom$Cache` needs run-time initialisation for native image, and that registration sits
  in the provider-neutral extension behind a class check rather than in the mina bundle, which is
  meant to be code-free. `quarkus-sshd` targets the server side and does not cover it.
- No retry or backoff on transient failures, and no health checks. Metrics and tracing cover both
  facades (`sftp-client-micrometer`, `sftp-client-opentelemetry`), but the tracing module's file
  attributes are experimental because **no OpenTelemetry semantic convention for file transfer
  exists** — see its `SftpAttributes`.
- The reactive facade lacks `visit` and the `java.nio.file.Path` transfers, deliberately. `visit`
  needs a streaming listing to be worth having (its blocking form stops the walk early, which
  `CompletionStage<List<..>>` cannot express), and `Path` transfers would put blocking file I/O
  behind a reactive signature without letting the caller choose the scheduler.
- The wrapper parity tests compare **method names**, not signatures. A wrapper can therefore still
  lag an *overload* — checking signatures would mean re-encoding each wrapper's type mapping
  (`CompletionStage`→`Uni`, `ByteBuffer`→`Buffer`) into the test, which is its own thing to get
  wrong.
- Numeric ownership is unavailable on SFTP v4+, which replaced uid/gid with owner *names*.
  `RemoteFile.ownerId()` returns `UNKNOWN_ID` there rather than a fabricated number, so a chown
  written on a v6 server cannot be read back through this API.
