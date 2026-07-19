# 0003 — Ship the async SPI without an async network provider

Status: **Accepted**

## Context

Every reactive facade this project ships — Mutiny, Reactor, coroutines — parks a worker thread,
because `SftpSession` hands out `InputStream` and `OutputStream`. That is why the baseline capability
is called `pipelinedStreaming` and not "async".

Genuine event-loop-native SFTP means owning request-id correlation, so an arriving `SSH_FXP_DATA` can
be pushed onto a `Flow.Publisher` from the I/O thread and downstream demand becomes the read window.
It does not mean writing SSH: MINA's transport is already asynchronous, and the gap is its *SFTP
client API* rather than its plumbing.

**The mechanism was proved with a throwaway.** It drove `SSH_FXP_INIT`/`OPEN`/`READ`/`CLOSE` over
`ChannelSubsystem` with `Streaming.Async`, framing packets by hand and routing replies through a
`Map<Integer, CompletableFuture<Buffer>>`. Payloads came back byte-correct, delivered on
`sshd-SshClient-nio2-thread-*`, with no executor in the file and no thread of ours in a read loop.

Getting a *discriminating* test out of it took three attempts, and the failures are the useful part:

- One request in flight proves nothing about correlation — a FIFO queue routes every reply
  correctly. Pipelining eight opens and eight reads still proved nothing, because MINA's server
  answers in strict arrival order, making FIFO and id-routing indistinguishable against that fixture.
  Correlation is required by the protocol, which permits reordering, not by any server under test.
- Injecting reordering *still* passed under FIFO routing, because pairwise reversal is an
  involution: it scrambles the `OPEN` replies and unscrambles the `READ` replies, and the two errors
  cancel. Releasing replies rotated rather than reversed finally separated them — 8/8 correlated by
  id, 0/8 by FIFO.

**No library removes the need to build this.** A survey of Apache MINA sshd, SSHJ, JSch and the
mwiede fork, Maverick Synergy, Vert.x and the Netty ecosystem found no JVM library exposing a
genuinely non-blocking SFTP *client* API. Maverick advertises "asynchronous" but means in-flight
request pipelining beneath a blocking API — `performOptimizedWrite` takes an `InputStream` — and it
is LGPLv3. Every credible option is an async transport with a blocking client API on top, which is
the gap this project would fill rather than one it can adopt its way out of.

## Decision

Land the design without the network provider: `AsyncSftpSession` and `AsyncSftpProvider` as optional
SPI, an `asyncStreaming()` capability, `AsyncReactiveSftpClient` in the core, and
`sftp-provider-inmemory` implementing the async contract — it completes inline for free, which makes
the async path live, exercised code rather than an interface nothing implements.

Record, but do not yet build, the MINA seam that a real async provider would use:

- `RawSftpClient.send(int cmd, Buffer) -> int requestId` encodes and sends, handing back the id.
- `DefaultSftpClient.process(Buffer)` is **protected**, runs on the I/O thread, and is precisely the
  correlation point — it already parses the id out of an arriving packet and parks the buffer in the
  `Map<Integer, Buffer>` that the blocking `receive(id)` waits on.
- `AbstractSftpClient` exposes the response *parsers* separately from the wait, as protected methods:
  `checkHandleResponse`, `checkAttributesResponse`, `checkDataResponse`, `readAttributes`,
  `writeAttributes`, `getReferencedName`.

An async provider therefore overrides `process` to complete a `CompletableFuture` instead of
notifying a monitor, and reuses MINA's encoders and parsers for everything else.

## Consequences

- The reactive facades are honest: `asyncStreaming()` is set only by a provider that genuinely never
  parks a thread, and today only the in-memory one does.
- An async MINA provider is a scoped follow-up rather than a rewrite. An early estimate that it meant
  redoing ~25 packet types, `ATTRS` coding for v3 and v6, extension negotiation and error mapping was
  wrong — that work already exists behind the seams above.
- **The real cost would be coupling, not protocol.** All three seams are `protected` members of
  `...sftp.client.impl` classes, not public API. MINA can change them in a minor release without
  calling it a break, so a provider built on them rides internals and needs pinned versions and its
  own conformance runs to notice when it rots. That is a maintenance risk, not a correctness one.
- **Deliberately not planned:** a third real provider (SSHJ). Its SFTP client is blocking, so it
  moves nothing toward the above, and coverage is already served by two backends, two server fixtures
  and an in-memory provider running the conformance suite six ways.
