# 0002 — `SftpSession` and `AsyncSftpSession` are siblings, and the pool is generic

Status: **Accepted**, supersedes an earlier inheritance-based design that was never released

## Context

Adding an optional thread-free session contract raised the question of how it relates to the
existing blocking one. The first design had `AsyncSftpSession extends SftpSession`, justified by the
belief that `BoundedSftpSessionPool` required a single session type and would otherwise have to be
forked.

That premise was never checked, and it is false. The pool calls `isValid()` and `close()` and
nothing else. It never invokes a single SFTP operation, because pooling is about the lifetime of a
connection rather than what travels over it.

Inheritance was not free. An async provider inherited nineteen blocking methods it had no use for,
two of them stream-shaped and unsatisfiable without a bounded `Flow.Publisher`-to-`InputStream`
bridge. The blocking defaults supplied on top joined their own stages, which is a deadlock aimed at
precisely the event-loop callers the interface exists to serve. It also misfired in practice:
implementing the in-memory session's async half silently redirected `list(String)` to the inherited
default, and the obvious `listAsync` implementation would have recursed until the stack gave out.

## Decision

Extract `SftpSessionLifecycle` — `capabilities()`, `isValid()`, `close()`, exactly what a pool
touches — and make `SftpSession` and `AsyncSftpSession` siblings over it. Make the pool generic:
`SftpSessionPool<S extends SftpSessionLifecycle>`, with one `BoundedSftpSessionPool<S>` serving both
and no duplicated validation, LIFO reuse, idle eviction or capacity accounting.

`AsyncSftpProvider` *does* extend `SftpProvider`, and the asymmetry is intentional: the tax that made
inheritance wrong at the session level was its size, and a provider owes exactly one extra method.
In exchange the whole discovery path — one `ServiceLoader` descriptor, one registry, one `name()`
lookup, the existing Spring and Quarkus wiring — is untouched.

Add `SftpSessionPool.borrowAsync(Executor)` as an additive default, with an override in
`BoundedSftpSessionPool` that satisfies the common case inline: a validated idle session is popped
LIFO under a lock held for a few instructions, and `isValid()` is contractually barred from making a
round trip, so there is nothing to offload. Only opening a connection, waiting for a slot at
capacity, and closing evicted sessions go to the executor.

## Consequences

- No per-operation branching. `DefaultReactiveSftpClient` is blocking-backed;
  `AsyncReactiveSftpClient` is its thread-free counterpart over a pool typed to `AsyncSftpSession`.
  `SftpClients.reactive` chooses once, when the client is built, rather than every call re-testing a
  session type or consulting a capability flag.
- A warm pool serving an async provider completes a metadata operation without occupying a worker.
- **A live footgun:** `borrowAsync` may complete on the caller's thread. The blocking fallback
  therefore hops to the worker explicitly instead of inheriting whichever thread it landed on.
  Without that, an event-loop caller hitting a warm pool would run a blocking `stat` on the event
  loop — the precise outcome the reactive facade exists to prevent. A test fails with
  `expected [sftp-test-worker] but found [main]` if the hop is removed.
- A blocking view of an async provider is not yet implemented. It is purely additive and only
  becomes necessary when an async-only provider exists.
