# 0007 — Sessions are current through a scoped context SPI, and recycled on the connection's health

Status: **Accepted**

## Context

`withSession(fn)` pinned one connection to a lambda, and that was all the library offered. Three
things were missing, all of which Hibernate users take for granted:

- **A current session.** Calls on the injected client from deeper in the call stack did not join the
  block's session. A service handed the client, rather than the block's operations, borrowed a
  connection per call. A nested block borrowed a second connection, and on a pool of one waited for
  itself until the acquire timeout.
- **An explicit session.** Work that throws checked exceptions, or spans several methods, fits a
  lambda badly.
- **Declarative demarcation** in the frameworks, and **any session at all** on the reactive facade.

Two further facts shaped the design. Where "current" is remembered must differ by runtime: a thread
local for blocking code, the Vert.x duplicated context in Quarkus (where Hibernate Reactive keeps
its), Reactor's `Context` in WebFlux, a coroutine context in Kotlin. And the Java baseline is
expected to reach 25, at which point a `ScopedValue` is the natural home.

## Decision

**A context SPI in the API module.** `SftpSessionContext` has two methods — `capture()` returns an
immutable `SessionBindings` snapshot; `callWith(bindings, work)` runs work with a snapshot bound and
restores the previous one afterwards. `SessionKey` names a binding by the owning client, **compared by
identity**, optionally narrowed to a tenant: the compound key Hibernate Reactive uses. The core's
default is a thread local; the Quarkus extension supplies a Vert.x one.

**Binding is scoped, never imperative.** There is no `put` and no `remove`. That is the only shape a
`ScopedValue` can implement, and it removes the class of bug where a pooled thread carries one
request's session into the next because a `remove` was missed. The consequence is that only scoped
forms bind: `withSession` and `@WithSftpSession`.

**`openSession()` is never bound** — Hibernate's `openSession()` as against `getCurrentSession()`. It is
the explicit handle, safe to give to another thread, and the way to get a second connection on purpose.

**The current session is looked up synchronously, when a call is made.** Reactor's and Kotlin's
contexts cannot be read from arbitrary code, so their adapters keep a captured snapshot in their own
context and replay it around each call into the library with `callWith`. The Vert.x context is
ambient; the Mutiny adapter emits each result back on the caller's context so the next step still
sees it.

**A session is recycled on its health, not on whether the block threw.** On close it goes back to the
pool if and only if the provider vouches for it (`isValid()`) and no stream opened on it is still
open. The SPI now obliges `isValid()` to report false whenever the channel may be out of step; JSch
was fixed to do so. A block's own exceptions propagate unchanged and no longer cost a reconnect.

**Some calls never join:** `download(String)` and the reactive `download`, whose results outlive the
call. While a stream is open on a session, its other calls are refused — several clients, JSch among
them, cannot interleave a read with other requests on one channel — and the client's calls borrow
separately instead of joining.

**A reactive session is a queue.** Operations run one at a time in the order issued, each chained on
the one before, so fanning out over a session is safe. A transfer holds the queue while it streams,
and anything issued meanwhile is refused rather than queued, since a call queued behind a download
and awaited in its `onNext` would never run. The session is usable before its connection arrives,
which is what lets `withSession` bind it on the caller's thread.

## Consequences

- On a plain thread-local context, a reactive unit of work's binding covers only its synchronous
  part: a continuation runs on whatever thread completed the step before. The operations handed to
  the work are always correct; calls on the client from continuations may borrow. The Vert.x context
  and the Reactor and coroutine adapters do not have this limit. Composites (`mkdirs`,
  `deleteRecursively`, `uploadAtomically`) resolve their target once for the same reason — routed step
  by step, their later steps escaped the session.
- A borrow made inside a unit of work — an unrouted `download`, a second `openSession`, another tenant
  — still waits for a pool slot. On a pool the session fills, that is a timeout, not a hang.
- In Spring, `@WithSftpSession` is applied by a proxy, so self-invocation is not demarcated.
- Pre-1.0 behaviour changes, recorded in the changelog: `withSession` no longer wraps the block's
  exceptions, recycles a healthy connection after a throw, and joins when nested.
