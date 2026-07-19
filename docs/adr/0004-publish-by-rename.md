# 0004 — `uploadAtomically` publishes by rename, and refuses rather than degrading on SFTP v3

Status: **Accepted**

## Context

A consumer polling a drop folder can pick up a file that is still being written. Ordinary `upload`
cannot prevent it: the file is visible under its final name from the first byte. The standard remedy
is to write to a temporary name and rename into place, and it is the thing consumers hand-roll most
often.

What "atomic" means depends on the server, and the capability model already carries the answer:

- With `posixRename` (SFTP v5+), rename replaces the target atomically. Genuinely atomic.
- Without it (v3, which is what OpenSSH speaks), rename onto an existing path fails. The only route
  is remove-then-rename, and between those two calls the file **does not exist**. A poller sees
  absence rather than a partial file, which is better but is not atomicity.

So this cannot be one method with one promise.

## Decision

`SftpOperations.uploadAtomically(String, InputStream)` and
`ReactiveSftpClient.uploadAtomically(String, Flow.Publisher<ByteBuffer>)`, both **default methods**
composed from `exists`, `upload`, `rename` and `remove`. No provider implements anything;
`withSession` inherits the blocking form, so inside a unit of work the write and the rename share one
connection; and both reactive clients inherit the reactive form without either implementing it.

The scratch file is a dotfile ending in `.part` in the destination's own directory — rename is only
atomic within a filesystem, so a temp file elsewhere would turn the publish into a copy. It is
removed best-effort on failure, with a cleanup failure attached as a suppressed exception rather than
replacing the real cause.

**On v3 it refuses.** Replacing an existing file would need remove-then-rename, and that gap is the
exact window the method exists to close, so it throws `UnsupportedOperationException` — matching how
the JSch provider already declines to emulate `CREATE_NEW` with a stat-then-create race. Writing to a
path that does not exist yet is atomic on every version and always allowed.

Shared naming and refusal text live in a package-private `AtomicPublish` so the blocking and reactive
paths cannot drift into different scratch suffixes or different explanations of the same limitation.

## Consequences

- The existence check is for the error message, not for correctness. If the destination appears
  concurrently, a v3 rename fails loudly; nothing silently overwrites.
- Callers on v3 who find the gap acceptable must remove the destination themselves, deliberately.
- **On testing this honestly:** an observer stream peeks at the destination half-way through the
  upload and asserts it still holds the previous version. That proves nothing on its own, so a
  deliberate control test asserts that plain `upload` at the same moment is visibly truncated. Both
  methods pass an "is the file correct afterwards" assertion; only the control establishes that the
  observation lands mid-transfer.
- One failure-path subtlety is invisible and nearly went untested. The original exception arrives
  wrapped in a `CompletionException`, and `addSuppressed` on the wrapper is lost, because
  `CompletableFuture.get()` reports the cause. Unwrapping first fixes it — but removing the unwrap
  passed all seven reactive integration tests, since provoking a failing `remove` over a real
  connection is awkward. A stub-based unit test in the core, where `upload` and `remove` both fail,
  is what makes that mutation fail.
