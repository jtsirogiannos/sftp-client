# 0008 — Tenants are isolated by connection, by root directory, or both; a root directory is a namespace

Status: **Accepted**

## Context

An application serving many tenants over SFTP needs every operation to land on the right tenant's
data, without each call site passing the tenant around — the problem Hibernate's multi-tenancy
solves for a database with a `CurrentTenantIdentifierResolver` and a `MultiTenantConnectionProvider`.

Hibernate offers three strategies. Two have SFTP analogues: a separate *database* per tenant is a
separate connection — host, credentials, pool — and a separate *schema* per tenant is a separate
directory on a shared connection. The third, a discriminator column, has none.

## Decision

**Both strategies, composable per tenant.** An `SftpTenantProvider` turns a tenant identifier into an
`SftpTenant`: a connection of its own, a root directory on the shared connection, or both. It is
asked once per tenant, on first use, and again only after `evictTenant`. `rootDirectoryTemplate`
covers the common case, and checks the identifier before it becomes a path segment, because an
identifier read from a request is attacker-controlled.

**A `TenantResolver` names the current tenant** when each call is made, on the calling thread.
`MultiTenantSftpClient` acts for it, or for one named with `forTenant`. With no current tenant, a call
fails; it never falls back to anything unconfined.

**A root directory is enforced client-side**, by `RootedSftpClient`. Every path is rewritten in and
out. Anything that could name a path outside is refused with `SSH_FX_PERMISSION_DENIED`: `..` above
the root, a backslash, NUL, a leading `~`, the empty path. Answers the server reports are translated
against the canonical root as well as the written one, since `realpath` resolves a root that is
itself a symlink. Links created through the view cannot point out of it.

**A session belongs to one tenant.** Session keys carry the tenant, so two tenants never share a
session, even when their directories sit on the same connection.

**Tenants are kept in one registry** for both facades: created outside any lock, swept for idle
connections on the aggregate traffic of all of them — since a quiet tenant's pool sees none of its
own — and closed on eviction.

## Consequences

- **A root directory is a namespace, not a security boundary.** It keeps honest code where it belongs.
  It cannot stop the server following a symlink something else planted inside the root, and every
  tenant shares the account. Tenants that must not reach each other need their own credentials and a
  server-side `ChrootDirectory` — the connection strategy.
- The connection strategy costs a pool per tenant. Tenant pools default to two sessions, and there is
  as yet no cap across all tenants; see the roadmap.
- `forTenant` returns a handle that looks the tenant up on every call, so it survives an eviction. A
  single operation that loses a race with an eviction is retried once; a unit of work is not, since
  something may already have happened.
- Fixing confinement exposed two provider bugs, both fixed: JSch treated paths as glob patterns with
  backslash escapes, and `deleteRecursively` refused only the literal string `/`.
