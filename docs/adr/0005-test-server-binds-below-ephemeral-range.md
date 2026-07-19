# 0005 — The embedded test server binds to loopback, below the ephemeral port range

Status: **Accepted**

## Context

`EmbeddedSftpServer` bound to port 0 on every interface. That produced an intermittent failure in the
conformance suite — roughly one connect in eight hundred — surfacing variously as
"connection is closed by foreign host", "Connection reset", and, memorably, "Auth fail for methods
'publickey'". None of it was the provider's doing, and the varying symptoms made it look like a race.

Two independent mechanisms, both rooted in using the ephemeral range at all:

1. **Wildcard binds ignore loopback-specific ones.** The OS allocates port 0 from the ephemeral range
   (49152–65535 on macOS and Linux) considering only wildcard bindings, so it hands out a port that
   another process already holds on `127.0.0.1`. The bind succeeds, but a client connecting to
   loopback is routed to the *more specific* socket — the other process — which then hangs up in
   whatever way it sees fit. A desktop typically has a dozen or more such listeners.
2. **Client source ports come from the same range.** A client connecting to a listener in that range
   can be handed the listener's own port as its source, connect to itself, and read back its own SSH
   banner, which surfaces as a protocol parse error.

The first was confirmed deterministically: a decoy listener on `127.0.0.1:P` plus a wildcard-bound
SFTP server on the same port reproduces "Connection reset" every time, while a loopback-bound server
is simply refused the bind and would pick another port.

## Decision

Bind to `127.0.0.1` explicitly, and choose a port at random from **20000–32000**, retrying on
`BindException` up to sixty times. Use the literal address rather than `"localhost"` in the fixture's
config, since that name can resolve to `::1` while the server holds the IPv4 loopback.

## Consequences

- Measured: roughly three failures per 2100 connects before, zero in 3000 after.
- A test SSH server is no longer reachable from the network, which it never should have been.
- The band is scanned from a random offset so concurrent test JVMs — surefire forks, or several
  modules building at once — collide rarely and recover immediately when they do. Collisions with a
  service on that band now retry rather than failing.
- Three regression tests assert the two properties, because reverting to `setPort(0)` would look like
  a harmless simplification while reintroducing a failure that only appears on a machine with the
  wrong port in use, in a test that appears unrelated.
