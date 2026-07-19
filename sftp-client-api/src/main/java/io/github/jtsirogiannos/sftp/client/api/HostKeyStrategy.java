package io.github.jtsirogiannos.sftp.client.api;

/**
 * How a provider treats the server's host key. Every provider must implement all three identically —
 * this is explicitly called out as a parity requirement in ROADMAP Phase 2, because a strategy that
 * silently means something weaker on one backend is a security bug, not a behavioural quirk.
 */
public enum HostKeyStrategy {

    /**
     * The key must already be present in the known-hosts file and match. Anything else fails the
     * connection. This is the default: a library connecting non-interactively has no user to prompt,
     * so the safe answer is to refuse rather than to trust.
     */
    STRICT,

    /**
     * Trust-on-first-use. An unknown host is accepted and recorded in the known-hosts file; a host
     * already recorded must still match, and a *changed* key always fails. Equivalent to OpenSSH's
     * {@code StrictHostKeyChecking=accept-new}.
     */
    ACCEPT_NEW,

    /**
     * Accept any host key and never write one down. Defeats man-in-the-middle protection entirely.
     * Named to be uncomfortable to type in production code — intended for tests against ephemeral
     * fixture servers whose key changes every run.
     */
    INSECURE_ACCEPT_ANY
}
