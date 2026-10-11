package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.context.SessionBindings;
import io.github.jtsirogiannos.sftp.client.context.SftpSessionContext;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * The current session, remembered per thread — the default, and the right one for blocking code.
 *
 * <p>Hibernate's {@code ThreadLocalSessionContext} in spirit, with one difference that matters: a
 * binding exists only for the extent of a {@link #callWith} and is always restored on the way out,
 * so a pooled thread cannot carry one request's session into the next. That is also why the
 * thread local is a plain one, never inheritable — a child thread inheriting a single-owner session
 * would share it with its parent.
 *
 * <p>One instance, shared: every client that uses it reads the same per-thread state, which is what
 * lets an adapter that replays bindings around a call reach whichever client it is calling.
 */
public final class ThreadLocalSftpSessionContext implements SftpSessionContext {

    private static final ThreadLocalSftpSessionContext INSTANCE = new ThreadLocalSftpSessionContext();

    private static final ThreadLocal<SessionBindings> CURRENT = new ThreadLocal<>();

    private ThreadLocalSftpSessionContext() {
    }

    public static ThreadLocalSftpSessionContext instance() {
        return INSTANCE;
    }

    @Override
    public SessionBindings capture() {
        SessionBindings bindings = CURRENT.get();
        return bindings == null ? SessionBindings.empty() : bindings;
    }

    @Override
    public <R> R callWith(SessionBindings bindings, Supplier<R> work) {
        Objects.requireNonNull(bindings, "bindings");
        SessionBindings previous = CURRENT.get();
        set(bindings);
        try {
            return work.get();
        } finally {
            set(previous);
        }
    }

    /** Removes rather than stores an empty snapshot, so an idle thread holds nothing at all. */
    private static void set(SessionBindings bindings) {
        if (bindings == null || bindings.isEmpty()) {
            CURRENT.remove();
        } else {
            CURRENT.set(bindings);
        }
    }

    @Override
    public String toString() {
        return "ThreadLocalSftpSessionContext";
    }
}
