package io.github.jtsirogiannos.sftp.client.context;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * An immutable snapshot of which session is current for which {@link SessionKey}.
 *
 * <p>The unit a {@link SftpSessionContext} stores and restores. Immutable on purpose: a context
 * binds by replacing the whole snapshot for a scope and putting the previous one back afterwards,
 * which is the only shape a {@code ScopedValue} can implement, and it lets an adapter capture the
 * snapshot on one thread and carry it — in a Reactor {@code Context} or a coroutine context — to
 * wherever the work continues.
 *
 * <p>Holds only a handful of entries in practice, one per endpoint and tenant a unit of work
 * touches, so a copy per binding costs nothing worth measuring.
 */
public final class SessionBindings {

    private static final SessionBindings EMPTY = new SessionBindings(Map.of());

    private final Map<SessionKey<?>, Object> sessions;

    private SessionBindings(Map<SessionKey<?>, Object> sessions) {
        this.sessions = sessions;
    }

    /** Nothing bound. */
    public static SessionBindings empty() {
        return EMPTY;
    }

    /** The session bound under {@code key}, if any. */
    public <S> Optional<S> get(SessionKey<S> key) {
        return Optional.ofNullable(sessions.get(key)).map(key.type()::cast);
    }

    /** A new snapshot with {@code session} bound under {@code key}, replacing anything there. */
    public <S> SessionBindings with(SessionKey<S> key, S session) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(session, "session");
        Map<SessionKey<?>, Object> copy = new HashMap<>(sessions);
        copy.put(key, key.type().cast(session));
        return new SessionBindings(Map.copyOf(copy));
    }

    /**
     * A new snapshot without whatever {@code key} named — how a context whose bindings outlive a
     * single call ends one binding without disturbing others made meanwhile.
     */
    public SessionBindings without(SessionKey<?> key) {
        if (!sessions.containsKey(key)) {
            return this;
        }
        Map<SessionKey<?>, Object> copy = new HashMap<>(sessions);
        copy.remove(key);
        return copy.isEmpty() ? EMPTY : new SessionBindings(Map.copyOf(copy));
    }

    public boolean isEmpty() {
        return sessions.isEmpty();
    }

    @Override
    public String toString() {
        return "SessionBindings" + sessions.keySet();
    }
}
