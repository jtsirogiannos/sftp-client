package io.github.jtsirogiannos.sftp.client.context;

import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/**
 * Where the current session lives: the strategy behind contextual session demarcation.
 *
 * <p>Hibernate's {@code CurrentSessionContext}, and Hibernate Reactive's {@code Context}, for SFTP.
 * A client asks it which session is current for its {@link SessionKey}; {@code withSession} and the
 * {@code @WithSftpSession} interceptors ask it to make one current for the extent of a piece of work.
 * Everything else in the library is indifferent to where that state is actually kept — a thread
 * local, a Vert.x context, a Reactor context, a coroutine context, or a {@code ScopedValue}.
 *
 * <h2>Binding is scoped, never imperative</h2>
 *
 * <p>There is deliberately no {@code put} and no {@code remove}. A binding exists for the extent of a
 * call and the previous state is restored when it returns, which is the only shape every candidate
 * store can honour: {@code ScopedValue.where(..).call(..)} cannot express anything else, and a thread
 * local bound imperatively is one forgotten {@code remove} away from leaking a session into the next
 * request a pooled thread serves.
 *
 * <h2>What an implementation provides</h2>
 *
 * <p>Two methods. {@link #capture()} answers what is bound right now; {@link #callWith} runs work with
 * a given snapshot bound and puts the old one back. Everything else is derived. Implementations whose
 * state can follow asynchronous work to its completion — a Vert.x duplicated context does — also
 * override {@link #composeWithin}.
 *
 * <p>The snapshot pair is also how adapters bridge contexts that are not ambient. Reactor's
 * {@code Context} and Kotlin's {@code CoroutineContext} cannot be read from arbitrary code, only from
 * inside the pipeline or the coroutine. Their adapters therefore keep a captured
 * {@link SessionBindings} in their own context and replay it around each call into the library with
 * {@link #callWith}, which is the moment the library looks.
 */
public interface SftpSessionContext {

    /** Everything bound in the current scope; {@link SessionBindings#empty()} when nothing is. */
    SessionBindings capture();

    /**
     * Runs {@code work} with {@code bindings} as the current state for its synchronous extent, then
     * restores whatever was current before — on every path out, exceptions included.
     */
    <R> R callWith(SessionBindings bindings, Supplier<R> work);

    /** The session bound under {@code key} in the current scope, if any. */
    default <S> Optional<S> current(SessionKey<S> key) {
        return capture().get(key);
    }

    /**
     * Runs {@code work} with {@code session} current under {@code key}, alongside whatever else is
     * already bound, for the work's synchronous extent.
     */
    default <S, R> R callWithin(SessionKey<S> key, S session, Supplier<R> work) {
        return callWith(capture().with(key, session), work);
    }

    /**
     * As {@link #callWithin}, for work that completes later: the binding should last until the
     * returned stage completes.
     *
     * <p>The default can only bind for the synchronous extent — the assembly of the stage, not its
     * continuations, which run on whatever thread completes each step. That is the honest limit of a
     * store tied to a thread. Contexts that travel with asynchronous work override this to keep the
     * binding until completion. Either way, the session handed to the work is always usable
     * directly; this only governs what calls on the <em>client</em> see.
     */
    default <S, R> CompletionStage<R> composeWithin(SessionKey<S> key, S session,
            Supplier<? extends CompletionStage<R>> work) {
        return callWithin(key, session, work::get);
    }
}
