/**
 * Contextual session demarcation: which session is current, and where that is remembered.
 *
 * <p>A unit of work that opens a session with {@code withSession} — or a method annotated
 * {@link io.github.jtsirogiannos.sftp.client.api.WithSftpSession} — makes it current, and from then
 * until the work ends every call on the client itself travels over that one connection, however deep
 * in the call stack it is made. That is Hibernate's {@code getCurrentSession()}, and this package is
 * the part that decides where "current" lives:
 *
 * <ul>
 *   <li>{@link io.github.jtsirogiannos.sftp.client.context.SftpSessionContext} — the strategy. The
 *       core defaults to a thread local; the Quarkus extension supplies one on the Vert.x duplicated
 *       context; the Reactor and coroutine adapters bridge their own contexts onto it.</li>
 *   <li>{@link io.github.jtsirogiannos.sftp.client.context.SessionKey} — which session: the owning
 *       client, compared by identity, optionally narrowed to a tenant.</li>
 *   <li>{@link io.github.jtsirogiannos.sftp.client.context.SessionBindings} — an immutable snapshot of
 *       everything current, the unit a context stores and an adapter carries.</li>
 * </ul>
 *
 * <p>Binding is always scoped to a call and never set or cleared imperatively, so that a
 * {@code ScopedValue} can implement the strategy once the Java baseline allows it.
 */
package io.github.jtsirogiannos.sftp.client.context;
