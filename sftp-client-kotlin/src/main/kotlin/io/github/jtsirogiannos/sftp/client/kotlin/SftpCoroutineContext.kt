package io.github.jtsirogiannos.sftp.client.kotlin

import io.github.jtsirogiannos.sftp.client.context.SessionBindings
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.withContext
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

/**
 * The current SFTP sessions, carried in the coroutine context.
 *
 * A coroutine hops threads at every suspension, so a thread-local binding made when a unit of work
 * starts is gone by the next call. The bindings travel here instead, and [SuspendingSftpOperations]
 * replays them around each call into the library — the moment the library looks for its current
 * session. Set by [SuspendingSftpClient.withSession]; there is no reason to create one by hand.
 */
class SftpSessionBindings(val bindings: SessionBindings) :
    AbstractCoroutineContextElement(SftpSessionBindings) {

    companion object Key : CoroutineContext.Key<SftpSessionBindings>
}

/**
 * The current SFTP tenant, carried in the coroutine context — the coroutine form of
 * `TenantContext.runAs`. Read by a [SuspendingSftpClient] over a multi-tenant client.
 */
class CurrentSftpTenant(val tenantId: String) : AbstractCoroutineContextElement(CurrentSftpTenant) {

    companion object Key : CoroutineContext.Key<CurrentSftpTenant>

    override fun toString(): String = "CurrentSftpTenant($tenantId)"
}

/**
 * Runs [block] as [tenantId]: every call it makes on a [SuspendingSftpClient] over a multi-tenant
 * client acts for that tenant, across suspensions and dispatcher hops.
 *
 * ```kotlin
 * withTenant("acme") {
 *     client.upload("/inbound/orders.csv", data)
 * }
 * ```
 */
suspend fun <R> withTenant(tenantId: String, block: suspend () -> R): R =
    withContext(CurrentSftpTenant(tenantId)) { block() }

internal suspend fun currentBindings(): SessionBindings =
    currentCoroutineContext()[SftpSessionBindings]?.bindings ?: SessionBindings.empty()
