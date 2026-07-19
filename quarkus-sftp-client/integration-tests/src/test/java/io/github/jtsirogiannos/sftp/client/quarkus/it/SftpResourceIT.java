package io.github.jtsirogiannos.sftp.client.quarkus.it;

import io.quarkus.test.junit.QuarkusIntegrationTest;

/**
 * Runs every {@link SftpResourceTest} case against the packaged application instead of an in-JVM
 * one: the runner jar in an ordinary build, a native binary under {@code -Pnative}.
 *
 * <p>This is the only thing that actually verifies native behaviour. The JVM build passing says
 * nothing about it — reflection registration and {@code META-INF/services} handling differ entirely
 * under GraalVM, and a missing {@code ServiceProviderBuildItem} produces a binary that builds
 * perfectly and then cannot find a provider at runtime.
 *
 * <p>No body on purpose: inheriting the cases guarantees the JVM and native runs cannot drift.
 */
@QuarkusIntegrationTest
class SftpResourceIT extends SftpResourceTest {
}
