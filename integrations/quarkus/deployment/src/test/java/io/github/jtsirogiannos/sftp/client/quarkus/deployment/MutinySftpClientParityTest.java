package io.github.jtsirogiannos.sftp.client.quarkus.deployment;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClientSession;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpOperations;
import io.github.jtsirogiannos.sftp.client.quarkus.runtime.MutinySftpClient;
import io.github.jtsirogiannos.sftp.client.quarkus.runtime.MutinySftpOperations;
import io.github.jtsirogiannos.sftp.client.quarkus.runtime.MutinySftpSession;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every operation on the neutral reactive client must exist on {@link MutinySftpClient} by name.
 *
 * <p>That wrapper is the only reactive surface a Quarkus application sees, and it is maintained by
 * hand — it once silently lagged the neutral client by thirteen operations, and nothing failed.
 * This test is the tripwire: add a method to {@link ReactiveSftpClient} and this build breaks until
 * the Mutiny delegation exists too.
 *
 * <p>Name-level, not signature-level, on purpose: the wrapper's whole job is changing the types
 * ({@code CompletionStage} to {@code Uni}, {@code ByteBuffer} to Vert.x {@code Buffer}), so
 * signatures cannot be compared mechanically without re-encoding those conversion rules here.
 */
class MutinySftpClientParityTest {

    @Test
    void operationsCoverEveryNeutralOperationByName() {
        assertCovered(Arrays.asList(ReactiveSftpOperations.class.getMethods()), MutinySftpOperations.class);
    }

    @Test
    void theClientCoversEveryNeutralClientMethodByName() {
        assertCovered(Arrays.asList(ReactiveSftpClient.class.getDeclaredMethods()), MutinySftpClient.class);
    }

    @Test
    void theSessionCoversEveryNeutralSessionMethodByName() {
        assertCovered(Arrays.asList(ReactiveSftpClientSession.class.getDeclaredMethods()),
            MutinySftpSession.class);
    }

    private static void assertCovered(List<Method> neutral, Class<?> wrapper) {
        Set<String> wrapped = Arrays.stream(wrapper.getDeclaredMethods())
            .map(Method::getName)
            .collect(Collectors.toSet());

        List<String> missing = neutral.stream()
            .filter(m -> !m.isSynthetic() && Modifier.isPublic(m.getModifiers()))
            .map(Method::getName)
            .distinct()
            .filter(name -> !wrapped.contains(name))
            .sorted()
            .toList();

        assertThat(missing)
            .describedAs("neutral methods with no Mutiny counterpart on " + wrapper.getSimpleName()
                + "; each is a one-line Uni.createFrom().completionStage(...) away")
            .isEmpty();
    }
}
