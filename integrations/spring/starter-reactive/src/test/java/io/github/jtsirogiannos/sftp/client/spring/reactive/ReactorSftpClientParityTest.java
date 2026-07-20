package io.github.jtsirogiannos.sftp.client.spring.reactive;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every operation on the neutral reactive client must exist here by name — the same tripwire the
 * Kotlin and Mutiny wrappers carry, because all three are maintained by hand and all three once
 * silently lagged the neutral client. Name-level, since the wrapper's job is changing the types.
 */
class ReactorSftpClientParityTest {

    @Test
    void coversEveryNeutralOperationByName() {
        Set<String> wrapped = Arrays.stream(ReactorSftpClient.class.getDeclaredMethods())
            .map(Method::getName)
            .collect(Collectors.toSet());

        List<String> missing = Arrays.stream(ReactiveSftpClient.class.getMethods())
            .filter(m -> !m.isSynthetic())
            .map(Method::getName)
            .distinct()
            .filter(name -> !wrapped.contains(name))
            .sorted()
            .toList();

        assertThat(missing)
            .describedAs("operations on ReactiveSftpClient with no Reactor delegation; "
                + "each is a one-line Mono.fromCompletionStage(...) away")
            .isEmpty();
    }
}
