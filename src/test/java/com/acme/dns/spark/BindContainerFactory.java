package com.acme.dns.spark;

import com.google.common.base.Preconditions;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.InternetProtocol;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@Slf4j
public class BindContainerFactory {
    private static final Path DOCKERFILE_DIR = Paths.get("src/test/resources");
    private static final int INTERNAL_DNS_PORT = 53;

    @SneakyThrows
    public GenericContainer<?> create(int port) {
        final List<String> bindings = createPortBindings(port);

        final ImageFromDockerfile image = new ImageFromDockerfile("custom-bind-image")
                .withFileFromPath(".", DOCKERFILE_DIR)
                .withFileFromPath("Dockerfile", DOCKERFILE_DIR.resolve("Dockerfile"));
        final GenericContainer<?> container = new GenericContainer<>(image)
                .withExposedPorts(INTERNAL_DNS_PORT)
                .waitingFor(Wait.forListeningPorts(INTERNAL_DNS_PORT));

        container.setPortBindings(bindings);
        container.start();
        return container;
    }

    /**
     * Create DNS docker port bindings (CP and UDP) to avoid listening on privileged port under ono-privileged user
     *
     * @param port non-privileged port to bind service port to,
     * @return list of port bindings.
     */
    private static @NotNull List<String> createPortBindings(final int port) {
        return Arrays.asList(
                // UDP: <external>:53/udp
                String.format("%d:%d/%s", port, INTERNAL_DNS_PORT, InternetProtocol.UDP),
                // TCP: <external>:53/tcp
                String.format("%d:%d/%s", port, INTERNAL_DNS_PORT, InternetProtocol.TCP)
        );
    }

    public static void deleteBindJournal() throws URISyntaxException, IOException {
        final URL resource = BindContainerFactory.class.getClassLoader().getResource("bind");
        Preconditions.checkArgument(Objects.nonNull(resource), "Bind9 configuration dir is not found");
        final Path bindDir = Paths.get(resource.toURI());

        Files.list(bindDir)
                .filter(BindContainerFactory::isBindJournal)
                .forEach(BindContainerFactory::deleteJournalFile);
    }

    @SneakyThrows
    private static void deleteJournalFile(Path journalPath) {
        log.info("Deleting Bind9 journal: {}", journalPath);
        Files.deleteIfExists(journalPath);
    }

    private static boolean isBindJournal(Path f) {
        return f.getFileName().toString().endsWith(".jnl");
    }

    @SneakyThrows
    public void stop(GenericContainer<?> container) {
        if (log.isTraceEnabled()) {
            log.info("Container logs: {}", container.getLogs());
        }
        if (container != null) {
            container.stop();
        }
        deleteBindJournal();
    }
}
