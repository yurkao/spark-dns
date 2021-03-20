package com.acme.dns.spark;

import com.google.common.base.Preconditions;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

@Slf4j
public class BindContainerFactory {
    static final String DOCKER_IMAGE_NAME = "yurkao/dns-bind:9.11";
    static final DockerImageName DOCKER_IMAGE = DockerImageName.parse(DOCKER_IMAGE_NAME);

    GenericContainer<?> createWin() {
       return new GenericContainer<>(DOCKER_IMAGE)
                .withClasspathResourceMapping("bind", "/etc/bind/", BindMode.READ_WRITE);
    }

    GenericContainer<?> createNix() {
        return new GenericContainer<>(DOCKER_IMAGE);
    }

    @SneakyThrows
    public GenericContainer<?> create() {
        final String osName = System.getProperty("os.name");
        log.info("Current OS name: {}", osName);
        boolean isWindows = osName.toLowerCase().startsWith("windows");
        GenericContainer<?> container;
        if (isWindows) {
            container = createWin();
        } else {
            container = createNix();
        }
        final GenericContainer<?> configuredContainer = container
                .withExposedPorts(53)
                .waitingFor(Wait.forListeningPort());

        configuredContainer.start();
        return configuredContainer;
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
        container.stop();
        deleteBindJournal();
    }
}
