package com.acme.dns.spark.write;

import com.acme.dns.dao.DnsAction;
import com.acme.dns.dao.DnsRecordUpdate;
import com.acme.dns.spark.BindContainerFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.xbill.DNS.Name;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Slf4j
class DnsUpdateTest {
    static final BindContainerFactory CONTAINER_FACTORY = new BindContainerFactory();

    @Container
    GenericContainer<?> container;

    @SneakyThrows
    @BeforeEach
    void setUp() {
        container = CONTAINER_FACTORY.create();
    }

    @AfterEach
    void tearDown() {
        CONTAINER_FACTORY.stop(container);
    }

    @Test
    void updateOK() throws IOException {
        final String zoneName = "example.acme.";

        final DnsRecordUpdate change = new DnsRecordUpdate();
        change.setAction(DnsAction.IXFR_ADD);
        change.setTimestamp(new Timestamp(System.currentTimeMillis()));
        change.setIp("127.0.0.1");
        change.setFqdn("foo." + zoneName);
        change.setTtl(1234);
        final DnsUpdate dnsUpdate = new DnsUpdate(container.getHost(), container.getFirstMappedPort());
        final Name zone = Name.fromString(zoneName);
        final Set<DnsRecordUpdate> updates = Collections.singleton(change);
        assertThatCode(() -> dnsUpdate.update(zone, updates))
                .as("simple update should succeed")
                .doesNotThrowAnyException();
    }

    @Test
    void updateFailure() throws IOException {
        final String zoneName = "foo.bar.baz.";

        final DnsRecordUpdate change = new DnsRecordUpdate();
        change.setAction(DnsAction.IXFR_ADD);
        change.setTimestamp(new Timestamp(System.currentTimeMillis()));
        change.setIp("127.0.0.1");
        change.setFqdn("foo." + zoneName);
        change.setTtl(1234);
        final DnsUpdate dnsUpdate = new DnsUpdate(container.getHost(), container.getFirstMappedPort());
        final Name zone = Name.fromString(zoneName);
        final Set<DnsRecordUpdate> updates = Collections.singleton(change);
        assertThatThrownBy(() ->dnsUpdate.update(zone, updates))
                .as("Updating non-existing zone should fail")
                .isInstanceOf(IllegalArgumentException.class);
    }
}
