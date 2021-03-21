package com.acme.dns.spark.write;

import com.acme.dns.dao.DnsRecordUpdate;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.xbill.DNS.*;

import java.io.IOException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class DnsUpdate {
    private static final int RECORD_TYPE = Type.A;
    private static final int DEFAULT_TIMEOUT = 60;
    private static final DnsChronologicalChange CHRONOLOGICAL_ORDER = new DnsChronologicalChange();
    final SimpleResolver resolver;

    public DnsUpdate(String dnsServerAddress, int port) throws UnknownHostException {
        this(dnsServerAddress, port, DEFAULT_TIMEOUT);
    }

    public DnsUpdate(String dnsServerAddress, int port, int timeout) throws UnknownHostException {
        resolver = new SimpleResolver(dnsServerAddress);

        resolver.setTimeout(Duration.of(timeout, ChronoUnit.SECONDS));
        resolver.setTCP(true);
        resolver.setPort(port);
        if(log.isDebugEnabled()) {
            Options.set("verbose");
        }
    }

    /**
     * Update DNS record in specified DNS zone
     * @param zone DNS zone to send updates to
     * @param updates DNS updates
     * @throws IOException in case of error
     */
    public void update(Name zone, Collection<DnsRecordUpdate> updates) throws IOException {
        final Map<DnsRecordUpdate, Optional<DnsRecordUpdate>> collect = updates.stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.maxBy(CHRONOLOGICAL_ORDER)));

        // get most recent update per DNS record
        final Collection<DnsRecordUpdate> chronoUpdates = collect.values().stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        if (chronoUpdates.isEmpty()) {
            // avoid network IO on empty updates
            log.info("No DNS updates for {} zone", zone);
            return;
        }
        log.info("Updating DNS zone {} in {} with {} updates", zone, resolver.getAddress(), chronoUpdates.size());
        if (log.isDebugEnabled()) {
            chronoUpdates.forEach(update -> log.debug("\t{}", update));
        }
        final Update update = new Update(zone);
        for (DnsRecordUpdate change: chronoUpdates) {
            switch (change.getAction()) {
                case AXFR:
                case IXFR_ADD:
                    update.add(Name.fromString(change.getFqdn()), RECORD_TYPE, change.getTtl(), change.getIp());
                    break;
                case IXFR_DELETE:
                    update.delete(Name.fromString(change.getFqdn()), RECORD_TYPE, change.getIp());
            }
        }

        final Message response = resolver.send(update);
        log.debug("Update response for {}: {}", zone, response);
        final int responseCode = response.getRcode();
        Preconditions.checkArgument(responseCode == 0,
                "Failed to update DNS zone " + zone + ": " + response);
    }
}
