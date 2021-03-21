package com.acme.dns.xfr;


import com.acme.dns.dao.DnsAction;
import com.acme.dns.dao.DnsChangeFactory;
import com.acme.dns.dao.DnsRecordChange;
import com.acme.dns.spark.read.ZoneVersion;
import lombok.extern.slf4j.Slf4j;
import org.xbill.DNS.*;

import java.io.IOException;
import java.net.SocketAddress;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class Xfr {
    private final Name zoneName;
    private final SocketAddress dnsServer;
    private final DnsZoneTransferHandler handler;
    private final int timeout;
    private final XfrType xfrType;

    public Xfr(Name zoneName, SocketAddress dnsServer, ZoneVersion zoneVersion, int timeout, XfrType xfrType) {
        this.zoneName = zoneName;
        this.dnsServer = dnsServer;
        handler = new DnsZoneTransferHandler(zoneVersion);
        this.timeout = timeout;
        this.xfrType = xfrType;
    }

    public List<DnsRecordChange> fetch(long serial) throws IOException, ZoneTransferException {
        log.info("Polling {} DNS zone with initial serial {} and timeout {}", zoneName, serial, timeout);
        final ZoneTransferIn xfr = ZoneTransferIn.newIXFR(zoneName, serial, false, dnsServer, null);
        xfr.setTimeout(Duration.of(timeout, ChronoUnit.SECONDS));
        xfr.run(handler);
        final XfrType resultXfrType;
        if (serial==0) {
            // this is very initial sync - so we want to get all DNS records
            resultXfrType = XfrType.AXFR;
        } else {
            resultXfrType = this.xfrType;
        }
        return getRecords(resultXfrType);
    }

    public List<DnsRecordChange> getRecords(XfrType xfrType) {
        final ArrayList<DnsRecordChange> dnsRecords = new ArrayList<>();
        log.info("Getting DNS records from {}", xfrType.name());
        if (XfrType.IXFR.equals(xfrType)) {
            final DnsChangeFactory deletesDnsChangeFactory = new DnsChangeFactory(DnsAction.IXFR_DELETE);
            final Stream<Record> deletedRecords = handler.getIxfr().stream()
                    .map(Delta::getDeletes).flatMap(Collection::stream);
            final List<DnsRecordChange> deletes = getOrgDnsRecords(deletesDnsChangeFactory, deletedRecords, "IXFR deletes");
            dnsRecords.addAll(deletes);

            final DnsChangeFactory addFactory = new DnsChangeFactory(DnsAction.IXFR_ADD);
            final Stream<Record> addedRecords = handler.getIxfr().stream()
                    .map(Delta::getAdds).flatMap(Collection::stream);
            final List<DnsRecordChange> adds = getOrgDnsRecords(addFactory, addedRecords, "IXFR adds");
            dnsRecords.addAll(adds);
        } else {
            final Stream<Record> axfrRecords = handler.getAxfr().stream();
            final DnsChangeFactory fullSyncFactory = new DnsChangeFactory(DnsAction.AXFR);
            final List<DnsRecordChange> axfr = getOrgDnsRecords(fullSyncFactory, axfrRecords, "AXFR");
            dnsRecords.addAll(axfr);
        }
        return dnsRecords;
    }

    private List<DnsRecordChange> getOrgDnsRecords(DnsChangeFactory dnsChangeFactory, Stream<Record> recordStream, String recordType) {
        final List<DnsRecordChange> dnsRecords = recordStream
                .filter(record -> Type.A == record.getType())
                .map(ARecord.class::cast)
                .map(dnsChangeFactory)
                .collect(Collectors.toList());
        if (!dnsRecords.isEmpty()) {
            log.info("Received {} {} records", dnsRecords.size(), recordType);
        }
        return dnsRecords;
    }

}