package com.acme.dns.xfr;


import com.acme.dns.spark.ZoneVersion;
import com.acme.dns.dao.DnsAction;
import com.acme.dns.dao.OrgDnsRecord;
import com.acme.dns.dao.RecordFactory;
import lombok.extern.slf4j.Slf4j;
import org.xbill.DNS.*;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class Xfr {
    private final Name zoneName;
    private final SocketAddress dnsServer;
    private final ZoneVersion zoneVersion;
    private final DnsZoneTransferHandler handler;

    public Xfr(Name zoneName, SocketAddress dnsServer, ZoneVersion zoneVersion) {
        this.zoneName = zoneName;
        this.dnsServer = dnsServer;
        this.zoneVersion = zoneVersion;
        handler = new DnsZoneTransferHandler(zoneVersion);
    }

    public List<OrgDnsRecord> fetch(long serial) throws IOException, ZoneTransferException {
        log.debug("Polling with serial: {}", zoneVersion);
        final ZoneTransferIn xfr = ZoneTransferIn.newIXFR(zoneName, serial, false, dnsServer, null);
        xfr.setTimeout(60);
        xfr.run(handler);
        return getRecords();
    }

    public List<OrgDnsRecord> getRecords() {

        final RecordFactory deletesRecordFactory = new RecordFactory(DnsAction.IXFR_DELETE);
        final Stream<Record> deletedRecords = handler.getIxfr().stream()
                .map(Delta::getDeletes).flatMap(Collection::stream);
        final List<OrgDnsRecord> deletes = getOrgDnsRecords(deletesRecordFactory, deletedRecords, "IXFR deletes");
        final ArrayList<OrgDnsRecord> dnsRecords = new ArrayList<>(deletes);

        final RecordFactory addFactory = new RecordFactory(DnsAction.IXFR_ADD);
        final Stream<Record> addedRecords = handler.getIxfr().stream()
                .map(Delta::getAdds).flatMap(Collection::stream);
        final List<OrgDnsRecord> adds = getOrgDnsRecords(addFactory, addedRecords, "IXFR adds");
        dnsRecords.addAll(adds);

        final Stream<Record> axfrRecords = handler.getAxfr().stream();
        final RecordFactory fullSyncFactory = new RecordFactory(DnsAction.AXFR);
        final List<OrgDnsRecord> axfr = getOrgDnsRecords(fullSyncFactory, axfrRecords, "AXFR");
        dnsRecords.addAll(axfr);
        return dnsRecords;
    }

    private List<OrgDnsRecord> getOrgDnsRecords(RecordFactory recordFactory, Stream<Record> recordStream, String recordType) {
        final List<OrgDnsRecord> dnsRecords = recordStream
                .filter(record -> Type.A == record.getType())
                .map(ARecord.class::cast)
                .map(recordFactory)
                .collect(Collectors.toList());
        if (!dnsRecords.isEmpty()) {
            log.info("Received {} {} records", dnsRecords.size(), recordType);
        }
        return dnsRecords;
    }

}