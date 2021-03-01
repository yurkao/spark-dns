package com.acme.dns.xfr;


import com.acme.dns.spark.ZoneVersion;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.xbill.DNS.Record;
import org.xbill.DNS.SOARecord;
import org.xbill.DNS.ZoneTransferIn;

import java.util.ArrayList;
import java.util.List;

/**
 * Zone transfer handler.
 */
@Getter
@Slf4j
@ToString(onlyExplicitlyIncluded = true)
@RequiredArgsConstructor
public class DnsZoneTransferHandler implements ZoneTransferIn.ZoneTransferHandler {
    private final List<Record> axfr = new ArrayList<>();
    private final List<Delta> ixfr = new ArrayList<>();
    @ToString.Include
    private final ZoneVersion axfrSerial;
    private XfrType type = null;

    public void startAXFR() {
        log.info("Starting AXFR");
        type = XfrType.AXFR;
    }

    @Override
    public void startIXFR() {
        log.info("Starting IXFR");
        type = XfrType.IXFR;
    }

    @Override
    public void startIXFRDeletes(Record soa) {
        final long soaSerial = getSOASerial(soa);
        axfrSerial.setVersion(soaSerial);
        log.info("{} Starting IXFR deletes ", this);
        final Delta delta = new Delta();
        delta.getDeletes().add(soa);
        delta.setStart(soaSerial);
        ixfr.add(delta);
    }

    public void startIXFRAdds(Record soa) {
        final long soaSerial = getSOASerial(soa);
        axfrSerial.setVersion(soaSerial);
        log.info("{} Starting IXFR adds ", this);
        final Delta delta = ixfr.get(ixfr.size() - 1);
        delta.getAdds().add(soa);
        delta.setEnd(soaSerial);
    }

    private static long getSOASerial(Record rec) {
        final SOARecord soa = (SOARecord) rec;
        return soa.getSerial();
    }

    public void handleRecord(Record r) {
        Preconditions.checkArgument(type!=null, "start AXFR/IXFR should be called before");
        switch (type) {
            case AXFR:
                handleAxfrRecord(r);
                break;
            case IXFR:
                handleIxfrRecord(r);
                break;
            default: throw new IllegalArgumentException("Unhandled zone transfer type " + type);
        }
    }

    private void handleIxfrRecord(Record r) {
        final List<Record> list;
        final Delta delta = ixfr.get(ixfr.size() - 1);
        if (delta.getAdds().isEmpty()) {
            log.debug("Appending {} to IXFR deletes", r);
            list = delta.getDeletes();
        } else {
            log.debug("Appending {} to IXFR adds", r);
            list = delta.getAdds();
        }
        list.add(r);
    }


    private void handleAxfrRecord(Record r) {
        if (r instanceof SOARecord) {
            final long soaSerial = getSOASerial(r);
            axfrSerial.setVersion(soaSerial);
            log.info("Received new AXFR SOA record with Serial {}: {}", soaSerial, r);
        }
        axfr.add(r);
    }

}
