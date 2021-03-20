package com.acme.dns.spark.write;

import com.acme.dns.dao.DnsRecordUpdate;

import java.util.Comparator;

/**
 * Dns update comparator: sorts updates in chronological order
 */
class DnsChronologicalChange implements Comparator<DnsRecordUpdate> {
    @Override
    public int compare(DnsRecordUpdate o1, DnsRecordUpdate o2) {
        final long time1 = o1.getTimestamp().getTime();
        final long time2 = o2.getTimestamp().getTime();
        return Long.compare(time1, time2);
    }
}
