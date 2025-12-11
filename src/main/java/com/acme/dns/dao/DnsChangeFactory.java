package com.acme.dns.dao;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.xbill.DNS.ARecord;

import java.util.function.Function;

@Slf4j
@RequiredArgsConstructor

/**
 * Maps DNS Java records into Spark-friendly change objects tagged with an action.
 */
public class DnsChangeFactory implements Function<ARecord, DnsRecordChange> {
    private final DnsAction action;


    /**
     * Convert a DNS {@link ARecord} into a {@link DnsRecordChange} with the factory's action.
     * @param record incoming DNS record
     * @return normalized change representation
     */
    @Override
    public DnsRecordChange apply(ARecord record) {

        final DnsRecordChange dnsRecord = new DnsRecordChange();
        final String fqdn = record.getName().toString().toLowerCase();
        final String ip = record.getAddress().getHostAddress();
        dnsRecord.setIp(ip);
        dnsRecord.setFqdn(fqdn);
        dnsRecord.setAction(action);

        return dnsRecord;
    }
}
