package com.acme.dns.dao;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.xbill.DNS.ARecord;

import java.util.function.Function;

@Slf4j
@RequiredArgsConstructor

public class DnsChangeFactory implements Function<ARecord, DnsRecordChange> {
    private final DnsAction action;


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
