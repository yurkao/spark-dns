package com.acme.dns.dao;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.xbill.DNS.ARecord;

import java.util.function.Function;

@Slf4j
@RequiredArgsConstructor

public class RecordFactory implements Function<ARecord, OrgDnsRecord> {
    private final DnsAction action;


    @Override
    public OrgDnsRecord apply(ARecord record) {

        final OrgDnsRecord dnsRecord = new OrgDnsRecord();
        final String fqdn = record.getName().toString().toLowerCase();
        final String ip = record.getAddress().getHostAddress();
        dnsRecord.setIp(ip);
        dnsRecord.setFqdn(fqdn);
        dnsRecord.setAction(action);

        return dnsRecord;
    }
}
