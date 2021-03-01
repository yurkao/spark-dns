package com.acme.dns.dao;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

/**
 * DNS A record
 */
@NoArgsConstructor
@ToString
@Data
public class OrgDnsRecord implements Serializable {
    private DnsAction action; // zone transfer type the record was retrieved
    private String fqdn;
    private String ip;
}
