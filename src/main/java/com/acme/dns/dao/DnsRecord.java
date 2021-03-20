package com.acme.dns.dao;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@Data
@NoArgsConstructor
@ToString
public class DnsRecord implements Serializable {
    @EqualsAndHashCode.Include
    private DnsAction action; // zone transfer type the record was retrieved
    @EqualsAndHashCode.Include
    private String fqdn;
    @EqualsAndHashCode.Include
    private String ip;
}
