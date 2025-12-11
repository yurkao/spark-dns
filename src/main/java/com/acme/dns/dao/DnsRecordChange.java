package com.acme.dns.dao;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * DNS A record produced by a zone transfer, enriched with the originating zone and timestamp.
 */
@NoArgsConstructor
@ToString
@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = true)
@Data
public class DnsRecordChange extends DnsRecord implements Serializable {
    // timestamp of XFR
    private Timestamp timestamp = new Timestamp(0L);
    // DNS zone name the DNS record relates to
    private String zone;
}
