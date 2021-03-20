package com.acme.dns.dao;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

@EqualsAndHashCode(callSuper = true, onlyExplicitlyIncluded = true)
@Data
@ToString(callSuper = true)
public class DnsRecordUpdate extends DnsRecord implements Serializable {
    private Timestamp timestamp = new Timestamp(0L);
    private int ttl = (int)Duration.of(1, ChronoUnit.HOURS).toMillis();
}
