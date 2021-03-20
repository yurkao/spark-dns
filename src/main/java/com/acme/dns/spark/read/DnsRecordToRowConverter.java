package com.acme.dns.spark.read;

import com.acme.dns.dao.DnsRecordChange;
import com.acme.dns.dao.OrgDnsRecordChange;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.function.Function;

/**
 * Convert DNS record to Spark Row
 */
@RequiredArgsConstructor
public class DnsRecordToRowConverter implements Function<DnsRecordChange, Row>, Serializable {
    public static final StructType SCHEMA = Encoders.bean(OrgDnsRecordChange.class).schema();
    private final Timestamp ts;
    private final String zone;
    private final String orgName;

    @Override
    public Row apply(DnsRecordChange record) {
        // create parameters should in alpha-ordered to match Encoder' schema
        return RowFactory.create(record.getAction().name(), record.getFqdn(), record.getIp(), orgName,  ts, zone);
    }
}
