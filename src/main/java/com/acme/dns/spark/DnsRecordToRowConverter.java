package com.acme.dns.spark;

import com.acme.dns.dao.OrgDnsRecord;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.function.Function;

/**
 * Convert DNS record to Spark Row
 */
@RequiredArgsConstructor
public class DnsRecordToRowConverter implements Function<OrgDnsRecord, Row>, Serializable {
    public static final StructType SCHEMA = Encoders.bean(OrgDnsRecord.class).schema()
            // timestamp of XFR
            .add("timestamp", DataTypes.TimestampType, false)
            // organization name the DNS server relates to
            .add("organization", DataTypes.StringType, false)
            // DNS zone name the DNS record relates to
            .add("zone", DataTypes.StringType, false)
            ;
    private final Timestamp ts;
    private final String zone;
    private final String orgName;

    @Override
    public Row apply(OrgDnsRecord record) {
        return RowFactory.create(record.getAction().name(), record.getFqdn(), record.getIp(), ts, orgName, zone);
    }
}
