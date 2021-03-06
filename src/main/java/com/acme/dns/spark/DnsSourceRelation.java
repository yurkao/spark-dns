package com.acme.dns.spark;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.StructType;

import java.util.Map;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Slf4j
public class DnsSourceRelation extends BaseRelation implements TableScan {
    private final SQLContext sqlContext;
    private final Map<DnsZoneParams, ZoneVersion> zoneVersionMap; // DNS zone
    private final GlobalDnsParams globalDnsParams;

    @Override
    public SQLContext sqlContext() {
        return sqlContext;
    }

    @Override
    public StructType schema() {
        return DnsRecordToRowConverter.SCHEMA;
    }

    /**
     * Create readable DNS table (invoked on driver side)
     * @return RDD of DNS rows
     */
    @Override
    public RDD<Row> buildScan() {
        return new DnsZoneRDD(sqlContext.sparkContext(), zoneVersionMap, globalDnsParams);
    }

    @Override
    public String toString() {
        return zoneVersionMap.keySet().stream().map(DnsZoneParams::toString).collect(Collectors.joining(","));
    }
}
