package com.acme.dns.spark;

import com.acme.dns.dao.OrgDnsRecord;
import com.acme.dns.xfr.Xfr;
import com.acme.dns.xfr.XfrType;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.xbill.DNS.ZoneTransferException;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
public class DnsZoneRDD extends RDD<Row> {

    // row class TAG is java API specific: not available in scala
    private static final ClassTag<Row> ROW_CLASS_TAG = scala.reflect.ClassTag$.MODULE$.apply(Row.class);
    // we have no RDD dependencies
    private static final Seq<Dependency<?>> DEPENDENCY_SEQ = JavaConverters.asScalaIteratorConverter(Collections.<Dependency<?>>emptyIterator()).asScala().toSeq();

    private final Partition[] partitions;
    private final Map<DnsZoneParams, ZoneVersion> zoneVersionMap;
    private final GlobalDnsParams globalDnsParams;


    public DnsZoneRDD(SparkContext sc, Map<DnsZoneParams, ZoneVersion> zoneVersionMap, GlobalDnsParams globalDnsParams) {
        super(sc, DEPENDENCY_SEQ, ROW_CLASS_TAG);
        this.zoneVersionMap = zoneVersionMap;
        this.globalDnsParams = globalDnsParams;

        int partitionId = 0;
        partitions = new DnsZonePartition[zoneVersionMap.size()];

        for(Map.Entry<DnsZoneParams, ZoneVersion> entry: zoneVersionMap.entrySet()) {
            final Partition partition = new DnsZonePartition(partitionId, entry.getKey());
            partitions[partitionId] = partition;
            partitionId++;
        }
    }


    /**
     * Get data for specific partition (invoked on executor side)
     *
     * @param split current partition
     * @param ignored task context
     * @return iterator or Spark rows
     */
    @SneakyThrows
    @Override
    public Iterator<Row> compute(Partition split, TaskContext ignored) {
        final DnsZonePartition partition = (DnsZonePartition) split;
        final DnsZoneParams zoneInfo = partition.getZoneInfo();
        final String orgName = zoneInfo.getOrgName();
        final ZoneVersion zoneVersion = zoneVersionMap.get(zoneInfo);

        final int timeout = globalDnsParams.getTimeout();
        final XfrType xfrType =  globalDnsParams.getXfrType();
        final Xfr source = new Xfr(zoneInfo.getName(), zoneInfo.getServer(), zoneVersion, timeout, xfrType);
        final long serial;
        if (XfrType.AXFR.equals(xfrType)) {
            serial = 0L; // on AXFR we always want to get all DNS records
        } else {
            serial = zoneInfo.getSerial();
        }

        List<OrgDnsRecord> dnsRecords;
        try {
            dnsRecords = source.fetch(serial);
        } catch (IOException | ZoneTransferException e) {
            if (!globalDnsParams.isIgnoreFailures()) {
                throw e;
            }
            log.info("Failed fetching data for {} with xfr={} and serial={}", partition, xfrType, serial, e);
            log.info("Suppressing error as requested");
            dnsRecords = Collections.emptyList();
        }

        final Timestamp ts = new Timestamp(System.currentTimeMillis());
        final DnsRecordToRowConverter rowConverter = new DnsRecordToRowConverter(ts, zoneInfo.getName().toString(), orgName);
        return JavaConverters.asScalaIteratorConverter(dnsRecords.stream().map(rowConverter).iterator()).asScala();
    }


    @Override
    public Partition[] getPartitions() {
        return partitions;
    }
}
