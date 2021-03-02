package com.acme.dns.spark;

import com.acme.dns.xfr.XfrType;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.streaming.Source;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.sources.StreamSourceProvider;
import org.apache.spark.sql.types.StructType;
import org.xbill.DNS.Name;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.Map;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.List;


@Slf4j
public class DnsSourceRelationProvider implements
        RelationProvider, // batch read
        StreamSourceProvider, // streaming read
        DataSourceRegister {

    @Override
    public String shortName() {
        return "dns";
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, Map<String, String> parameters) {
        final DnsSourceOptions options = new DnsSourceOptions(parameters);
        final HashMap<DnsZoneParams, ZoneVersion> map = createDnsZoneVersionMap(sqlContext, options);
        final int timeout = options.getTimeout();
        final XfrType xfrType = options.getXfrType();
        log.info("Loading from {} DNS zones with {}", map.size(), xfrType.name());
        return new DnsSourceRelation(sqlContext, map, timeout, xfrType);
    } // allow to pick/lookup data source by name

    @Override
    public Tuple2<String, StructType> sourceSchema(SQLContext sqlContext, Option<StructType> schema, String providerName, Map<String, String> parameters) {
        return Tuple2.apply(this.shortName(), DnsRecordToRowConverter.SCHEMA);
    }

    @Override
    public Source createSource(SQLContext sqlContext, String metadataPath, Option<StructType> schema, String providerName, Map<String, String> parameters) {
        final DnsSourceOptions options = new DnsSourceOptions(parameters);
        final HashMap<DnsZoneParams, ZoneVersion> dnsZoneVersionMap = createDnsZoneVersionMap(sqlContext, options);
        final int timeout = options.getTimeout();
        final XfrType xfrType = options.getXfrType();
        log.info("Loading from {} DNS zones", dnsZoneVersionMap.size());
        return new DnsStreamingSource(sqlContext, metadataPath, dnsZoneVersionMap, timeout, xfrType);
    }

    private HashMap<DnsZoneParams, ZoneVersion> createDnsZoneVersionMap(SQLContext sqlContext, DnsSourceOptions options) {
        final List<Name> zones = options.getZones();
        final HashMap<DnsZoneParams, ZoneVersion> map = new HashMap<>();
        zones.forEach(zone -> {
            final String organization = options.getOrganization();
            final SocketAddress dnsServer = options.getServer();
            final long initialSerial = options.getInitialSerial();
            final DnsZoneParams zoneInfo = new DnsZoneParams(zone, dnsServer, initialSerial, organization);
            final ZoneVersion zoneVersion = new ZoneVersion();
            final String accName = String.format("%s %s", organization, zone.toString());
            sqlContext.sparkContext().register(zoneVersion, accName);
            map.put(zoneInfo, zoneVersion);
        });
        return map;
    }
}
