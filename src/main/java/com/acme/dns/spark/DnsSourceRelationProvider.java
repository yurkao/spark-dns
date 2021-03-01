package com.acme.dns.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.RelationProvider;
import org.xbill.DNS.Name;
import scala.collection.immutable.Map;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.List;


@Slf4j
public class DnsSourceRelationProvider implements
        RelationProvider, // batch read
        DataSourceRegister {

    @Override
    public String shortName() {
        return "dns";
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, Map<String, String> parameters) {
        final DnsSourceOptions options = new DnsSourceOptions(parameters);
        final List<Name> zones = options.getZones();
        final HashMap<DnsZoneParams, ZoneVersion> map = new HashMap<>();
        log.info("Loading from {} DNS zones", zones.size());
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

        return new DnsSourceRelation(sqlContext, map);
    } // allow to pick/lookup data source by name

}
