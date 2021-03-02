package com.acme.dns.spark;

import com.acme.dns.xfr.XfrType;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import org.xbill.DNS.Name;
import scala.collection.JavaConverters;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Global DNS data source options provided via either via spark.read.options()
 * or Spark SQL 'CREATE TABLE'
 */
@Getter
@ToString
public class DnsSourceOptions implements Serializable {
    public static final int MIN_PORT_VALUE = 1;
    public static final int MAX_PORT_VALUE = (2 << 16) - 1;
    // DNS server address (IP or FQDN)
    public static final String SERVER_OPT = "server";
    // DNS server TCP port to make zone transfers with
    public static final String PORT_OPT = "port";
    public static final String DEFAULT_PORT = "53";
    public static final String ORG_NAME_OPT = "organization";
    // initial serial to start zone transfers from
    public static final String SERIAL_OPT = "serial";
    public static final String DEFAULT_SERIAL = "0";
    // comma separated value of zone names
    public static final String ZONE_OPT = "zones";
    // zone transfer timeout (in seconds)
    public static final String XFR_TIMEOUT_OPT = "timeout";
    public static final String DEFAULT_XFR_TIMEOUT = "10";
    public static final String XFR_TYPE_OPT = "xfr";
    public static final String DEFAULT_XFR_TYPE = "IXFR";

    private final List<Name> zones;
    private final SocketAddress server;
    private final String organization;
    private final long initialSerial;
    private final int timeout;
    private final XfrType xfrType;


    public DnsSourceOptions(scala.collection.immutable.Map<String, String> parameters) {
        final Map<String, String> options = toJavaMap(parameters);
        zones = parseZones(options);
        server = parseServer(options);
        organization = parseOrganization(options);
        initialSerial = parseInitialSerial(options);
        timeout = parseXfrTimeout(options);
        xfrType = parseXfrType(options);
    }

    public static Map<String, String> toJavaMap(scala.collection.immutable.Map<String, String> parameters) {
        return JavaConverters.mapAsJavaMapConverter(parameters).asJava();
    }

    @SneakyThrows
    public static List<Name> parseZones(Map<String, String> options) {
        final String value = options.get(ZONE_OPT);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value), "DNS zone is required");
        return Arrays.stream(value.split(",")).distinct().map(DnsSourceOptions::parseZone).collect(Collectors.toList());
    }

    @SneakyThrows
    public static Name parseZone(String zoneName) {
        return Name.fromString(zoneName);
    }

    @SneakyThrows
    public static SocketAddress parseServer(Map<String, String> options) {
        final String serverValue = options.get(SERVER_OPT);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(serverValue), "DNS server address is required");
        final String portValue = options.getOrDefault(PORT_OPT, DEFAULT_PORT);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(portValue), "port value could not be empty");
        final int port = Integer.parseInt(portValue);

        Preconditions.checkArgument(port >= MIN_PORT_VALUE && port < MAX_PORT_VALUE,
                "DNS port value must be positive and less than " + MAX_PORT_VALUE);
        return new InetSocketAddress(serverValue, port);
    }

    @SneakyThrows
    public static String parseOrganization(Map<String, String> options) {
        final String value = options.get(ORG_NAME_OPT);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value), "Organization name is required");
        return value;
    }

    @SneakyThrows
    public static long parseInitialSerial(Map<String, String> options) {
        final String value = options.getOrDefault(SERIAL_OPT, DEFAULT_SERIAL);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value), "Initial serial count not be empty");
        final long serial = Long.parseLong(value);
        Preconditions.checkArgument(serial >=0, "Initial serial must be positive number");
        return serial;
    }

    @SneakyThrows
    public static int parseXfrTimeout(Map<String, String> options) {
        final String value = options.getOrDefault(XFR_TIMEOUT_OPT, DEFAULT_XFR_TIMEOUT);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value), "Initial serial count not be empty");
        final int timeout = Integer.parseInt(value);
        Preconditions.checkArgument(timeout >= 0, "Timeout must be positive number");
        return timeout;
    }

    @SneakyThrows
    public static XfrType parseXfrType(Map<String, String> options) {
        final String value = options.getOrDefault(XFR_TYPE_OPT, DEFAULT_XFR_TYPE);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value), "Xfr type not be empty");
        return XfrType.valueOf(value.toUpperCase());
    }
}
