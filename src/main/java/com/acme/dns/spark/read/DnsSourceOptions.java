package com.acme.dns.spark.read;

import com.acme.dns.spark.common.DnsOptions;
import com.acme.dns.xfr.XfrType;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import org.xbill.DNS.Name;

import java.io.Serializable;
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
@EqualsAndHashCode(callSuper = true) // sonarlint
public class DnsSourceOptions extends DnsOptions implements Serializable {

    public static final String ORG_NAME_OPT = "organization";
    // initial serial to start zone transfers from
    public static final String SERIAL_OPT = "serial";
    public static final String DEFAULT_SERIAL = "0";
    // comma separated value of zone names
    public static final String ZONE_OPT = "zones";

    public static final String XFR_TYPE_OPT = "xfr";
    public static final String DEFAULT_XFR_TYPE = "IXFR";
    public static final String IGNORE_FAILURES_OPT = "ignore-failures";
    public static final String DEFAULT_IGNORE_FAILURES = "false";
    public static final String MAX_KEPT_COMMITS = "max-kept-commits";
    public static final String DEFAULT_MAX_KEPT_COMMITS = "10";

    private final List<Name> zones;
    private final String organization;
    private final long initialSerial;
    private final XfrType xfrType;
    private final boolean ignoreFailures;
    private final int maxKeptCommits; // streaming only


    public DnsSourceOptions(scala.collection.immutable.Map<String, String> parameters) {
        super(parameters);
        zones = parseZones(options);
        organization = parseOrganization(options);
        initialSerial = parseInitialSerial(options);
        xfrType = parseXfrType(options);
        ignoreFailures = parseIgnoreChanges(options);
        maxKeptCommits = parseMaxKeptCommits(options);
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
    public static XfrType parseXfrType(Map<String, String> options) {
        final String value = options.getOrDefault(XFR_TYPE_OPT, DEFAULT_XFR_TYPE);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value), "Xfr type not be empty");
        return XfrType.valueOf(value.toUpperCase());
    }

    @SneakyThrows
    public static boolean parseIgnoreChanges(Map<String, String> options) {
        final String value = options.getOrDefault(IGNORE_FAILURES_OPT, IGNORE_FAILURES_OPT);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value), "ignore failures must not be empty");
        return Boolean.parseBoolean(value.toLowerCase());
    }

    @SneakyThrows
    public static int parseMaxKeptCommits(Map<String, String> options) {
        final String value = options.getOrDefault(MAX_KEPT_COMMITS, DEFAULT_MAX_KEPT_COMMITS);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value), "Max kept commits not be empty");
        final int maxKeptCommits = Integer.parseInt(value);
        Preconditions.checkArgument(maxKeptCommits >= 0, "Max kept commits must be positive number");
        return maxKeptCommits;
    }
}
