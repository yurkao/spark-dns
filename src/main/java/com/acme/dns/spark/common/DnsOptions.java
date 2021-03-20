package com.acme.dns.spark.common;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Data;
import lombok.SneakyThrows;
import scala.collection.JavaConverters;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Map;


@Data
public class DnsOptions implements Serializable {
    public static final int MIN_PORT_VALUE = 1;
    public static final int MAX_PORT_VALUE = (2 << 16) - 1;
    // DNS server address (IP or FQDN)
    public static final String SERVER_OPT = "server";
    // DNS server TCP port to make zone transfers with
    public static final String PORT_OPT = "port";
    public static final String DEFAULT_PORT = "53";
    // zone transfer timeout (in seconds)
    public static final String XFR_TIMEOUT_OPT = "timeout";
    public static final String DEFAULT_XFR_TIMEOUT = "10";
    protected final transient Map<String, String> options;
    private final InetSocketAddress server;
    private final int timeout;

    public DnsOptions(scala.collection.immutable.Map<String, String> parameters) {
        options = toJavaMap(parameters);
        timeout = parseXfrTimeout(options);
        server = parseServer(options);

    }

    public static Map<String, String> toJavaMap(scala.collection.immutable.Map<String, String> parameters) {
        return JavaConverters.mapAsJavaMapConverter(parameters).asJava();
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
    public static InetSocketAddress parseServer(Map<String, String> options) {
        final String serverValue = options.get(SERVER_OPT);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(serverValue), "DNS server address is required");
        final String portValue = options.getOrDefault(PORT_OPT, DEFAULT_PORT);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(portValue), "port value could not be empty");
        final int port = Integer.parseInt(portValue);

        Preconditions.checkArgument(port >= MIN_PORT_VALUE && port < MAX_PORT_VALUE,
                "DNS port value must be positive and less than " + MAX_PORT_VALUE);
        return new InetSocketAddress(serverValue, port);
    }
}
