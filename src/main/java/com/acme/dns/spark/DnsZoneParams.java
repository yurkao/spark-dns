package com.acme.dns.spark;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.xbill.DNS.Name;

import java.io.Serializable;
import java.net.SocketAddress;

/**
 * Single zone transfer param (per DNS zone)
 */
@RequiredArgsConstructor
@ToString
@Data
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class DnsZoneParams implements Serializable {
    @EqualsAndHashCode.Include
    private final Name name;
    private final SocketAddress server;
    private final long serial;
    private final String orgName;

    /**
     * Make a copy of zone parameters for specified zone serial
     * @param serial new serial
     * @return copy of current zone with specified serial
     */
    public DnsZoneParams copy(long serial) {
        return new DnsZoneParams(name, server, serial, orgName);
    }
}
