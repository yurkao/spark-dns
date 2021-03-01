package com.acme.dns.spark;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.xbill.DNS.Name;

import java.io.Serializable;
import java.net.SocketAddress;

/**
 * Single zone tranfer param (per DNS zone)
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
}
