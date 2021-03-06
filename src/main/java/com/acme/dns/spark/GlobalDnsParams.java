package com.acme.dns.spark;

import com.acme.dns.xfr.XfrType;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;

/**
 * DNS zones wide parameters
 */
@RequiredArgsConstructor
@Getter
public class GlobalDnsParams implements Serializable {
    private final int timeout; // DNS zone transfer timeout
    private final XfrType xfrType; // DNS zone transfer type
    private final boolean ignoreFailures;
}
