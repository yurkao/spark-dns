package com.acme.dns.xfr;

/**
 * Supported DNS zone transfer strategies used by the Spark 3.5.x source.
 * <ul>
 *     <li>{@code AXFR} - full zone transfer.</li>
 *     <li>{@code IXFR} - incremental zone transfer.</li>
 * </ul>
 */
public enum XfrType {
    AXFR,
    IXFR
}
