package com.acme.dns.dao;

/**
 * Type of DNS change represented in streaming/batch output.
 */
public enum DnsAction {
    IXFR_ADD,
    IXFR_DELETE,
    AXFR
}
