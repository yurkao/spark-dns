package com.acme.dns.dao;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@ToString(callSuper = true)
public class OrgDnsRecordChange extends DnsRecordChange implements Serializable {
    // organization name the DNS server relates to
    private String organization;

}
