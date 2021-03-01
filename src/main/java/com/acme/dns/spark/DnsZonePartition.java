package com.acme.dns.spark;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.spark.Partition;

@RequiredArgsConstructor
@Getter
public class DnsZonePartition implements Partition {
    private final int partitionId;
    private final DnsZoneParams zoneInfo;

    @Override
    public int index() {
        return partitionId;
    }
}
