package com.acme.dns.spark.read;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.spark.Partition;

@RequiredArgsConstructor
@Getter
@ToString
public class DnsZonePartition implements Partition {
    private final int partitionId;
    private final DnsZoneParams zoneInfo;

    @Override
    public int index() {
        return partitionId;
    }
}
