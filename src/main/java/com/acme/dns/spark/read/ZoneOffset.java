package com.acme.dns.spark.read;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class ZoneOffset {
    private long serial;
    // timestamp of
    // thcurrent timestamp to force structured streaming execute `getBatch` method.
    // This is because we cannot POLL DNS zone without fetching all its data and Spark Structured Streaming invokes
    // `getBatch` IFF previous offsets returned by `getOffset` are not equal to currently returned offsets
    private long timestamp = System.currentTimeMillis();
    public ZoneOffset(long serial) {
        this.serial = serial;
    }

}
