package com.acme.dns.spark;

import lombok.ToString;
import org.apache.spark.util.AccumulatorV2;

/**
 * per DNS zone accumulator sharable between driver and executor to track last loaded
 * SOA record.
 * Preparing for Structured Streaming read support
 * also good for Spark Web UI tracking
 */
@ToString
public class ZoneVersion extends AccumulatorV2<Long, Long> {
    private long version;

    @Override
    public boolean isZero() {
        return version == 0L;
    }

    @Override
    public AccumulatorV2<Long, Long> copy() {
        final ZoneVersion progress = new ZoneVersion();
        progress.version = version;
        return progress;
    }

    @Override
    public void reset() {
        version = 0L;
    }

    @Override
    public void add(Long v) {
        setVersion(v);
    }

    @Override
    public void merge(AccumulatorV2<Long, Long> other) {
        setVersion(other.value());
    }

    public void setVersion(long value) {
        // serial is increasing with zone static/dynamic updates, so
        // we are always interested in most up-to-date SOA serial
        version = Math.max(value, version);
    }

    @Override
    public Long value() {
        return version;
    }
}
