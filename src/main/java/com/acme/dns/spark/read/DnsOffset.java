package com.acme.dns.spark.read;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.execution.streaming.Offset;
import org.apache.spark.sql.execution.streaming.SerializedOffset;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

@ToString
@RequiredArgsConstructor
@Slf4j
public class DnsOffset extends Offset {
    private static final ObjectMapper mapper = new ObjectMapper();
    @Getter
    private final Map<String, ZoneOffset> zoneOffsetMap;
    /**
     * Used for serializing
     * @return serialized offsets for Spark to save them on persistent storage e.g. on Commit
     */
    @SneakyThrows
    @Override
    public String json() {
        return mapper.writeValueAsString(zoneOffsetMap);
    }


    protected static DnsOffset deserializeDnsOffset(String json) throws IOException {
        final TypeReference<Map<String, ZoneOffset>> typeRef = new TypeReference<>() {};
        final Map<String, ZoneOffset> tenantOffsets = mapper.readValue(json, typeRef);
        return new DnsOffset(tenantOffsets);
    }

    public static DnsOffset convert(org.apache.spark.sql.execution.streaming.Offset offset) throws IOException {
        final DnsOffset demoOffset;
        if (offset instanceof SerializedOffset) {
            log.debug("Resuming streaming from checkpoint: {}", offset);
            final String json = offset.json();
            demoOffset = deserializeDnsOffset(json);
            return demoOffset;
        } else {
            log.debug("On-going stream (currently running): {}", offset);
            demoOffset = (DnsOffset) offset;
        }
        return demoOffset;
    }

    public void log(String desc) {
        zoneOffsetMap.forEach((tid, offsetWindow) -> log.debug("\t{} {}: {}", desc, tid, offsetWindow));
    }


    /**
     * have to implement custom equals for serialized, because Structured Streaming does not advances to the next
     * batch of previously committed offsets equals to current offsets returned by
     * org.apache.spark.sql.execution.streaming.Source.getOffset method
     *
     * @param o previously committed offset
     * @return true if offsets are equal, false otherwise
     */
    @Override
    public boolean equals(final Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof DnsOffset)){
            return false;
        }
        final DnsOffset other = (DnsOffset) o;
        if (!other.canEqual(this)) {
            return false;
        }
        if (!zoneOffsetMap.keySet().equals(other.zoneOffsetMap.keySet())) {
            // offsets are not the same (not equals) if there's difference in zones
            // in worst case we will produce empty batch - if some zone was removed  and no data on remain tenants
            // (e.g. on streaming restart)
            return false;
        }
        for (Map.Entry<String, ZoneOffset> entry : zoneOffsetMap.entrySet()) {
            final String tenantId = entry.getKey();
            final ZoneOffset thisOffset = entry.getValue();
            final ZoneOffset otherOffset = other.zoneOffsetMap.get(tenantId);
            if (!Objects.equals(thisOffset, otherOffset)) {
                // specific zone progress differs
                return false;
            }
        }
        return true;
    }


    protected boolean canEqual(final Object other) {
        return other instanceof DnsOffset;
    }

    @Override
    public int hashCode() {
        return this.json().hashCode();
    }

}
