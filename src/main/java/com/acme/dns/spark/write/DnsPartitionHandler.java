package com.acme.dns.spark.write;

import com.acme.dns.dao.DnsRecord;
import com.acme.dns.dao.DnsRecordUpdate;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.xbill.DNS.Name;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@RequiredArgsConstructor
@Slf4j
public class DnsPartitionHandler implements ForeachPartitionFunction<Row> {
    public static final String JSON_COLUMN = "json"; 
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final TypeReference<DnsRecordUpdate> typeRef = new TypeReference<>() {};

    private final DnsSinkOptions options;

    /**
     * partition handling: invoked on executor
     * @param rowIterator rows
     * @throws Exception in case of error
     */
    @Override
    public void call(Iterator<Row> rowIterator) throws Exception {
        if (!rowIterator.hasNext()) {
            return;
        }
        final Spliterator<Row> spliterator = Spliterators.spliteratorUnknownSize(rowIterator, Spliterator.ORDERED);

        // per zone updates
        final Map<Name, List<DnsRecordUpdate>> zonedRecords = StreamSupport.stream(spliterator,false)
                .map(DnsPartitionHandler::decode)
                .map(DnsPartitionHandler::normalize)
                .collect(Collectors.groupingBy(DnsPartitionHandler::getZone));
        final InetSocketAddress server = options.getServer();
        final DnsUpdate dnsUpdate = new DnsUpdate(server.getHostName(), server.getPort(), options.getTimeout());

        for (Map.Entry<Name, List<DnsRecordUpdate>> entry : zonedRecords.entrySet()) {
            final Name zone = entry.getKey();
            final Collection<DnsRecordUpdate> updates = entry.getValue();
            dnsUpdate.update(zone, updates);
        }
    }

    /**
     * Get DNS zone from FQDN in DNS record, e.g. mail.google.com -> google.com.
     * @param record DNS record
     * @return fully qualified DNS zone name (incl. trailing dot)
     */
    @SneakyThrows
    public static Name getZone(DnsRecord record) {
        final String fqdn = record.getFqdn();
        // empty
        Preconditions.checkArgument(!Strings.isNullOrEmpty(fqdn),
                "Cannot determine DNS zone from DNS record: " + record);
        String zoneName = Arrays.stream(fqdn.split("\\.")).skip(1).collect(Collectors.joining("."));
        if (!zoneName.endsWith(".")) {
            zoneName += ".";
        }
        return Name.fromString(zoneName);
    }

    /**
     * Validate DNS update values
     * @param update DNS update
     * @throws IllegalArgumentException on invalid value in update
     */
    public static void validate(DnsRecordUpdate update) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(update.getIp()), "Invalid IP  in " + update);
        Preconditions.checkArgument(Objects.nonNull(update.getAction()), "Invalid action in " + update);
        Preconditions.checkArgument(Objects.nonNull(update.getTimestamp()), "Invalid timestamp in " + update);
        Preconditions.checkArgument(update.getTtl() > 0, "Invalid TTL in " + update);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(update.getFqdn()), "Invalid FQDN in " + update);
    }

    /**
     * Normalize DNS update record: make FQDN be absolute
     *
     * @param update DNS update to normalize
     * @return normalized DNS update
     */
    public static DnsRecordUpdate normalize(DnsRecordUpdate update) {
        final String fqdn = update.getFqdn();
        Preconditions.checkArgument(!Strings.isNullOrEmpty(fqdn), "Invalid FQDN in " + update);
        if (!fqdn.endsWith(".")) {
            // make absolute FQDN
            update.setFqdn(fqdn + ".");
        }
        return update;
    }

    /**
     * Decode Spark Row to DNS record update POJO
     * @param row Spark Row
     * @return decoded DNS record update
     * @throws IllegalArgumentException in case of value errors
     */
    @SneakyThrows
    public static DnsRecordUpdate decode(Row row)  {
        final DnsRecordUpdate update = mapper.readValue(row.getString(row.fieldIndex(JSON_COLUMN)), typeRef);
        log.debug("Decoded DNS update: {}", update);
        validate(update);
        return update;
    }
}
