package com.acme.dns.spark.write;

import com.acme.dns.dao.DnsAction;
import com.acme.dns.dao.DnsRecord;
import com.acme.dns.dao.DnsRecordUpdate;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.xbill.DNS.Name;

import java.net.InetSocketAddress;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@RequiredArgsConstructor
@Slf4j
public class DnsPartitionHandler implements ForeachPartitionFunction<Row> {
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

        final Map<Name, List<DnsRecordUpdate>> zonedRecords = StreamSupport.stream(spliterator,false)
                .map(this::decode)
                .filter(Objects::nonNull)
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
     * Decode Spark Row to DNS record update POJO
     * @param row Spark Row
     * @return decoded DNS record update
     * @throws IllegalArgumentException in case of value errors
     */
    public DnsRecordUpdate decode(Row row) {
        final DnsRecordUpdate update = new DnsRecordUpdate();
        update.setTtl(row.getInt(row.fieldIndex("ttl")));
        final String action = row.getString(row.fieldIndex("action"));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(action), "Invalid action " + action + " in "+ row);
        update.setAction(DnsAction.valueOf(action));
        final Timestamp timestamp = row.getTimestamp(row.fieldIndex("timestamp"));
        Preconditions.checkArgument(Objects.nonNull(timestamp), "Invalid timestamp " + timestamp + " in "+ row);
        update.setTimestamp(timestamp);
        final String ip = row.getString(row.fieldIndex("ip"));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(ip), "Invalid IP address " + ip + " in "+ row);
        update.setIp(ip);
        String fqdn = row.getString(row.fieldIndex("fqdn"));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(fqdn), "Invalid fqdn " + fqdn + " in "+ row);
        if (!fqdn.endsWith(".")) {
            fqdn += ".";
        }
        update.setFqdn(fqdn);
        return update;
    }
}
