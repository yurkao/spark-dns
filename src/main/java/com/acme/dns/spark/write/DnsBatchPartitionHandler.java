package com.acme.dns.spark.write;

import com.acme.dns.dao.DnsRecordUpdate;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.acme.dns.spark.write.DnsPartitionHandler.*;

@RequiredArgsConstructor
@Slf4j
public class DnsBatchPartitionHandler implements ForeachPartitionFunction<Row> {
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
        final Stream<DnsRecordUpdate> dnsRecordUpdateStream = StreamSupport.stream(spliterator, false)
                .map(DnsBatchPartitionHandler::decode);
        final DnsPartitionHandler partitionHandler = new DnsPartitionHandler(options);
        partitionHandler.runUpdate(dnsRecordUpdateStream);
    }

    /**
     * Decode Spark Row to DNS record update POJO
     * @param row Spark Row
     * @return decoded DNS record update
     * @throws IllegalArgumentException in case of value errors
     */
    @SneakyThrows
    public static DnsRecordUpdate decode(Row row)  {
        final DnsRecordUpdate update = mapper.readValue(row.getString(row.fieldIndex(UPDATE_COLUMN)), typeRef);
        log.debug("Decoded batch DNS update: {}", update);
        return update;
    }
}
