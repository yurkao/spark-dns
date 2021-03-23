package com.acme.dns.spark.write;

import com.acme.dns.dao.DnsRecordUpdate;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.acme.dns.spark.write.DnsPartitionHandler.*;

/**
 * Streaming partition handler
 */
@Slf4j
@RequiredArgsConstructor
public class DnsStreamingSink implements VoidFunction<Iterator<InternalRow>> {
    private final StructType schema;
    private final DnsSinkOptions options;

    // invoked on executor side
    @Override
    public void call(Iterator<InternalRow> it) throws IOException {
        if (!it.hasNext()) {
            return;
        }
        final Spliterator<InternalRow> spliterator = Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED);

        final Stream<DnsRecordUpdate> updateStream = StreamSupport.stream(spliterator, false)
                .map(this::decode);
        new DnsPartitionHandler(options).runUpdate(updateStream);
    }


    /**
     * Decode Spark InternalRow to DNS record update POJO
     * @param row Spark InternalRow
     * @return decoded DNS record update
     * @throws IllegalArgumentException in case of value errors
     */
    @SneakyThrows
    public DnsRecordUpdate decode(InternalRow row)  {
        final DnsRecordUpdate update = mapper.readValue(row.getString(schema.fieldIndex(UPDATE_COLUMN)), typeRef);
        log.debug("Decoded streaming DNS update: {}", update);
        return update;
    }
}
