package com.acme.dns.spark.write;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.streaming.Sink;
import org.apache.spark.sql.types.StructType;

/**
 * Similar to DataStreamWriter::foreachBatch
 */
@Slf4j
@RequiredArgsConstructor
public class DnsStreamingBatchHandler implements Sink {
    private final DnsSinkOptions options;
    @Override
    public void addBatch(long batchId, Dataset<Row> data) {
        log.info("Adding streaming batch {}", batchId);

        // this is required tor streaming
        // otherwise we will get exception that we should use writeStream...start()
        final QueryExecution queryExecution = data.queryExecution();
        final StructType schema = queryExecution.analyzed().schema();

        final DnsStreamingSink sink = new DnsStreamingSink(schema, options);
        queryExecution.toRdd().toJavaRDD()
                .foreachPartition(sink);
    }
}
