package com.acme.dns.spark.write;

import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.streaming.Sink;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.streaming.OutputMode;
import scala.collection.Seq;
import scala.collection.immutable.Map;

import java.util.Arrays;

import static com.acme.dns.spark.write.DnsPartitionHandler.UPDATE_COLUMN;
import static com.acme.dns.spark.write.DnsSinkRelation.DATA_SOURCE_NAME;

public class DnsSinkRelationProvider implements
        CreatableRelationProvider, // batch write via Dataset::write API
        RelationProvider, // batch write via SQL INSERT INTO
        StreamSinkProvider, // streaming write
        DataSourceRegister {

    @Override // batch write via Dataset::write API
    public BaseRelation createRelation(SQLContext sqlContext, SaveMode mode, Map<String, String> parameters, Dataset<Row> data) {
        final DnsSinkOptions sinkOptions = new DnsSinkOptions(parameters);
        final DnsBatchPartitionHandler partitionHandler = new DnsBatchPartitionHandler(sinkOptions);
        final Column jsonStruct = functions.struct(Arrays.stream(data.columns()).map(Column::new).toArray(Column[]::new));
        // encode as JSON for ease of Row decoding: fasterxml::ObjectMapper vs Row::get(Row::fieldIndex)
        data.withColumn(UPDATE_COLUMN, functions.to_json(jsonStruct)).select(UPDATE_COLUMN).foreachPartition(partitionHandler);
        return new DnsSinkRelation(sqlContext, sinkOptions);
    }

    @Override
    public String shortName() {
        return DATA_SOURCE_NAME;
    }

    @Override // SQL INSERT INTO
    public BaseRelation createRelation(SQLContext sqlContext, Map<String, String> parameters) {
        final DnsSinkOptions sinkOptions = new DnsSinkOptions(parameters);
        return new DnsSinkRelation(sqlContext, sinkOptions);
    }

    @Override // Structured Streaming writeStream
    public Sink createSink(SQLContext sqlContext, Map<String, String> parameters, Seq<String> partitionColumns, OutputMode outputMode) {
        final DnsSinkOptions sinkOptions = new DnsSinkOptions(parameters);
        return new DnsStreamingBatchHandler(sinkOptions);
    }
}
