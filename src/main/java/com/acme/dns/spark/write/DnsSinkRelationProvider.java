package com.acme.dns.spark.write;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.RelationProvider;
import scala.collection.immutable.Map;

import static com.acme.dns.spark.write.DnsSinkRelation.DATA_SOURCE_NAME;


public class DnsSinkRelationProvider implements
        CreatableRelationProvider, // batch write via Dataset::write API
        RelationProvider, // batch write via SQL INSERT INTO
        DataSourceRegister {

    @Override // batch write via Dataset::write API
    public BaseRelation createRelation(SQLContext sqlContext, SaveMode mode, Map<String, String> parameters, Dataset<Row> data) {
        final DnsSinkOptions sinkOptions = new DnsSinkOptions(parameters);
        final DnsPartitionHandler partitionHandler = new DnsPartitionHandler(sinkOptions);
        data.foreachPartition(partitionHandler);
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
}
