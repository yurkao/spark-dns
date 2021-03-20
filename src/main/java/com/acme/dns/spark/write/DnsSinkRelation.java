package com.acme.dns.spark.write;

import com.acme.dns.dao.DnsRecordUpdate;
import com.acme.dns.spark.common.DnsOptions;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.InsertableRelation;
import org.apache.spark.sql.types.StructType;

@Slf4j
@RequiredArgsConstructor
public class DnsSinkRelation extends BaseRelation implements InsertableRelation {
    public static final String DATA_SOURCE_NAME = "dns_update";
    public static final StructType OUTPUT_SCHEMA = Encoders.bean(DnsRecordUpdate.class).schema();
    private final SQLContext sqlContext;
    private final DnsSinkOptions options;

    @Override
    public SQLContext sqlContext() {
        return sqlContext;
    }

    @Override
    public StructType schema() {
        return OUTPUT_SCHEMA;
    }

    @Override // invoked on Spark SQL INSERT INTO
    public void insert(Dataset<Row> data, boolean overwrite) {
        // SQL is just a wrapper for Dataset API
        // overwrite is ignored since we cannot really control it on DNS server
        data.write().format(DATA_SOURCE_NAME)
                .option(DnsOptions.SERVER_OPT, options.getServer().getHostName())
                .option(DnsOptions.PORT_OPT, options.getServer().getPort())
                .option(DnsOptions.XFR_TIMEOUT_OPT, options.getTimeout())
                .save();
    }
}
