package com.acme.dns;

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;

@Slf4j
class DnsSourceRelationProviderTest {
    static SparkSession spark;

    static final Map<String, String> options = ImmutableMap.of(
            "server", "10.0.0.1",
            "port", "53",
            "zones", "example.acme.,another.zone",
            "organization", "Acme Inc."
    );

    @BeforeAll
    static void init() {
        spark = SparkSession.builder().master("local").getOrCreate();
    }

    @AfterAll
    static void cleanup() {
        spark.close();
    }

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
        spark.sql("drop table if exists my_table");
    }

    @Test
    void sparkBatchRead() {
        assertThatCode(() -> {
            final Dataset<Row> df = spark.read().format("dns").options(options).load().persist();
            log.info("Schema: {}", df.schema().treeString());
            log.info("Rows: {}", df.count());
            df.show(false);
        })
                .as("Reading data via Spark batch read should succeed")
                .doesNotThrowAnyException();
    }

    @Test
    void sqlBatchRead() {
        assertThatCode(() -> {
            spark.sql( "CREATE TABLE my_table USING dns " +
                    "OPTIONS (server='10.0.0.1', port=53, zones='example.acme,another.zone', organization='Acme Inc.')");
            spark.sql("DESC TABLE my_table").show();
            spark.sql("SELECT * FROM my_table").show(false);
        })
                .as("Reading data via Spark SQL should succeed")
                .doesNotThrowAnyException();
    }
}