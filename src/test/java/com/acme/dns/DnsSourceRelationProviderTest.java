package com.acme.dns;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

@Slf4j
class DnsSourceRelationProviderTest {
    static SparkSession spark;
    static FileSystem fs;
    String checkpoint;
    String outputPath;

    static final Map<String, String> options = new HashMap<>();

    @BeforeAll
    static void init() throws IOException {
        spark = SparkSession.builder().master("local").getOrCreate();
        fs = FileSystem.newInstance(spark.sparkContext().hadoopConfiguration());
        options.put("server", "10.0.0.1");
        options.put("port", "53");
        options.put("zones", "example.acme.,another.zone");
        options.put("organization", "Acme Inc.");
        options.put("xfr", "ixfr");
    }

    @AfterAll
    static void cleanup() {
        spark.close();
    }

    @BeforeEach
    void setUp() {
        checkpoint = "./checkpoint-" + UUID.randomUUID();
        outputPath = "./output-" + UUID.randomUUID();
    }

    @AfterEach
    void tearDown() throws IOException {
        spark.sql("drop table if exists my_table");
        deleteDir(checkpoint);
        deleteDir(outputPath);
    }

    private void deleteDir(final String location) throws IOException {
        final Path path = new Path(location);
        if (!fs.exists(path)) {
            return;
        }
        fs.delete(path, true);
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

    @Test
    void sparkStreamingRead() {
        final Trigger trigger = Trigger.Once();
        assertThatCode(() -> runStream(trigger))
                .as("Reading data via Spark streaming read should succeed")
                .doesNotThrowAnyException();
        final Dataset<Row> df = spark.read().parquet(outputPath);
        assertThat(df.count())
                .as("Some DNS data should be written to parquet")
                .isNotZero();
    }

    @Test
    void resumeStream() throws StreamingQueryException, TimeoutException {
        final Trigger trigger = Trigger.Once();
        runStream(trigger);
        log.info("");
        log.info("");
        log.info("");
        log.info("Resuming streaming query");
        log.info("");
        log.info("");
        log.info("");
        runStream(trigger);
        final Dataset<Row> parquet = spark.read().parquet(outputPath);
        final Dataset<Row> df = parquet
                .groupBy("action", "fqdn", "ip", "organization", "zone")
                .count();

        df.show(1000, false);
        final List<Long> counts = df.select("count").as(Encoders.LONG()).collectAsList();
        assertThat(new HashSet<>(counts))
                .as("all DNS entries should be produced once since no updates are published to DNS")
                .hasSize(1)
                .contains(1L);

    }

    private void runStream(Trigger trigger) throws StreamingQueryException, TimeoutException {
        final StreamingQuery streamingQuery = spark.readStream().format("dns").options(options).load()
                .writeStream()
                .format("parquet")
                .option("truncate", "false")
                .option("path", outputPath)
                .option("checkpointLocation", checkpoint)
                .trigger(trigger)
                .start();
        streamingQuery.awaitTermination(20000L);
        if (!streamingQuery.isActive()) {
            streamingQuery.stop();
        }
    }
}