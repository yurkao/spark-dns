package com.acme.dns.spark.read;

import com.acme.dns.spark.BindContainerFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.*;

@Slf4j
class DnsSourceRelationProviderTest {
    static final BindContainerFactory CONTAINER_FACTORY = new BindContainerFactory();
    static SparkSession spark;
    static FileSystem fs;
    String checkpoint;
    String outputPath;

    @Container
    GenericContainer<?> container;

    Map<String, String> options;
    int xfrPort;
    String xfrHost;

    @SneakyThrows
    @BeforeAll
    static void init() {
        spark = SparkSession.builder().master("local").getOrCreate();
        fs = FileSystem.newInstance(spark.sparkContext().hadoopConfiguration());
    }

    @AfterAll
    static void cleanup() {
        spark.close();
    }

    @SneakyThrows
    @BeforeEach
    void setUp() {
        container = CONTAINER_FACTORY.create();
        xfrPort = container.getMappedPort(53);
        xfrHost = container.getHost();

        checkpoint = "./checkpoint-" + UUID.randomUUID();
        outputPath = "./output-" + UUID.randomUUID();
        options = new HashMap<>();
        options.put("server", xfrHost);
        options.put("port", String.valueOf(xfrPort));
        options.put("zones", "example.acme.,another.zone");
        options.put("organization", "Acme Inc.");
        options.put("xfr", "ixfr");
    }

    @AfterEach
    void tearDown() throws IOException {
        spark.sql("drop table if exists my_table");
        deleteDir(checkpoint);
        deleteDir(outputPath);
        CONTAINER_FACTORY.stop(container);
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
    void sparkBatchReadTimeoutNoIgnoreFailures() {
        options.put("ignore-failures", "false");
        options.put("timeout", "0");
        assertThatThrownBy(() -> {
            final Dataset<Row> df = spark.read().format("dns").options(options).load().persist();
            log.info("Schema: {}", df.schema().treeString());
            log.info("Rows: {}", df.count());
            df.show(false);
        })
                .as("On timeout, Spark should fail")
                .isInstanceOf(SparkException.class);
    }

    @Test
    void sparkBatchReadTimeoutIgnoreFailures() {
        options.put("ignore-failures", "true");
        options.put("timeout", "0");
        assertThatCode(() -> {
            final Dataset<Row> df = spark.read().format("dns").options(options).load().persist();
            log.info("Schema: {}", df.schema().treeString());
            log.info("Rows: {}", df.count());
            df.show(false);
        })
                .as("On timeout and ignore-failures, Spark must not fail (empty dataset should be returned)")
                .doesNotThrowAnyException();
    }
    @Test
    void sqlBatchRead() {
        assertThatCode(() -> {
            spark.sql( "CREATE TABLE my_table USING dns " +
                    "OPTIONS (server='" + xfrHost + "', port=" + xfrPort + ", zones='example.acme,another.zone', organization='Acme Inc.')");
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
    @Timeout(value = 30)
    void sparkBatchReadIgnoreFailures() {
        options.put("port", "22");
        options.put("ignore-failures", "true");
        options.put("timeout", "1");

        assertThatCode(() -> spark.read().format("dns").options(options).load().show(false))
                .as("XFR failures should be suppressed")
                .doesNotThrowAnyException();
    }

    @Test
    @Timeout(value = 30)
    void sparkBatchReadNoIgnoreFailures() {
        options.put("port", "22");
        options.put("timeout", "1");

        assertThatThrownBy(() -> spark.read().format("dns").options(options).load().show(false))
                .as("Reading should fail if ignore failures is disabled")
                .isInstanceOf(SparkException.class);
    }

    @Test
    @Timeout(value = 30)
    void sqlBatchReadIgnoreFailures() {
        options.put("port", "22");
        options.put("timeout", "1");
        options.put("ignore-failures", "true");

        assertThatCode(() -> {
            spark.sql( "CREATE TABLE my_table USING dns " +
                    "OPTIONS (server='"+ xfrHost +"', " +
                    "port=1, zones='example.acme', timeout=2, organization='Acme Inc.', 'ignore-failures'='true')");
            spark.sql("SELECT * FROM my_table").show(false);
        })
                .as("XFR failures should be suppressed")
                .doesNotThrowAnyException();
    }

    @Test
    void sqlBatchReadNoIgnoreFailures() {

        assertThatThrownBy(() -> {
            spark.sql( "CREATE TABLE my_table USING dns " +
                    "OPTIONS (server='"+ xfrHost +"', " +
                    "port=1, zones='example.acme', timeout=2, organization='Acme Inc.', 'ignore-failures'='false')");
            spark.sql("SELECT * FROM my_table").show(false);
        })
                .as("SQL Reading should fail if ignore failures is disabled")
                .isInstanceOf(SparkException.class);
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