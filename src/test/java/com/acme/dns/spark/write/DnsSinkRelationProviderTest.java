package com.acme.dns.spark.write;

import com.acme.dns.dao.DnsAction;
import com.acme.dns.dao.DnsRecordUpdate;
import com.acme.dns.spark.BindContainerFactory;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.xbill.DNS.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static com.acme.dns.spark.BindContainerFactory.deleteBindJournal;
import static org.assertj.core.api.Assertions.assertThatCode;

@Slf4j
class DnsSinkRelationProviderTest {
    static SparkSession spark  = SparkSession.builder().master("local").getOrCreate();
    static final String GENERATED_DATA_VIEW_NAME = "data";
    static final String OUTPUT_TABLE_NAME = "output";
    static final BindContainerFactory CONTAINER_FACTORY = new BindContainerFactory();

    static FileSystem fs;

    @Container
    GenericContainer<?> container;

    // DNS tst resolver to validate updated records
    SimpleResolver resolver;
    int xfrPort;
    String xfrHost;
    String checkpoint;
    String dataPath;


    @BeforeAll
    static void init() throws IOException {
        fs = FileSystem.newInstance(spark.sparkContext().hadoopConfiguration());
    }

    @AfterAll
    static void cleanup() throws IOException {
        fs.close();
    }

    @BeforeEach
    void setUp() throws IOException, URISyntaxException {
        deleteBindJournal();
        container = CONTAINER_FACTORY.create();
        xfrPort = container.getMappedPort(53);
        xfrHost = container.getHost();
        resolver = new SimpleResolver(xfrHost);

        resolver.setTimeout(Duration.of(10, ChronoUnit.SECONDS));
        resolver.setTCP(true);
        resolver.setPort(xfrPort);
        if(log.isDebugEnabled()) {
            Options.set("verbose");
        }
        dataPath = "dns-updates-" + UUID.randomUUID();
        checkpoint = "checkpoints-" + UUID.randomUUID();
    }

    @AfterEach
    void tearDown() throws IOException, URISyntaxException {
        CONTAINER_FACTORY.stop(container);
        spark.catalog().dropTempView(GENERATED_DATA_VIEW_NAME);
        spark.sql("DROP TABLE IF EXISTS " + OUTPUT_TABLE_NAME);
        deleteDir(dataPath);
        deleteDir(checkpoint);
        deleteBindJournal();
    }

    private void deleteDir(final String location) throws IOException {
        final Path path = new Path(location);
        if (!fs.exists(path)) {
            return;
        }
        fs.delete(path, true);
    }

    @Test
    void write() throws TextParseException {
        final Dataset<Row> data = createData();

        assertThatCode(() ->
            data.write().format("dns_update")
                    .option("server", xfrHost)
                    .option("port", xfrPort)
                    .option("timeout", 5)
                    .save()
            )
                .as("DNS update should succeed")
                .doesNotThrowAnyException();
        validateUpdates(data);
    }

    @Test
    void writeStreamForeachBatch() throws TextParseException {
        final Dataset<Row> data = createData();
        data.write().parquet(dataPath);
        assertThatCode(() -> {
            final StreamingQuery query = spark.readStream().format("parquet").schema(data.schema()).load(dataPath)
                    .writeStream()
                    .option("checkpointLocation", checkpoint)
                    .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batchDf, batchId) -> batchDf.write()
                            .format("dns_update")
                            .option("server", xfrHost)
                            .option("port", xfrPort)
                            .option("timeout", 5)
                            .save())
                    .start();
            query.awaitTermination(Duration.of(10, ChronoUnit.SECONDS).toMillis());
            if (query.isActive()) {
                query.stop();
            }

        }).doesNotThrowAnyException();
        validateUpdates(data);
    }

    @Test
    void writeStream() throws TextParseException {
        final Dataset<Row> data = createData();
        data.write().parquet(dataPath);
        final Dataset<Row> updates = spark.readStream().format("parquet").schema(data.schema()).load(dataPath);
        final Column jsonStruct = functions.struct(Arrays.stream(data.columns()).map(Column::new).toArray(Column[]::new));

        assertThatCode(() -> {
            final StreamingQuery query = updates
                    .withColumn("update", functions.to_json(jsonStruct))
                    .select("update")
                    .writeStream().format("dns_update")
                    .option("server", xfrHost)
                    .option("port", xfrPort)
                    .option("timeout", 5)
                    .option("checkpointLocation", checkpoint)
                    .start();
                query.awaitTermination(Duration.of(10, ChronoUnit.SECONDS).toMillis());
                if (query.isActive()) {
                    query.stop();
                }

        }).doesNotThrowAnyException();
        validateUpdates(data);

    }

    @Test
    void sqlInsert() throws AnalysisException, TextParseException {
        final Dataset<Row> data = createData();

        data.createTempView(GENERATED_DATA_VIEW_NAME);
        final String createExpr = "CREATE TABLE " + OUTPUT_TABLE_NAME + " USING dns_update " +
                "OPTIONS (server='" + xfrHost + "', port=" + xfrPort + ", timeout=10)";
        log.info("Create SQL expression: {}", createExpr);
        final String insertExpr = "INSERT INTO " + OUTPUT_TABLE_NAME + " TABLE " + GENERATED_DATA_VIEW_NAME;
        log.info("INSERT SQL expression: {}", insertExpr);
        assertThatCode(() -> {
            spark.sql(createExpr);
            spark.sql(insertExpr);
        })
                .as("SQL INSERT INTO should succeed")
                .doesNotThrowAnyException();
        validateUpdates(data);
    }

    private void validateUpdates(Dataset<Row> data) throws TextParseException {
        final List<DnsRecordUpdate> updates = data.as(Encoders.bean(DnsRecordUpdate.class)).collectAsList();
        for(DnsRecordUpdate update: updates) {
            final String fqdn = update.getFqdn();
            final Lookup lookup = new Lookup(fqdn, Type.A);
            lookup.setResolver(resolver);
            final Record[] records = lookup.run();
            if (DnsAction.IXFR_DELETE.equals(update.getAction())) {
                Preconditions.checkArgument(Objects.isNull(records) || records.length == 0,
                        "Deleted FQDN should NOT be resolved: " + fqdn);
            } else {
                Preconditions.checkArgument(Objects.nonNull(records) && records.length == 1,
                        "New FQDN should be resolved: " + fqdn);
            }
        }
    }

    private Dataset<Row> createData() {
        final Dataset<Row> data = spark.range(10)
                .withColumn("action", functions.when(functions.col("id").lt(5), functions.lit("IXFR_ADD")).otherwise( functions.lit("IXFR_DELETE")))
                .withColumn("fqdn", functions.concat(functions.lit("host"), functions.col("id"), functions.lit(".example.acme")))
                .withColumn("ip", functions.concat(functions.lit("127.0.0."), functions.col("id").mod(256)))
                .withColumn("timestamp", functions.current_timestamp())
                .withColumn("ttl", functions.col("id").plus(1).cast("integer"))
                .drop("id");
        data.printSchema();
        return data;
    }

}