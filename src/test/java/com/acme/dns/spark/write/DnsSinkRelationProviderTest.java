package com.acme.dns.spark.write;

import com.acme.dns.dao.DnsAction;
import com.acme.dns.dao.DnsRecordUpdate;
import com.acme.dns.spark.BindContainerFactory;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.xbill.DNS.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;

import static com.acme.dns.spark.BindContainerFactory.deleteBindJournal;
import static org.assertj.core.api.Assertions.assertThatCode;

@Slf4j
class DnsSinkRelationProviderTest {
    static SparkSession spark  = SparkSession.builder().master("local").getOrCreate();
    static final String GENERATED_DATA_VIEW_NAME = "data";
    static final String OUTPUT_TABLE_NAME = "output";
    static final BindContainerFactory CONTAINER_FACTORY = new BindContainerFactory();

    @Container
    GenericContainer<?> container;

    // DNS tst resolver to validate updated records
    SimpleResolver resolver;
    int xfrPort;
    String xfrHost;

    @BeforeEach
    void setUp() throws IOException, URISyntaxException {
        deleteBindJournal();
        container = CONTAINER_FACTORY.create();
        xfrPort = container.getMappedPort(53);
        xfrHost = container.getHost();
        resolver = new SimpleResolver(xfrHost);

        resolver.setTimeout(10);
        resolver.setTCP(true);
        resolver.setPort(xfrPort);
        if(log.isDebugEnabled()) {
            Options.set("verbose");
        }
    }

    @AfterEach
    void tearDown() {
        CONTAINER_FACTORY.stop(container);
        spark.catalog().dropTempView(GENERATED_DATA_VIEW_NAME);
        spark.sql("DROP TABLE IF EXISTS " + OUTPUT_TABLE_NAME);
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