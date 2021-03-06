package com.acme.dns.spark;

import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.streaming.Offset;
import org.apache.spark.sql.execution.streaming.Source;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.collection.Seq;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

@ToString(onlyExplicitlyIncluded=true)
@Slf4j
public class DnsStreamingSource implements Source {

    private final SQLContext sqlContext;
    private final Map<DnsZoneParams, ZoneVersion> zoneVersionMap;
    private final GlobalDnsParams globalDnsParams;
    private final ProgressSerDe offsetManager;
    private int batchId;

    public DnsStreamingSource(SQLContext sqlContext, GlobalDnsParams globalDnsParams, Map<DnsZoneParams, ZoneVersion> zoneVersionMap, ProgressSerDe offsetManager) {
        this.sqlContext = sqlContext;
        this.globalDnsParams = globalDnsParams;
        this.zoneVersionMap = zoneVersionMap;
        this.offsetManager = offsetManager;
        batchId = offsetManager.getcurrentBatchId();
    }

    @Override
    public StructType schema() {
        return DnsRecordToRowConverter.SCHEMA;
    }

    @Override
    public Option<Offset> getOffset() {
        final DnsOffset dnsOffset = createDnsOffset();
        return Option.apply(dnsOffset);
    }

    private DnsOffset createDnsOffset() {
        final Map<String, ZoneOffset> zoneOffsetMap = new HashMap<>();
        for(Map.Entry<DnsZoneParams, ZoneVersion> entry: zoneVersionMap.entrySet()) {
            final DnsZoneParams zoneParams = entry.getKey();
            final Long zoneSerial = entry.getValue().value();

            zoneOffsetMap.put(zoneParams.getName().toString(), new ZoneOffset(zoneSerial));
        }
        return new DnsOffset(zoneOffsetMap);
    }

    @SneakyThrows
    @Override
    public Dataset<Row> getBatch(Option<Offset> start, Offset end) {
        log.info("--------- getBatch #{} ------------", batchId);
        log.info("start={}", start);
        final DnsOffset startOffset = loadOffset(start);
        startOffset.log("start");
        final DnsOffset endOffset = DnsOffset.convert(end);
        endOffset.log("end");
        log.info("end={}", end);

        final Map<String, DnsZoneParams> zoneNameParamsMap = zoneVersionMap.keySet().stream()
                .collect(Collectors.toMap(dnsZoneParams -> dnsZoneParams.getName().toString(), Function.identity()));
        final Map<DnsZoneParams, ZoneVersion> batchParams = new HashMap<>();
        endOffset.getZoneOffsetMap().forEach((zoneName, value) -> {
            final long serial = value.getSerial();
            final DnsZoneParams mappedZoneParams = zoneNameParamsMap.get(zoneName);
            if (Objects.isNull(mappedZoneParams)) {
                log.warn("Ignoring zone {}, since it was removed from options between stream restart", zoneName);
                return;
            }
            final DnsZoneParams dnsZoneParams = mappedZoneParams.copy(serial);
            final ZoneVersion zoneVersion = zoneVersionMap.get(dnsZoneParams);
            log.info("Offset for zone {} (current version {}): {}", zoneName, zoneVersion, serial);
            batchParams.put(dnsZoneParams, zoneVersion);
        });
        final BaseRelation relation = new DnsSourceRelation(sqlContext, batchParams, globalDnsParams);

        final Seq<AttributeReference> attributeReferenceSeq = relation.schema().toAttributes();
        final LogicalRelation plan = new LogicalRelation(relation, attributeReferenceSeq, Option.empty(), true);

        final SparkSession spark = sqlContext.sparkSession();
        final QueryExecution queryExecution = spark.sessionState().executePlan(plan);
        final StructType schema = queryExecution.analyzed().schema();

        final ExpressionEncoder<Row> rowExpressionEncoder = RowEncoder.apply(schema);
        log.info("--------- getBatch #{} done ------------", batchId);
        batchId++;
        return new Dataset<>(spark, plan, rowExpressionEncoder);
    }

    /**
     * Deserialize offset, <code>offset</code> parameter could be one of following
     * <ol>
     *     <li>Option.empty() - very first <code>getBatch</code> call</li>
     *     <li>Option.of(SerializedOffset) - serialized offset (when stream resumed after being terminated)</li>
     *     <li>Option.of(DnsOffset) - on running stream call</li>
     * </ol>
     * @param offset offset to deserialize
     * @return concrete implementation of streaming Offset
     * @throws IOException in case of deserialize error
     */
    private DnsOffset loadOffset(Option<Offset> offset) throws IOException {
        final DnsOffset loadedOffset;
        if (offset.nonEmpty()) {
            // non-first micro-batch
            log.debug("Converting offset of non-first micro-batch");
            loadedOffset = DnsOffset.convert(offset.get());
        } else {
            log.debug("Creating initial offsets on very first micro-batch");
            // initial micro-batch
            // Advance tenant offsets (the until field to be the current time)
            loadedOffset = createDnsOffset();
        }
        return loadedOffset;
    }

    @SneakyThrows
    @Override
    public void commit(Offset end) {
        log.info("--------- commit #{} ------------", batchId);
        final DnsOffset offset = DnsOffset.convert(end);
        offset.log("committing");
        final Map<String, DnsZoneParams> zoneNameParamsMap = zoneVersionMap.keySet().stream().collect(Collectors.toMap(dnsZoneParams -> dnsZoneParams.getName().toString(), Function.identity()));
        offset.getZoneOffsetMap().forEach((zoneName, value) -> {
            final long serial = value.getSerial();
            final DnsZoneParams dnsZoneParams = zoneNameParamsMap.get(zoneName).copy(serial);
            if (Objects.isNull(dnsZoneParams)) {
                log.warn("Ignoring zone {}, since it was removed from options between stream restart", zoneName);
                return;
            }
            final ZoneVersion zoneVersion = zoneVersionMap.get(dnsZoneParams);
            zoneVersion.setVersion(serial);
            log.info("Updated zone {} serial {} -> {}", zoneName, serial, zoneVersion.value());
        });

        // since DNS does not have polling mechanism and offsets are not presenting end of read data,
        // but start of reading data we have to commit offsets of data been read and do not rely on Spark commits

        offsetManager.commit(zoneVersionMap);
        log.info("--------- commit #{} end ------------", batchId);
    }

    @Override
    public void stop() {
        offsetManager.commit(zoneVersionMap);
        zoneVersionMap.values().forEach(ZoneVersion::reset);
    }

    @Override
    public org.apache.spark.sql.connector.read.streaming.Offset initialOffset() {
        return createDnsOffset();
    }

    @SneakyThrows
    @Override
    public org.apache.spark.sql.connector.read.streaming.Offset deserializeOffset(String json) {
        return DnsOffset.deserializeDnsOffset(json);
    }

}
