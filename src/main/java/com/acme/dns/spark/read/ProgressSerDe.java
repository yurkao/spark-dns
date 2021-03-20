package com.acme.dns.spark.read;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.CharStreams;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * since DNS does not have polling mechanism and offsets are not presenting end of read data, but start of reading data
 * we have to commit offsets of data been read and do not rely on Spark commits
 */
@Slf4j
public class ProgressSerDe {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Comparator<FileStatus> MOST_RECENT_BATCH_FIRST = new BatchIdComparator().reversed();
    private static final Predicate<FileStatus> COMMIT_FILE_FILTER = new BatchIdCommitFilter();

    private final FileSystem fs;
    private final Path metadataPath;
    private final int maxKeptCommits;
    // commit file name is batchId
    // keep most recent batchID in head of queue
    private final PriorityQueue<FileStatus> commits = new PriorityQueue<>(MOST_RECENT_BATCH_FIRST);

    @SneakyThrows
    public ProgressSerDe(FileSystem fs, Path metadataPath, int maxKeptCommits) {
        this.fs = fs;
        this.metadataPath = metadataPath;
        this.maxKeptCommits = maxKeptCommits;
        fs.mkdirs(metadataPath);
        final FileStatus[] fileStatuses = fs.listStatus(metadataPath);
        commits.addAll(Arrays.stream(fileStatuses).filter(COMMIT_FILE_FILTER).collect(Collectors.toList()));
    }

    public void loadSavedProgress(Map<DnsZoneParams, ZoneVersion> zoneVersionMap) {
        final int batchId = getcurrentBatchId() - 1;
        if (batchId < 0) {
            // we are on the very first batch
            return;
        }
        final Path progressPath = new Path(metadataPath, String.valueOf(batchId));
        final Map<String, Long> progresses = deserializeProgress(fs, progressPath);
        final Map<String, DnsZoneParams> map = zoneVersionMap.keySet()
                .stream()
                .collect(Collectors.toMap(dnsZoneParams -> dnsZoneParams.getName().toString(), Function.identity()));
        for(Map.Entry<String, Long> savedProgresses: progresses.entrySet()) {
            final String zoneName = savedProgresses.getKey();
            final DnsZoneParams dnsZoneParams = map.get(zoneName);
            if (Objects.isNull(dnsZoneParams)) {
                log.warn("Ignoring zone {}, since it was removed from options between stream restart", zoneName);
                continue;
            }
            final ZoneVersion progress = zoneVersionMap.get(dnsZoneParams);
            final Long savedSerial = savedProgresses.getValue();
            log.info("Restoring DNS zone {} progress to {}", dnsZoneParams, savedSerial);
            progress.setVersion(savedSerial);
        }
    }

    @SneakyThrows
    public void commit(Map<DnsZoneParams, ZoneVersion> zoneVersionMap) {
        final int batchId = getcurrentBatchId();
        final Path progressPath = new Path(metadataPath, String.valueOf(batchId));
        final HashMap<String, Long> progresses = new HashMap<>();
        zoneVersionMap.forEach((s, tenantProgress) -> progresses.put(s.getName().toString(), tenantProgress.value()));

        log.info("Committing tenant progresses to: {}", progressPath);
        serializeProgress(progressPath, progresses);
        final FileStatus status = new FileStatus();
        status.setPath(progressPath);
        commits.add(status);
        cleanup();
    }

    private void serializeProgress(Path progressPath, HashMap<String, Long> progresses) throws IOException {
        final String content = mapper.writeValueAsString(progresses);

        try (final FSDataOutputStream outputStream = fs.create(progressPath, true)) {
            outputStream.write(content.getBytes());
        }
    }

    @SneakyThrows
    private static Map<String, Long> deserializeProgress(FileSystem fs, Path progressPath) {
        if (!fs.exists(progressPath)) {
            return Collections.emptyMap();
        }
        final String content;
        try(final FSDataInputStream inputStream = fs.open(progressPath)) {
            content = CharStreams.toString(new InputStreamReader(
                    inputStream, StandardCharsets.UTF_8));
        }
        final TypeReference<Map<String, Long>> typeRef = new TypeReference<>() {};
        return mapper.readValue(content, typeRef);
    }

    /**
     * Cleanup on commits
     */
    private void cleanup() {
        final Collection<FileStatus> removedCommits = commits.stream()
                .sorted(MOST_RECENT_BATCH_FIRST)
                .skip(maxKeptCommits)
                .map(this::deleteOldCommit)
                .collect(Collectors.toList());
        log.info("Removed {} old commits", removedCommits.size());
        commits.removeAll(removedCommits);
    }

    private FileStatus deleteOldCommit(FileStatus fileStatus) {
        final Path commitPath = fileStatus.getPath();
        log.info("Removing commit file: {}", commitPath);
        try {
            fs.delete(commitPath, true);
        } catch (IOException e) {
            log.warn("Failed to delete commit file {}", commitPath, e);
            // return file status anyway to reduce memory usage
        }
        return fileStatus;
    }

    /**
     * Get current batch ID
     * @return 0 if there're no commits, otherwise last commit +1
     */
    public int getcurrentBatchId() {
        final int batchId;
        if (commits.isEmpty()) {
            batchId = 0;
        } else {
            final FileStatus fileStatus = commits.peek();
            batchId = BatchIdComparator.getBatchId(fileStatus) + 1;
        }
        return batchId;
    }

    /**
     * filter committed files: commit file names should contain digits only
     */
    static class BatchIdCommitFilter implements Predicate<FileStatus> {

        @Override
        public boolean test(FileStatus fileStatus) {
            if (!fileStatus.isFile()) {
                return false;
            }
            final int batchId;
            try {
                batchId = BatchIdComparator.getBatchId(fileStatus);
            } catch (NumberFormatException notBatchIdCommit) {
                return false;
            }

            return batchId >= 0;
        }
    }

    static class BatchIdComparator implements Comparator<FileStatus> {

        @Override
        public int compare(FileStatus o1, FileStatus o2) {
            final long b1 = getBatchId(o1);
            final long b2 = getBatchId(o2);
            return Long.compare(b1, b2);
        }

        public static int getBatchId(FileStatus o1) {
            return Integer.parseInt(o1.getPath().getName());
        }
    }
}
