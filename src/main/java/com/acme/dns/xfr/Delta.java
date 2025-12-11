package com.acme.dns.xfr;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.xbill.DNS.Record;

import java.util.ArrayList;
import java.util.List;


/**
 * Container for a single IXFR delta segment, tracking start/end serials
 * and the records that were added or removed between those versions.
 */
@NoArgsConstructor
@Data
public class Delta {

    /**
     * The starting serial number of this delta.
     */
    private long start;

    /**
     * The ending serial number of this delta.
     */
    private long end;

    /**
     * A list of records added between the start and end versions.
     */
    private final List<Record> adds = new ArrayList<>();

    /**
     * A list of records deleted between the start and end versions.
     */
    private final List<Record> deletes = new ArrayList<>();

}
