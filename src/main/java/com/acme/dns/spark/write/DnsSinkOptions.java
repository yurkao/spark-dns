package com.acme.dns.spark.write;


import com.acme.dns.spark.common.DnsOptions;

import java.io.Serializable;

/**
 * Options wrapper for the Spark DNS sink (Spark 3.5.x), reusing shared option parsing.
 */
public class DnsSinkOptions extends DnsOptions implements Serializable {
    public DnsSinkOptions(scala.collection.immutable.Map<String, String> parameters) {
        super(parameters);
    }
}
