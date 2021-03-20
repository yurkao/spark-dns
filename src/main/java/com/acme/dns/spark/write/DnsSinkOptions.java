package com.acme.dns.spark.write;


import com.acme.dns.spark.common.DnsOptions;

import java.io.Serializable;

public class DnsSinkOptions extends DnsOptions implements Serializable {
    public DnsSinkOptions(scala.collection.immutable.Map<String, String> parameters) {
        super(parameters);
    }
}
