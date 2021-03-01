# Introduction
Spark data source for retrieving DNS `A` type records from DNS server.
The spark DNS data source uses zone transfers to retrieve data from DNS server. 
It tries to use `IXFR` for every zone transfer though some DNS server implementation may return `AXFR` response.

The spark DNS data source may operate on multiple DNS zones in single data frame.
Due to nature of DNS zone transfer, data retrieval for single zone transfer cannot be done in parallel, 
though data from multiple zones is retrieved in parallel (each DNS zone is handled in different Spark partition of RDD)

# Rationale
1. Learning Spark internals
2. integrating Spark with 3rd party data sources
3. Just for fun

## Data source options
| Option name | Description | default value | Required | 
| ----------- | ----------- | ----------- | ----------- |
| server | DNS server address (IP or fqdn) | N/A | Y |
| port | DNS server TCP port for zone transfers | 53 | N |
| organization | Name of organization the DNS server relates to (free text) | N/A | Y |
| serial | Initial DNS zone serial to start zone transfer with | 0 | N |
| zones | Comma separated list of DNS forward zones | N/A | Y |

## Schema
```
root
 |-- action: string (nullable = true)
 |-- fqdn: string (nullable = true)
 |-- ip: string (nullable = true)
 |-- timestamp: timestamp (nullable = false)
 |-- organization: string (nullable = false)
 |-- zone: string (nullable = false)
```
1. action: one of following
    - `AXFR` - if DNS record was received from AXFR DNS zone transfer
    - `IXFR_ADD` - if DNS record was received from IXFR DNS zone transfer and it's a new record added to the DNS zone via DDNS
    - `IXFR_DELETE` - if DNS record was received from IXFR DNS zone transfer and the record was removed from DNS zone via DDNS
2. fqdn: : FQDN of DNS record
3. ip: IP of DNS record
4. timestamp: timestamp of DNS zone transfer
5. organization: organization name provided via data source options
6. zone: DNS zone name the DNS record relates to

## Building
JDK 11 is required for building
```
./gradlew clean jar
```

## Usage examples
### Submitting application
```
spark-submit --jars spark-dns-1.0.0.jar
pyspark --jars spark-dns-1.0.0.jar
```
### Spark API (PySpark)
```
>>> options = dict(server=10.0.0.1",
               port="53",
               zones="example.acme.,another.zone",
               organization="Acme Inc.",
               serial=1234567890)
>>> spark.read.format("dns").options(**options).load().show(truncate=False)
```
### Spark SQL
```
>>> spark.sql("CREATE TABLE my_table USING dns OPTIONS (server='10.0.0.1', port=53, zones='example.acme,another.zone', serial=1234567890, ,organization='Acme Inc.')")
>>> spark.sql("SELECT * FROM my_table").show(truncate=False)
```
## Output example
```
+------+--------------------------+-------------+-----------------------+------------+-------------+
|action|fqdn                      |ip           |timestamp              |organization|zone         |
+------+--------------------------+-------------+-----------------------+------------+-------------+
|AXFR  |dns-server.example.acme.  |192.168.3.3  |2021-03-01 23:28:17.499|Acme Inc.   |example.acme.|
|AXFR  |foo.example.acme.         |192.168.2.10 |2021-03-01 23:28:17.499|Acme Inc.   |example.acme.|
|AXFR  |newhost1.example.acme.    |192.168.2.17 |2021-03-01 23:28:17.499|Acme Inc.   |example.acme.|
|AXFR  |newhost2.example.acme.    |192.168.2.33 |2021-03-01 23:28:17.499|Acme Inc.   |example.acme.|
|AXFR  |newhost3.example.acme.    |192.168.2.34 |2021-03-01 23:28:17.499|Acme Inc.   |example.acme.|
|AXFR  |newhost4.example.acme.    |192.168.2.35 |2021-03-01 23:28:17.499|Acme Inc.   |example.acme.|
|AXFR  |newhost5.example.acme.    |192.168.2.38 |2021-03-01 23:28:17.499|Acme Inc.   |example.acme.|
|AXFR  |newhost6.example.acme.    |192.168.2.178|2021-03-01 23:28:17.499|Acme Inc.   |example.acme.|
|AXFR  |workstation1.example.acme.|192.168.3.2  |2021-03-01 23:28:17.499|Acme Inc.   |example.acme.|
|AXFR  |workstation2.example.acme.|192.168.5.2  |2021-03-01 23:28:17.499|Acme Inc.   |example.acme.|
|AXFR  |dns-server.another.zone.  |10.0.0.71    |2021-03-01 23:28:18.093|Acme Inc.   |another.zone |
|AXFR  |workstation1.another.zone.|10.0.0.12    |2021-03-01 23:28:18.093|Acme Inc.   |another.zone |
|AXFR  |workstation2.another.zone.|10.0.0.13    |2021-03-01 23:28:18.093|Acme Inc.   |another.zone |
|AXFR  |workstation3.another.zone.|10.0.0.14    |2021-03-01 23:28:18.093|Acme Inc.   |another.zone |
|AXFR  |workstation4.another.zone.|10.0.0.15    |2021-03-01 23:28:18.093|Acme Inc.   |another.zone |
+------+--------------------------+-------------+-----------------------+------------+-------------+
```
## Features and limitations
### Limitations
1. Providing multiple DNS servers in options for same the same dataset/table is currently not supported

### Currently implemented features
1. Spark batch read
2. Retrieving DNS `A` records from multiple DNS zone (though from single DNS server)
3. New DNS SOA serial of DNS zone is available in Accumulator via Spark UI (refer to relevant stage)

### Upcoming features
1. Zone transfer timeout
2. Handling temporary failures during zone transfer (similar to `failOnDataLoss` in Spark+Kafka)
3. Transaction signatures support for DNS zone transfers (aka TSIGs)
4. Specifying explicit zone transfer type (AXFR/IXFR) to use when retrieving data from DNS server
4. Spark Structured Streaming read support

# Tested on
| Spark version | JDK | DNS servers | 
| ----------- | ----------- | ----------- |
| Official 3.0.1 (2.12)  | AdoptedJdk 11 | Bind9, Windows DNS |
| Official 3.0.1 (2.12) | AdoptedJdk 11 | Bind9, Windows DNS | 
| Spark 2.4 (CDH 6.3.x) (2.11) | AdoptedJdk 11 | Bind9, Windows DNS | 
| Spark 2.4 (CDH 6.3.x) (2.11) | AdoptedJdk 11 | Bind9, Windows DNS |

# Links
1. https://www.debian.org/doc/manuals/network-administrator/ch-bind.html DNS server setup 
2. https://wiki.debian.org/DDNS Setup DNS server with dynamic updates from DHCP

# Special thanks
Special thanks to Jacek Laskowski (https://github.com/jaceklaskowski) for teaching me Spark internals
 