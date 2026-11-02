# Apache Kafka — Exhaustive Reference Card

> **Version coverage:** Kafka 3.x (KRaft mode & ZooKeeper), `confluent-kafka-python` 2.x, `kafka-python` 2.x  
> **Last updated:** 2026-05

---

## Table of Contents

1. [Core Concepts & Architecture](#1-core-concepts--architecture)
2. [Cluster Setup & Configuration](#2-cluster-setup--configuration)
3. [Topics & Partitions](#3-topics--partitions)
4. [Producers](#4-producers)
5. [Consumers & Consumer Groups](#5-consumers--consumer-groups)
6. [Kafka Streams (Java/Scala API)](#6-kafka-streams)
7. [Kafka Connect](#7-kafka-connect)
8. [Schema Registry & Avro](#8-schema-registry--avro)
9. [Security](#9-security)
10. [Monitoring & Observability](#10-monitoring--observability)
11. [Python Integration](#11-python-integration)
12. [Design Patterns](#12-design-patterns)
13. [CLI Quick Reference](#13-cli-quick-reference)
14. [Configuration Reference Tables](#14-configuration-reference-tables)
15. [Best Practices](#15-best-practices)
16. [Common Pitfalls](#16-common-pitfalls)

---

## 1. Core Concepts & Architecture

### Fundamental Building Blocks

| Concept | Description |
|---|---|
| **Broker** | A Kafka server. Stores log segments on disk. Each broker has a unique `broker.id`. |
| **Cluster** | Multiple brokers forming a single logical system. |
| **Topic** | Named log of records. Logical channel. Immutable append-only. |
| **Partition** | Ordered, immutable sequence of records within a topic. Unit of parallelism. |
| **Offset** | Monotonically increasing integer identifying a record within a partition. |
| **Record/Message** | Key + Value + Timestamp + Headers. Serialized byte arrays on the wire. |
| **Producer** | Client that publishes records to topics. |
| **Consumer** | Client that reads records from topics. Pulls from broker. |
| **Consumer Group** | Logical group of consumers sharing a `group.id`. Each partition assigned to exactly one member. |
| **Replication Factor** | How many broker copies of a partition exist. Minimum recommended: 3. |
| **Leader** | The single broker that handles all reads/writes for a partition replica set. |
| **Follower/ISR** | In-Sync Replicas: followers that are caught up to the leader within `replica.lag.time.max.ms`. |
| **Controller** | Broker elected to manage partition leadership and broker membership. |
| **KRaft** | Kafka Raft Metadata mode — replaces ZooKeeper as metadata store (default from Kafka 3.3). |
| **Log Compaction** | Retention policy where only the latest record per key is kept (`cleanup.policy=compact`). |
| **Retention** | Time-based (`retention.ms`) or size-based (`retention.bytes`) record expiry. |

### Architecture Diagram (Text)

```
Producers                  Kafka Cluster                       Consumers
─────────                  ─────────────                       ─────────
App A ──► [Partition 0] ── Broker 1 (Leader P0)  ──────────► Group A (Consumer 1)
App B ──► [Partition 1] ── Broker 2 (Leader P1)  ──────────► Group A (Consumer 2)
App C ──► [Partition 2] ── Broker 3 (Leader P2)  ──────────► Group A (Consumer 3)
                                                  ──────────► Group B (Consumer 4,5,6)

         ZooKeeper / KRaft  ◄─── Controller metadata ───
```

### Record Anatomy

```
┌──────────────────────────────────────────────────────────┐
│  Offset  │ Timestamp │ Key (bytes) │ Value (bytes) │ Headers │
└──────────────────────────────────────────────────────────┘
```

- **Key**: Optional. Used for partition routing (same key → same partition). Null = round-robin.
- **Value**: The payload. Any byte array. Schema typically enforced via Schema Registry.
- **Headers**: Metadata map (e.g., trace IDs, content-type). Not indexed.
- **Timestamp**: Either producer-set (`CreateTime`) or broker-set (`LogAppendTime`).

---

## 2. Cluster Setup & Configuration

### KRaft Mode (Kafka 3.3+ recommended)

```bash
# 1. Generate cluster ID
KAFKA_CLUSTER_ID=$(kafka-storage.sh random-uuid)

# 2. Format storage directory
kafka-storage.sh format \
  --config /etc/kafka/kraft/server.properties \
  --cluster-id $KAFKA_CLUSTER_ID

# 3. Start broker
kafka-server-start.sh /etc/kafka/kraft/server.properties
```

### Essential `server.properties` Settings

```properties
# Broker identity
broker.id=1
node.id=1                                   # KRaft only

# Networking
listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
advertised.listeners=PLAINTEXT://kafka1.example.com:9092
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT

# KRaft
process.roles=broker,controller              # or: broker | controller
controller.quorum.voters=1@kafka1:9093,2@kafka2:9093,3@kafka3:9093
controller.listener.names=CONTROLLER

# Storage
log.dirs=/var/kafka/data
num.partitions=6                             # default partitions for new topics
default.replication.factor=3
min.insync.replicas=2                        # ISR threshold for acks=all writes

# Retention
log.retention.hours=168                      # 7 days
log.retention.bytes=-1                       # unlimited by size
log.segment.bytes=1073741824                 # 1 GB per segment
log.retention.check.interval.ms=300000

# Performance
num.network.threads=8
num.io.threads=16
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600           # 100 MB max message size

# Replication
replica.lag.time.max.ms=30000
auto.leader.rebalance.enable=true
```

### Docker Compose (Quick Dev Setup)

```yaml
version: '3.8'
services:
  kafka:
    image: confluentinc/cp-kafka:7.6.0
    hostname: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qg"
    volumes:
      - kafka-data:/var/lib/kafka/data

  schema-registry:
    image: confluentinc/cp-schema-registry:7.6.0
    depends_on: [kafka]
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:9092

volumes:
  kafka-data:
```

---

## 3. Topics & Partitions

### Topic Configuration Properties

| Property | Default | Description |
|---|---|---|
| `num.partitions` | 1 | Number of partitions |
| `replication.factor` | 1 | Number of replicas |
| `min.insync.replicas` | 1 | Min ISRs required for writes |
| `retention.ms` | 604800000 (7d) | Record retention in ms |
| `retention.bytes` | -1 (unlimited) | Max size per partition |
| `cleanup.policy` | `delete` | `delete`, `compact`, or `delete,compact` |
| `segment.bytes` | 1073741824 | Log segment size |
| `segment.ms` | 604800000 | Force segment roll after ms |
| `max.message.bytes` | 1048588 | Max record batch size |
| `compression.type` | `producer` | `none`, `gzip`, `snappy`, `lz4`, `zstd`, `producer` |
| `message.timestamp.type` | `CreateTime` | `CreateTime` or `LogAppendTime` |
| `delete.retention.ms` | 86400000 | How long tombstones are retained (compact) |
| `min.cleanable.dirty.ratio` | 0.5 | Fraction of log to compact |
| `unclean.leader.election.enable` | false | Allow out-of-sync replica to become leader |

### Partition Count Guidelines

```
Rule of thumb: target_throughput_MB_s / partition_throughput_MB_s
                                                                    
Partition throughput (producer):  ~10 MB/s
Partition throughput (consumer):  ~50 MB/s

Example: 500 MB/s target → 50 partitions minimum (producer-bound)

Max consumers in a group = number of partitions
Too few partitions = throughput bottleneck + limited consumer parallelism
Too many partitions = higher latency for leader failover + more file handles
```

### Replication Factor Rules

| RF | Min Brokers | Tolerates | Use Case |
|---|---|---|---|
| 1 | 1 | 0 failures | Dev/test only |
| 2 | 2 | 1 failure | Moderate |
| 3 | 3 | 1 failure (2 with `min.isr=1`) | Production standard |
| 5 | 5 | 2 failures | Critical data |

---

## 4. Producers

### Producer Configuration — All Key Properties

| Config Key | Default | Description |
|---|---|---|
| `bootstrap.servers` | — | `host:port` list |
| `acks` | `all` | `0` (fire-forget), `1` (leader), `all`/-1 (all ISRs) |
| `retries` | `2147483647` | Max retry attempts |
| `retry.backoff.ms` | `100` | Wait between retries |
| `delivery.timeout.ms` | `120000` | Upper bound for ack |
| `max.in.flight.requests.per.connection` | `5` | In-flight batches per broker |
| `enable.idempotence` | `true` | Exactly-once at producer level |
| `transactional.id` | — | Enables transactions |
| `batch.size` | `16384` | Max batch size in bytes |
| `linger.ms` | `0` | Wait to fill batch before send |
| `buffer.memory` | `33554432` | Total producer buffer (32 MB) |
| `compression.type` | `none` | `gzip`, `snappy`, `lz4`, `zstd` |
| `key.serializer` | — | Serializer class for keys |
| `value.serializer` | — | Serializer class for values |
| `partitioner.class` | default | Custom partitioner |
| `max.block.ms` | `60000` | Max time `send()` blocks |
| `request.timeout.ms` | `30000` | Request response timeout |
| `metadata.max.age.ms` | `300000` | Force metadata refresh |

### Delivery Guarantee Comparison

| Setting | `acks` | `enable.idempotence` | `transactional.id` | Guarantee |
|---|---|---|---|---|
| Fire & Forget | `0` | false | — | At most once |
| At Least Once | `all` | false | — | At least once |
| Idempotent | `all` | true | — | Exactly once (per session) |
| Transactional | `all` | true | Set | Exactly once (cross-partition) |

### Partitioning Strategy

```
Record Key → Partitioner → Partition Number
                                            
Default (Kafka 3+):  Sticky partitioning (batch to same partition until full)
Key-based:           murmur2(key) % numPartitions  — same key always → same partition
Null key:            Round-robin (or sticky)
Custom:              Implement org.apache.kafka.clients.producer.Partitioner
```

---

## 5. Consumers & Consumer Groups

### Consumer Configuration — All Key Properties

| Config Key | Default | Description |
|---|---|---|
| `bootstrap.servers` | — | Broker addresses |
| `group.id` | — | Consumer group identifier |
| `auto.offset.reset` | `latest` | `earliest`, `latest`, `none` |
| `enable.auto.commit` | `true` | Auto-commit offsets |
| `auto.commit.interval.ms` | `5000` | Auto-commit frequency |
| `fetch.min.bytes` | `1` | Min data to fetch |
| `fetch.max.bytes` | `52428800` | Max fetch size (50 MB) |
| `fetch.max.wait.ms` | `500` | Max wait if < min bytes |
| `max.partition.fetch.bytes` | `1048576` | Max per-partition fetch (1 MB) |
| `max.poll.records` | `500` | Max records per `poll()` |
| `max.poll.interval.ms` | `300000` | Max time between polls before rebalance |
| `session.timeout.ms` | `45000` | Heartbeat timeout |
| `heartbeat.interval.ms` | `3000` | Heartbeat frequency |
| `isolation.level` | `read_uncommitted` | `read_uncommitted` or `read_committed` |
| `partition.assignment.strategy` | `RangeAssignor` | Assignor class |
| `key.deserializer` | — | Key deserializer |
| `value.deserializer` | — | Value deserializer |
| `group.instance.id` | — | Static membership ID (avoids rebalance on restart) |

### Offset Management

```
Offset commit strategies:

1. Auto-commit (easy, at-least-once risk):
   enable.auto.commit=true
   auto.commit.interval.ms=5000

2. Manual sync (safe):
   consumer.commitSync()              # blocks until ack
   consumer.commitSync(offsets)       # commit specific offsets

3. Manual async (high throughput):
   consumer.commitAsync()             # non-blocking, callback on complete
   consumer.commitAsync(offsets, callback)

4. Per-message commit (lowest throughput, highest safety):
   for record in consumer.poll():
       process(record)
       consumer.commitSync({TopicPartition: OffsetAndMetadata(offset+1)})
```

### Rebalance Protocols

| Protocol | Trigger | Downtime | Notes |
|---|---|---|---|
| Eager (Stop-the-world) | Member joins/leaves | All partitions revoked | Default |
| Cooperative Incremental | Member joins/leaves | Only migrating partitions revoked | Use `CooperativeStickyAssignor` |
| Static Membership | Restart within `session.timeout.ms` | None | Set `group.instance.id` |

### Partition Assignment Strategies

| Strategy | Algorithm | Use Case |
|---|---|---|
| `RangeAssignor` | Range per topic | Default; can be uneven |
| `RoundRobinAssignor` | Evenly across all | Balanced load |
| `StickyAssignor` | Minimize movements on rebalance | Stateful consumers |
| `CooperativeStickyAssignor` | Incremental rebalance | Zero-downtime rebalance |

---

## 6. Kafka Streams

> Java/Scala DSL for stateful stream processing built into Kafka.

### Core Abstractions

| Class | Description |
|---|---|
| `StreamsBuilder` | Entry point; builds topology |
| `KStream<K,V>` | Unbounded stream of records |
| `KTable<K,V>` | Changelog stream (materialized view, one record per key) |
| `GlobalKTable<K,V>` | Fully replicated KTable on all instances |
| `KGroupedStream` | Grouped stream ready for aggregation |
| `KGroupedTable` | Grouped table ready for aggregation |
| `SessionWindows` | Session-based windowing |
| `TimeWindows` | Tumbling / hopping windows |
| `SlidingWindows` | Sliding windows |
| `Materialized` | State store configuration |
| `Topology` | DAG of processing nodes |

### Key Operations Table

| Operation | KStream | KTable | Description |
|---|---|---|---|
| `map(mapper)` | ✓ | ✓ | Transform key+value |
| `mapValues(mapper)` | ✓ | ✓ | Transform value only |
| `filter(predicate)` | ✓ | ✓ | Keep matching records |
| `flatMap(mapper)` | ✓ | — | 1-to-many transform |
| `flatMapValues(mapper)` | ✓ | — | Expand values |
| `selectKey(mapper)` | ✓ | — | Re-key stream |
| `groupBy(mapper)` | ✓ | ✓ | Group by new key |
| `groupByKey()` | ✓ | — | Group by existing key |
| `reduce(reducer)` | ✓ | ✓ | Running reduction |
| `aggregate(init, agg)` | ✓ | ✓ | Running aggregation |
| `count()` | ✓ | ✓ | Count per key |
| `join(other, joiner)` | ✓ | ✓ | Stream-stream / stream-table join |
| `leftJoin(other, joiner)` | ✓ | ✓ | Left outer join |
| `outerJoin(other, joiner)` | ✓ | — | Full outer join |
| `merge(stream)` | ✓ | — | Merge two streams |
| `branch(predicates)` | ✓ | — | Split into multiple streams |
| `peek(action)` | ✓ | — | Side-effect on each record |
| `to(topic)` | ✓ | — | Write to output topic |
| `toStream()` | — | ✓ | Convert KTable → KStream |
| `suppress(until)` | — | ✓ | Suppress intermediate updates |
| `transformValues(supplier)` | ✓ | — | Stateful value transform |
| `process(supplier)` | ✓ | — | Low-level processor API |

### Windowing Types

```java
// Tumbling Window — fixed, non-overlapping
TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5))

// Hopping Window — fixed size, overlapping
TimeWindows.ofSizeAndGrace(Duration.ofMinutes(5), Duration.ofSeconds(30))
           .advanceBy(Duration.ofMinutes(1))

// Sliding Window — continuous, event-time based
SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5))

// Session Window — inactivity gap based
SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(5))
```

### Streams Config (Java)

```java
Properties props = new Properties();
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-app");
props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
```

---

## 7. Kafka Connect

### Architecture

```
Source System  ─►  Source Connector  ─►  Kafka Topic  ─►  Sink Connector  ─►  Target System
                   (Transforms/SMTs)                       (Transforms/SMTs)
```

### Key Components

| Component | Description |
|---|---|
| **Connector** | Logical job config. Source or Sink. |
| **Task** | Unit of work. Multiple tasks per connector = parallelism. |
| **Worker** | JVM process running Connect runtime. |
| **Standalone mode** | Single worker, single process. Dev only. |
| **Distributed mode** | Multiple workers, fault-tolerant. Production. |
| **SMT** | Single Message Transform — lightweight per-message transformation. |
| **Converter** | Serializes/deserializes data (JSON, Avro, Protobuf). |

### REST API Reference

| Method | Endpoint | Action |
|---|---|---|
| GET | `/connectors` | List all connectors |
| POST | `/connectors` | Create connector |
| GET | `/connectors/{name}` | Get connector info |
| GET | `/connectors/{name}/status` | Get status |
| PUT | `/connectors/{name}/config` | Update config |
| DELETE | `/connectors/{name}` | Delete connector |
| POST | `/connectors/{name}/restart` | Restart connector |
| PUT | `/connectors/{name}/pause` | Pause connector |
| PUT | `/connectors/{name}/resume` | Resume connector |
| GET | `/connectors/{name}/tasks` | List tasks |
| POST | `/connectors/{name}/tasks/{taskId}/restart` | Restart task |

### Common SMTs

| SMT | Purpose |
|---|---|
| `ExtractField` | Extract a nested field as top-level value |
| `InsertField` | Add a static field |
| `ReplaceField` | Include/exclude/rename fields |
| `MaskField` | Mask sensitive fields with null/zeros |
| `HoistField` | Wrap value in a struct |
| `Flatten` | Flatten nested structs |
| `Cast` | Type conversion |
| `TimestampConverter` | Convert timestamp formats |
| `SetSchemaMetadata` | Override schema name/version |
| `Filter` | Drop records matching predicate |
| `Router` | Route to different topics |

---

## 8. Schema Registry & Avro

### Schema Registry REST API

| Method | Endpoint | Action |
|---|---|---|
| GET | `/subjects` | List all subjects |
| GET | `/subjects/{subject}/versions` | List versions |
| GET | `/subjects/{subject}/versions/{version}` | Get schema |
| GET | `/subjects/{subject}/versions/latest` | Get latest schema |
| POST | `/subjects/{subject}/versions` | Register schema |
| POST | `/subjects/{subject}` | Check if schema exists |
| DELETE | `/subjects/{subject}` | Delete subject |
| GET | `/schemas/ids/{id}` | Get schema by ID |
| GET | `/config` | Get global compatibility |
| PUT | `/config` | Update global compatibility |
| PUT | `/config/{subject}` | Override subject compatibility |

### Compatibility Levels

| Level | Meaning |
|---|---|
| `BACKWARD` | New schema can read old data (default) |
| `FORWARD` | Old schema can read new data |
| `FULL` | Both backward and forward |
| `BACKWARD_TRANSITIVE` | Backward against all previous versions |
| `FORWARD_TRANSITIVE` | Forward against all previous versions |
| `FULL_TRANSITIVE` | Both against all versions |
| `NONE` | No compatibility checks |

### Wire Format (Confluent)

```
┌──────────┬──────────────┬───────────────┐
│  0x00    │  Schema ID   │  Avro Payload │
│  1 byte  │  4 bytes BE  │  N bytes      │
└──────────┴──────────────┴───────────────┘
```

---

## 9. Security

### Authentication Methods

| Method | Protocol | Config |
|---|---|---|
| No auth | PLAINTEXT | Default |
| SASL/PLAIN | SASL_PLAINTEXT / SASL_SSL | Username/password |
| SASL/SCRAM-SHA-256 | SASL_SSL | Salted challenge-response |
| SASL/SCRAM-SHA-512 | SASL_SSL | Stronger SCRAM |
| SASL/GSSAPI (Kerberos) | SASL_PLAINTEXT / SASL_SSL | Enterprise/AD |
| SASL/OAUTHBEARER | SASL_SSL | OAuth 2.0 tokens |
| mTLS | SSL | Mutual TLS certificate auth |

### ACL Operations

| Operation | Resource | Description |
|---|---|---|
| `Read` | Topic, Group | Consume from topic / use group |
| `Write` | Topic | Produce to topic |
| `Create` | Topic, Cluster | Create topics |
| `Delete` | Topic | Delete topics |
| `Alter` | Topic | Change topic config |
| `Describe` | Topic, Group, Cluster | View metadata |
| `DescribeConfigs` | Topic, Broker | View configs |
| `AlterConfigs` | Topic, Broker | Modify configs |
| `ClusterAction` | Cluster | Inter-broker ops |
| `All` | Any | All operations |

### SSL Producer/Consumer Config

```properties
security.protocol=SSL
ssl.truststore.location=/ssl/kafka.client.truststore.jks
ssl.truststore.password=truststore-password
ssl.keystore.location=/ssl/kafka.client.keystore.jks
ssl.keystore.password=keystore-password
ssl.key.password=key-password
```

### SASL/PLAIN Config

```properties
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  username="alice" \
  password="alice-secret";
```

---

## 10. Monitoring & Observability

### Critical Metrics to Monitor

| Metric | JMX Bean | Alert Threshold |
|---|---|---|
| **Under-replicated partitions** | `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions` | > 0 |
| **Active controller count** | `kafka.controller:type=KafkaController,name=ActiveControllerCount` | != 1 |
| **Offline partitions** | `kafka.controller:type=KafkaController,name=OfflinePartitionsCount` | > 0 |
| **Producer request rate** | `kafka.server:type=BrokerTopicMetrics,name=ProducerRequestsPerSec` | — |
| **Consumer lag** | `kafka.consumer:type=consumer-fetch-manager-metrics,name=records-lag-max` | > threshold |
| **Request queue size** | `kafka.network:type=RequestChannel,name=RequestQueueSize` | > 50 |
| **Log flush latency** | `kafka.log:type=LogFlushStats,name=LogFlushRateAndTimeMs` | > 1000ms |
| **Network processor idle** | `kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent` | < 30% |
| **Request handler idle** | `kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent` | < 30% |
| **Bytes in/out per sec** | `kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec` | Capacity |

### Consumer Lag Monitoring

```bash
# Built-in CLI
kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --describe --group my-group

# Output columns:
# TOPIC  PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID  HOST
```

### Prometheus JMX Exporter

```yaml
# jmx_exporter_config.yml
lowercaseOutputName: true
rules:
  - pattern: "kafka.server<type=(.+), name=(.+)><>(Count|Value)"
    name: "kafka_server_$1_$2"
  - pattern: "kafka.controller<type=(.+), name=(.+)><>(Count|Value)"
    name: "kafka_controller_$1_$2"
```

---

## 11. Python Integration

### Libraries

| Library | Install | Notes |
|---|---|---|
| `confluent-kafka-python` | `pip install confluent-kafka` | C librdkafka bindings, production recommended |
| `kafka-python` | `pip install kafka-python` | Pure Python, easier API |
| `faust` | `pip install faust-streaming` | Async stream processing (Kafka Streams for Python) |
| `aiokafka` | `pip install aiokafka` | Asyncio-based |

---

### confluent-kafka-python

#### Producer

```python
from confluent_kafka import Producer
import json

# Configuration
conf = {
    'bootstrap.servers': 'kafka1:9092,kafka2:9092',
    'acks': 'all',
    'enable.idempotence': True,
    'max.in.flight.requests.per.connection': 5,
    'retries': 2147483647,
    'compression.type': 'snappy',
    'batch.size': 65536,          # 64 KB
    'linger.ms': 10,
    'buffer.memory': 67108864,    # 64 MB
    'client.id': 'my-producer',
}

producer = Producer(conf)

# Delivery callback
def delivery_report(err, msg):
    if err is not None:
        print(f'Delivery failed: {err}')
    else:
        print(f'Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')

# Produce messages
for i in range(1000):
    key = f'key-{i % 10}'
    value = json.dumps({'id': i, 'data': 'payload'})
    producer.produce(
        topic='my-topic',
        key=key.encode('utf-8'),
        value=value.encode('utf-8'),
        on_delivery=delivery_report,
        headers={'source': 'my-app', 'version': '1.0'},
    )
    producer.poll(0)  # trigger callbacks

producer.flush()  # block until all messages delivered
```

#### Consumer

```python
from confluent_kafka import Consumer, KafkaException, TopicPartition

conf = {
    'bootstrap.servers': 'kafka1:9092',
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,        # manual commit for at-least-once
    'max.poll.interval.ms': 300000,
    'session.timeout.ms': 45000,
    'heartbeat.interval.ms': 3000,
    'fetch.min.bytes': 1,
    'fetch.max.wait.ms': 500,
    'max.partition.fetch.bytes': 1048576,
    'isolation.level': 'read_committed', # if producer uses transactions
}

consumer = Consumer(conf)

# Rebalance callbacks
def on_assign(consumer, partitions):
    print(f'Assigned: {partitions}')

def on_revoke(consumer, partitions):
    consumer.commit(asynchronous=False)   # commit before revoke
    print(f'Revoked: {partitions}')

consumer.subscribe(['my-topic'], on_assign=on_assign, on_revoke=on_revoke)

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())

        # Process message
        key = msg.key().decode('utf-8') if msg.key() else None
        value = json.loads(msg.value().decode('utf-8'))
        print(f'Topic: {msg.topic()}, Partition: {msg.partition()}, '
              f'Offset: {msg.offset()}, Key: {key}, Value: {value}')

        # Manual commit (after processing)
        consumer.commit(asynchronous=False)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()  # triggers final commit + leaves group cleanly
```

#### Transactional Producer

```python
from confluent_kafka import Producer

producer = Producer({
    'bootstrap.servers': 'kafka:9092',
    'transactional.id': 'my-transactional-producer',
    'enable.idempotence': True,
})

producer.init_transactions()

try:
    producer.begin_transaction()

    producer.produce('output-topic', key='k1', value='v1')
    producer.produce('output-topic', key='k2', value='v2')

    # Optionally send consumer offsets atomically
    # producer.send_offsets_to_transaction(offsets, consumer_metadata)

    producer.commit_transaction()
except Exception as e:
    producer.abort_transaction()
    raise
```

#### Admin Client

```python
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource

admin = AdminClient({'bootstrap.servers': 'kafka:9092'})

# Create topics
topics = [
    NewTopic('my-topic', num_partitions=12, replication_factor=3,
             config={'retention.ms': '604800000', 'compression.type': 'snappy'}),
]
fs = admin.create_topics(topics)
for topic, f in fs.items():
    try:
        f.result()
        print(f'Topic {topic} created')
    except Exception as e:
        print(f'Failed to create topic {topic}: {e}')

# List topics
metadata = admin.list_topics(timeout=10)
for t in metadata.topics.values():
    print(f'{t.topic}: {len(t.partitions)} partitions')

# Delete topics
fs = admin.delete_topics(['old-topic'], operation_timeout=30)

# Describe/alter configs
resource = ConfigResource('topic', 'my-topic')
result = admin.describe_configs([resource])
for res, f in result.items():
    configs = f.result()
    for k, v in configs.items():
        print(f'  {k} = {v.value}')
```

#### Schema Registry + Avro (confluent_kafka)

```python
from confluent_kafka import Producer, Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer

schema_registry_conf = {'url': 'http://schema-registry:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

user_schema_str = """
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": ["null", "string"], "default": null}
  ]
}
"""

avro_serializer = AvroSerializer(schema_registry_client, user_schema_str)
avro_deserializer = AvroDeserializer(schema_registry_client)
string_serializer = StringSerializer('utf_8')

# Producer
producer = Producer({'bootstrap.servers': 'kafka:9092'})
user = {'id': 1, 'name': 'Alice', 'email': 'alice@example.com'}
producer.produce(
    topic='users',
    key=string_serializer('user-1'),
    value=avro_serializer(user, SerializationContext('users', MessageField.VALUE)),
)
producer.flush()

# Consumer
consumer = Consumer({'bootstrap.servers': 'kafka:9092', 'group.id': 'user-consumer'})
consumer.subscribe(['users'])
msg = consumer.poll(1.0)
if msg:
    user = avro_deserializer(msg.value(), SerializationContext('users', MessageField.VALUE))
    print(user)
```

---

### kafka-python

```python
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
import json

# Producer
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None,
    acks='all',
    retries=3,
    linger_ms=10,
    compression_type='gzip',
    max_request_size=1048576,
)

future = producer.send('my-topic', key='my-key', value={'hello': 'world'})
record_metadata = future.get(timeout=10)  # block for confirmation
print(f'Sent to {record_metadata.topic}:{record_metadata.partition}@{record_metadata.offset}')
producer.flush()
producer.close()

# Consumer
consumer = KafkaConsumer(
    'my-topic',
    bootstrap_servers=['kafka:9092'],
    group_id='my-group',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    max_poll_records=100,
    consumer_timeout_ms=1000,
)

for message in consumer:
    print(f'{message.topic}:{message.partition}:{message.offset} = {message.value}')
    consumer.commit()

consumer.close()

# Seeking to specific offset
from kafka import TopicPartition
tp = TopicPartition('my-topic', 0)
consumer.assign([tp])
consumer.seek(tp, 1000)   # seek to offset 1000
consumer.seek_to_beginning(tp)
consumer.seek_to_end(tp)

# Admin
admin = KafkaAdminClient(bootstrap_servers='kafka:9092')
admin.create_topics([NewTopic(name='new-topic', num_partitions=6, replication_factor=3)])
admin.delete_topics(['old-topic'])
admin.close()
```

---

### aiokafka (Asyncio)

```python
import asyncio
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

async def produce():
    producer = AIOKafkaProducer(bootstrap_servers='kafka:9092')
    await producer.start()
    try:
        await producer.send_and_wait('my-topic', b'Hello Kafka', key=b'key1')
    finally:
        await producer.stop()

async def consume():
    consumer = AIOKafkaConsumer(
        'my-topic',
        bootstrap_servers='kafka:9092',
        group_id='my-group',
        auto_offset_reset='earliest',
    )
    await consumer.start()
    try:
        async for msg in consumer:
            print(f'{msg.topic}:{msg.partition}:{msg.offset} = {msg.value}')
    finally:
        await consumer.stop()

asyncio.run(produce())
asyncio.run(consume())
```

---

### Faust (Stream Processing in Python)

```python
import faust

app = faust.App('my-app', broker='kafka://kafka:9092')

# Define a topic with schema
class Order(faust.Record):
    order_id: str
    amount: float
    user_id: str

orders_topic = app.topic('orders', value_type=Order)
totals_topic = app.topic('order-totals')

# Stateless agent
@app.agent(orders_topic)
async def process_orders(orders):
    async for order in orders:
        print(f'Order {order.order_id}: ${order.amount}')
        await totals_topic.send(value=order)

# Table (stateful aggregation — backed by Kafka changelog)
order_count = app.Table('order-count', default=int)

@app.agent(orders_topic)
async def count_orders(orders):
    async for order in orders.group_by(Order.user_id):
        order_count[order.user_id] += 1
        print(f'User {order.user_id} has {order_count[order.user_id]} orders')

# Timed operations
@app.timer(interval=10.0)
async def periodic_flush():
    print('Running periodic flush...')

# CLI: faust -A my_app worker -l info
```

---

## 12. Design Patterns

### 1. Event Sourcing

```
Commands ──► Command Handler ──► Domain Events ──► Kafka Topic (event log)
                                                        │
                                          ┌─────────────┼─────────────┐
                                          ▼             ▼             ▼
                                     Read Model    Audit Log    Downstream Service
```

- Topics act as the single source of truth.
- Consumers rebuild state by replaying the event log.
- Combine with `cleanup.policy=compact` to retain only latest event per key.

### 2. CQRS (Command Query Responsibility Segregation)

```
Write Path:  API ──► Kafka Producer ──► Command Topic ──► Command Consumer ──► DB Write
Read Path:   API ──► Read Model ──────────────────────────────────────────────────────►
                          ▲
              Kafka Consumer (materializes read model from event stream)
```

### 3. Outbox Pattern (Transactional Outbox)

```
Service ──► DB Transaction (business data + outbox table)
               │
       Outbox Poller / CDC (Debezium)
               │
           Kafka Topic ──► Downstream consumers
```

- Eliminates dual-write problem.
- Guarantees at-least-once delivery to Kafka.

### 4. Saga Pattern (Distributed Transactions)

```
Order Service  ──► "order.created"  ──► Payment Service ──► "payment.processed"
                                                          └──► "payment.failed"
                                                                      │
                                             "order.cancelled" ◄──────┘
```

- Choreography: services react to events.
- Orchestration: a saga coordinator sends commands and listens for results.

### 5. Dead Letter Queue (DLQ)

```python
# Consumer with DLQ pattern
def process_with_dlq(consumer, producer, dlq_topic, max_retries=3):
    msg = consumer.poll(1.0)
    if msg is None:
        return
    
    retry_count = int(msg.headers().get('retry-count', b'0'))
    
    try:
        process_message(msg)
        consumer.commit(asynchronous=False)
    except Exception as e:
        if retry_count < max_retries:
            # Re-enqueue with incremented retry count
            producer.produce(
                msg.topic(),
                key=msg.key(),
                value=msg.value(),
                headers={'retry-count': str(retry_count + 1).encode()},
            )
        else:
            # Send to DLQ
            producer.produce(
                dlq_topic,
                key=msg.key(),
                value=msg.value(),
                headers={'error': str(e).encode(), 'original-topic': msg.topic().encode()},
            )
        consumer.commit(asynchronous=False)
```

### 6. Fan-Out

```
Master Topic ──► Consumer Group A (Analytics)
             ──► Consumer Group B (Notifications)
             ──► Consumer Group C (Archival)
```

- Each consumer group reads all messages independently.
- No coupling between consumers.
- Scale each group independently.

### 7. Kafka as a Message Bus (Pub/Sub)

```python
# Publisher
producer.produce('domain.events', key=entity_id, value=event_json)

# Multiple subscribers
consumer_analytics = Consumer({..., 'group.id': 'analytics-group'})
consumer_notify    = Consumer({..., 'group.id': 'notification-group'})
consumer_audit     = Consumer({..., 'group.id': 'audit-group'})
```

### 8. Stream-Table Join (Enrichment)

```
Orders Stream ──┐
                ├──► Kafka Streams Join ──► Enriched Orders Stream
Products Table ─┘   (enrich order with product details)
```

```python
# Faust implementation
products_table = app.Table('products', default=dict)

@app.agent(orders_topic)
async def enrich_orders(orders):
    async for order in orders:
        product = products_table.get(order.product_id, {})
        enriched = {**order.asdict(), 'product_name': product.get('name')}
        await enriched_orders_topic.send(value=enriched)
```

### 9. Windowed Aggregation

```python
# Count events per 5-minute tumbling window (Faust)
@app.agent(events_topic)
async def windowed_count(events):
    async for event in events.group_by(Event.user_id):
        window_count[event.user_id].incr()
```

### 10. Change Data Capture (CDC) with Debezium

```json
// debezium-postgres-connector.json
{
  "name": "postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "debezium",
    "database.password": "secret",
    "database.dbname": "mydb",
    "database.server.name": "mydb",
    "table.include.list": "public.orders,public.users",
    "plugin.name": "pgoutput",
    "publication.autocreate.mode": "filtered",
    "slot.name": "debezium_slot",
    "topic.prefix": "cdc"
  }
}
```

### 11. Request-Reply (Synchronous over Kafka)

```python
import uuid

# Requester
reply_topic = f'replies.{uuid.uuid4()}'
consumer = Consumer({..., 'group.id': reply_topic})
consumer.subscribe([reply_topic])

producer.produce(
    'requests',
    key=request_id,
    value=json.dumps(payload),
    headers={'reply-to': reply_topic.encode(), 'correlation-id': request_id.encode()},
)

# Wait for reply (with timeout)
msg = consumer.poll(timeout=5.0)
if msg:
    reply = json.loads(msg.value())
```

---

## 13. CLI Quick Reference

### Topic Management

```bash
# Create topic
kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --topic my-topic \
  --partitions 12 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config compression.type=snappy

# List topics
kafka-topics.sh --bootstrap-server kafka:9092 --list

# Describe topic
kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic my-topic

# Alter topic (add partitions only — can't reduce)
kafka-topics.sh --bootstrap-server kafka:9092 \
  --alter --topic my-topic --partitions 24

# Delete topic
kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic my-topic

# Change topic config
kafka-configs.sh --bootstrap-server kafka:9092 \
  --alter --entity-type topics --entity-name my-topic \
  --add-config retention.ms=86400000

# Describe topic config
kafka-configs.sh --bootstrap-server kafka:9092 \
  --describe --entity-type topics --entity-name my-topic
```

### Producer / Consumer (Console)

```bash
# Console producer
kafka-console-producer.sh --bootstrap-server kafka:9092 \
  --topic my-topic \
  --property key.separator=: \
  --property parse.key=true

# Console consumer (from beginning)
kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic my-topic \
  --from-beginning \
  --property print.key=true \
  --property print.timestamp=true \
  --max-messages 100

# Console consumer with Avro (Confluent)
kafka-avro-console-consumer --bootstrap-server kafka:9092 \
  --schema-registry http://schema-registry:8081 \
  --topic users --from-beginning
```

### Consumer Group Management

```bash
# List groups
kafka-consumer-groups.sh --bootstrap-server kafka:9092 --list

# Describe group (show lag)
kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --describe --group my-group

# Reset offsets (dry run)
kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --group my-group --topic my-topic \
  --reset-offsets --to-earliest --dry-run

# Reset offsets (execute)
kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --group my-group --topic my-topic \
  --reset-offsets --to-earliest --execute

# Reset to specific offset
kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --group my-group --topic my-topic:0 \
  --reset-offsets --to-offset 5000 --execute

# Reset to datetime
kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --group my-group --topic my-topic \
  --reset-offsets --to-datetime 2024-01-01T00:00:00.000 --execute

# Delete group
kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --delete --group my-group
```

### ACL Management

```bash
# Grant producer access
kafka-acls.sh --bootstrap-server kafka:9092 \
  --add --allow-principal User:alice \
  --operation Write --operation Describe \
  --topic my-topic

# Grant consumer access
kafka-acls.sh --bootstrap-server kafka:9092 \
  --add --allow-principal User:alice \
  --operation Read --operation Describe \
  --topic my-topic \
  --group my-group

# List ACLs
kafka-acls.sh --bootstrap-server kafka:9092 --list

# Remove ACL
kafka-acls.sh --bootstrap-server kafka:9092 \
  --remove --allow-principal User:alice \
  --operation Write --topic my-topic
```

### Rebalance & Partition Reassignment

```bash
# Generate reassignment plan
kafka-reassign-partitions.sh --bootstrap-server kafka:9092 \
  --generate \
  --topics-to-move-json-file topics.json \
  --broker-list "1,2,3"

# Execute reassignment
kafka-reassign-partitions.sh --bootstrap-server kafka:9092 \
  --execute --reassignment-json-file reassignment.json

# Verify reassignment
kafka-reassign-partitions.sh --bootstrap-server kafka:9092 \
  --verify --reassignment-json-file reassignment.json

# Preferred replica election
kafka-leader-election.sh --bootstrap-server kafka:9092 \
  --election-type preferred \
  --all-topic-partitions
```

### Offset Tools

```bash
# Get offsets for a topic
kafka-get-offsets.sh --bootstrap-server kafka:9092 \
  --topic my-topic

# Get earliest / latest
kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list kafka:9092 \
  --topic my-topic \
  --time -1  # -1 = latest, -2 = earliest
```

---

## 14. Configuration Reference Tables

### Producer Performance Tuning

| Goal | Setting | Value |
|---|---|---|
| High throughput | `linger.ms` | 20–100 |
| High throughput | `batch.size` | 65536–1048576 |
| High throughput | `compression.type` | `lz4` or `snappy` |
| Low latency | `linger.ms` | 0 |
| Low latency | `batch.size` | 16384 |
| Durability | `acks` | `all` |
| Durability | `enable.idempotence` | `true` |
| Durability | `retries` | Max int |
| Ordering | `max.in.flight.requests.per.connection` | 1 (or 5 with idempotence) |

### Consumer Performance Tuning

| Goal | Setting | Value |
|---|---|---|
| High throughput | `fetch.min.bytes` | 65536 |
| High throughput | `fetch.max.wait.ms` | 500 |
| High throughput | `max.poll.records` | 1000–5000 |
| Low latency | `fetch.min.bytes` | 1 |
| Low latency | `fetch.max.wait.ms` | 0 |
| Stability | `max.poll.interval.ms` | > processing time |
| Stability | `session.timeout.ms` | 30000–60000 |
| Stability | `heartbeat.interval.ms` | session/3 |

### Broker Tuning

| Setting | Recommended | Notes |
|---|---|---|
| `num.network.threads` | CPU cores | Network I/O |
| `num.io.threads` | CPU cores × 2 | Disk I/O |
| `socket.send.buffer.bytes` | 1048576 | OS SO_SNDBUF |
| `socket.receive.buffer.bytes` | 1048576 | OS SO_RCVBUF |
| `log.flush.interval.messages` | Long | Let OS handle |
| `log.flush.interval.ms` | Long | Let OS fsync |
| `replica.fetch.max.bytes` | 10485760 | Must ≥ `max.message.bytes` |
| `num.replica.fetchers` | 4 | Replication threads |
| `zookeeper.session.timeout.ms` | 18000 | ZK mode only |

---

## 15. Best Practices

### Topic Design

- **Name topics by domain event**, not by system: `order.created` not `order-service-output`.
- **Use a consistent naming convention**: `{domain}.{entity}.{event}` or `{team}.{domain}.{topic}`.
- **Partition count**: Start with `max(expected_consumers, throughput / 10MB_s)`. Never reduce.
- **Replication factor**: Always 3 in production. `min.insync.replicas=2`.
- **Avoid too many topics**: Each topic-partition has overhead (file handles, memory, leader election time).
- **Use `cleanup.policy=compact`** for reference data (latest value per key) and `delete` for event streams.
- **Separate concerns**: One topic per event type. Don't multiplex unrelated events.

### Producer Best Practices

- **Enable idempotence** (`enable.idempotence=true`) always. Nearly zero overhead.
- **Use transactions** only when you need cross-partition atomicity or consume-transform-produce exactly once.
- **Compress data**: `lz4` for speed, `zstd` for best ratio.
- **Use meaningful keys** to control partitioning. Null keys lose ordering guarantees.
- **Tune `linger.ms` and `batch.size`** together for throughput vs latency trade-off.
- **Handle `delivery.report` callbacks**: Never ignore delivery errors.
- **Always call `flush()`** before shutdown to drain the buffer.

### Consumer Best Practices

- **Disable auto-commit** for at-least-once semantics. Commit after processing.
- **Commit after processing**, not before (would lose messages on failure).
- **Use cooperative rebalancing** (`CooperativeStickyAssignor`) for zero-downtime rebalance.
- **Use static membership** (`group.instance.id`) for stateful consumers.
- **Keep `max.poll.interval.ms`** well above your maximum processing time.
- **Process records in batches** for throughput; batch-commit offsets.
- **Always close consumers** cleanly to trigger final offset commit and group leave.
- **Monitor consumer lag** — lag growing = undersized consumer group or slow processing.

### Schema & Serialization

- **Always use a schema** (Avro, Protobuf, JSON Schema) with Schema Registry in production.
- **Prefer Avro or Protobuf** over JSON for production — smaller, faster, validated.
- **Default compatibility to `BACKWARD`**: New consumers can read old data.
- **Never delete fields** unless migrating compatibility to `NONE`.
- **Version schemas explicitly** in subject naming: `{topic}-value`.

### Operational Best Practices

- **Rack awareness**: Spread replicas across failure domains (`broker.rack`).
- **Monitor under-replicated partitions**: Any > 0 is a problem.
- **Controlled shutdown**: Use `kafka-server-stop.sh` not `kill -9`.
- **Rolling restarts**: One broker at a time, wait for ISR recovery.
- **Capacity plan storage**: `retention_time × throughput × replication_factor × 1.2` (buffer).
- **Use separate disks** for Kafka logs (not OS disk). RAID 10 or JBOD.
- **Tune OS**: `vm.swappiness=1`, `vm.dirty_ratio=80`, increase `ulimit -n` file descriptors.
- **Disable disk caching** for Kafka data dirs: Kafka manages its own page cache usage.

### Security Best Practices

- **Never use PLAINTEXT in production**. Use SASL_SSL or SSL.
- **Rotate credentials** regularly for SASL/PLAIN and SCRAM.
- **Least privilege**: Grant only the operations each service needs.
- **Encrypt in transit**: SSL for all client-broker and inter-broker communication.
- **Encrypt at rest**: Use volume encryption (OS/cloud-level) for sensitive data.
- **Audit ACL changes**: Log all `kafka-acls.sh` operations.

---

## 16. Common Pitfalls

| Pitfall | Symptom | Fix |
|---|---|---|
| **Reducing partition count** | Not possible via CLI | Plan partition count upfront; can only increase |
| **Null key + ordering** | Messages appear out of order | Use consistent non-null keys for ordering |
| **Auto-commit data loss** | Messages lost after consumer crash before commit | Set `enable.auto.commit=false`, commit manually |
| **Slow processing exceeding `max.poll.interval.ms`** | Consumer kicked from group, rebalance storm | Increase `max.poll.interval.ms` or reduce `max.poll.records` |
| **`min.insync.replicas` > RF** | Producer stuck, can't write | Always keep `min.isr < replication.factor` |
| **Leader not preferred replica** | Uneven broker load | Run preferred leader election periodically |
| **Consumer lag growing indefinitely** | Backpressure, under-provisioned | Scale consumer group, optimize processing |
| **Broker OOM** | Broker crash, partition unavailability | Tune heap (6 GB typical), rely on page cache |
| **Message too large** | `RecordTooLargeException` | Align `max.message.bytes` (broker), `message.max.bytes` (topic), `fetch.max.bytes` (consumer), `replica.fetch.max.bytes` (broker) |
| **Duplicate processing** | At-least-once without idempotent consumers | Make consumers idempotent or use exactly-once |
| **ZooKeeper session timeout** | Controller failover, temporary unavailability | Tune `zookeeper.session.timeout.ms` |
| **Hot partition** | One partition gets all traffic | Ensure keys are evenly distributed; use custom partitioner |
| **Unclean leader election** | Data loss after broker failure | Keep `unclean.leader.election.enable=false` (default) |
| **Forgetting `producer.flush()`** | Messages silently dropped on shutdown | Always flush before process exit |
| **Offset reset to earliest on new group** | Reprocessing all historical data | Set `auto.offset.reset=latest` for new real-time consumers |

---

## Quick Decision Guide

```
Need ordered messages?
  └─► Use a key + single partition per entity

Need exactly-once?
  └─► Producer: enable.idempotence=true + transactional.id
  └─► Consumer: isolation.level=read_committed + transaction API

Need fan-out?
  └─► Multiple consumer groups on same topic

Need stateful aggregation in Python?
  └─► Use Faust or Kafka Streams (Java)

Need to replay history?
  └─► Consumer seek() to earliest offset

Need schema evolution?
  └─► Schema Registry + Avro/Protobuf + BACKWARD compatibility

Need zero-downtime rebalance?
  └─► CooperativeStickyAssignor + static membership

Need to integrate databases?
  └─► Debezium CDC (source) or JDBC Connector (sink)
```

---

*Reference: [Apache Kafka Docs](https://kafka.apache.org/documentation/) | [Confluent Docs](https://docs.confluent.io/) | [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python) | [kafka-python](https://kafka-python.readthedocs.io/) | [Faust](https://faust-streaming.github.io/faust/)*
