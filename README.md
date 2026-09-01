# Kafka Clone in Go

A minimal **Kafka-inspired message broker written in Go**, built to understand the core concepts behind distributed messaging systems such as **topics, producers, consumers, offsets, consumer groups, and durable logs**.

> This project is intentionally lightweight and educational. It is **not intended to be a production replacement for Apache Kafka**.

## ✨ Features

* 📌 **Topics** — Create and manage independent message streams
* 📨 **Producers** — Publish messages with keys and values
* 📥 **Consumers** — Read messages from a specific offset
* 👥 **Consumer Groups** — Track consumption using consumer group identifiers
* 💾 **Durable Storage** — Persist messages using append-only JSONL files
* 🔢 **Offset-based Consumption** — Each message has a monotonically increasing offset
* ⚡ **In-memory Broker** — Fast message operations with lightweight in-memory state
* 🛠️ **CLI Interface** — Publish and consume messages directly from the terminal
* 🎯 **Demo Mode** — Easily simulate producer/consumer traffic

---

## 🏗️ Architecture

The broker follows a simplified Kafka-like architecture:

```text
                    ┌──────────────────┐
                    │      Producer    │
                    └────────┬─────────┘
                             │
                             │ Publish
                             ▼
                    ┌──────────────────┐
                    │      Broker      │
                    │                  │
                    │  ┌────────────┐  │
                    │  │   Topic    │  │
                    │  │            │  │
                    │  │  Log       │  │
                    │  │  0 → msg   │  │
                    │  │  1 → msg   │  │
                    │  │  2 → msg   │  │
                    │  └────────────┘  │
                    └────────┬─────────┘
                             │
                             │ Read(offset)
                             ▼
                    ┌──────────────────┐
                    │      Consumer    │
                    │                  │
                    │   Consumer Group │
                    └──────────────────┘
```

Messages are stored in an append-only topic log:

```text
Topic: orders

Offset     Key          Value
------     ---          ----------------
0          user-42      created-order
1          user-17      payment-completed
2          user-42      order-shipped
```

The offset allows consumers to resume reading from a particular position.

## 🚀 Getting Started

### Prerequisites

* Go 1.20+
* Git

### Clone the repository

```bash
git clone <repository-url>
cd kafka-clone
```

### Run the demo

```bash
go run ./cmd/kafkaclone --demo
```

The demo creates a topic, publishes messages, and consumes them using the broker.

---

## 📨 Publish Messages

Publish a message to a topic:

```bash
go run ./cmd/kafkaclone \
  --topic orders \
  --key user-42 \
  --value "created-order"
```

Another example:

```bash
go run ./cmd/kafkaclone \
  --topic orders \
  --key user-17 \
  --value "payment-completed"
```

Messages are appended to the topic's persistent log.

---

## 📥 Consume Messages

Consume messages from a topic:

```bash
go run ./cmd/kafkaclone \
  --topic orders \
  --consume \
  --group demo-group
```

A consumer reads messages using their offsets.

Conceptually:

```text
Consumer
   │
   │ offset = 0
   ▼
┌───────┬───────┬───────┐
│ msg 0 │ msg 1 │ msg 2 │
└───────┴───────┴───────┘
    ▲
    │
  read
```

---

## 💾 Storage

Topic messages are persisted under:

```text
./data/
```

For example:

```text
data/
├── orders.jsonl
├── payments.jsonl
└── users.jsonl
```

Each line represents one message.

Example:

```json
{"offset":0,"key":"user-42","value":"created-order"}
{"offset":1,"key":"user-17","value":"payment-completed"}
{"offset":2,"key":"user-42","value":"order-shipped"}
```

This provides simple **append-only durable storage** while keeping the implementation easy to understand.

---

## 🧠 Core Concepts

### Topics

A topic is an independent stream of messages.

```text
orders
payments
notifications
```

Each topic maintains its own log and offsets.

### Producers

Producers publish messages to topics.

```text
Producer → Broker → Topic Log
```

### Consumers

Consumers read messages from a topic starting from an offset.

```text
Topic Log → Consumer
```

### Offsets

Every message receives a monotonically increasing offset:

```text
0 → message A
1 → message B
2 → message C
3 → message D
```

A consumer can use the offset to determine where it should continue reading.

### Consumer Groups

Consumers can be associated with a consumer group:

```text
             orders
                │
       ┌────────┴────────┐
       ▼                 ▼
   Consumer A         Consumer B
       │                 │
       └──── group: orders-workers ────┘
```

This project uses consumer groups as a simplified abstraction inspired by Kafka.

---

## 🔥 Example Workflow

Start by publishing several messages:

```bash
go run ./cmd/kafkaclone --topic orders --key user-1 --value "order-created"

go run ./cmd/kafkaclone --topic orders --key user-2 --value "order-created"

go run ./cmd/kafkaclone --topic orders --key user-1 --value "order-shipped"
```

Then consume:

```bash
go run ./cmd/kafkaclone \
  --topic orders \
  --consume \
  --group orders-workers
```

You should see the messages along with their offsets.

---

## 🛠️ Technology Stack

| Technology | Purpose                          |
| ---------- | -------------------------------- |
| Go         | Core implementation              |
| Goroutines | Concurrent broker operations     |
| Channels   | Communication between components |
| JSONL      | Persistent message storage       |
| File I/O   | Durable append-only logs         |
| CLI        | Producer/consumer interaction    |

---

## 🎯 What This Project Demonstrates

This project is primarily designed to explore **backend systems and concurrency in Go**.

It covers concepts such as:

* Message brokers
* Producer/consumer architecture
* Append-only logs
* Offsets
* Consumer groups
* Persistence
* Concurrency
* Goroutines
* Channels
* File-based storage
* CLI application design
* Basic systems architecture

---

## 🔮 Future Improvements

Possible extensions include:

* [ ] Multiple partitions per topic
* [ ] Partition-based message ordering
* [ ] Consumer group offset persistence
* [ ] Message replication
* [ ] Leader/follower architecture
* [ ] TCP-based broker protocol
* [ ] Network producers and consumers
* [ ] Concurrent consumers
* [ ] Message batching
* [ ] Configurable retention policies
* [ ] Log segmentation
* [ ] Crash recovery
* [ ] Checksums
* [ ] Metrics with Prometheus
* [ ] Docker support
* [ ] Integration tests
* [ ] Benchmarking

---

## 📚 Why Build a Kafka Clone?

Apache Kafka is built around a few powerful ideas:

```text
Messages
   ↓
Append-only log
   ↓
Offsets
   ↓
Consumers
   ↓
Consumer groups
   ↓
Partitions + replication
```

Implementing a simplified version from scratch makes these concepts much easier to understand than simply using Kafka as a black box.

---

## ⚠️ Disclaimer

This is an **educational Kafka-inspired broker**.

It does not currently provide the fault tolerance, distributed coordination, replication, partition management, networking, scalability, durability guarantees, or operational tooling of Apache Kafka.

Use Apache Kafka or another production-grade message broker for real-world workloads.

---

## 📄 License

This project is open source. Add your preferred license here, such as MIT.
