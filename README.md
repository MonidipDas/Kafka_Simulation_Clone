
# Kafka Clone in Go

A lightweight **Kafka-inspired message broker built in Go** to understand core concepts of message streaming systems.

> Educational project — not a production-ready Kafka replacement.

## Features

* **Topics** — Organize messages into streams
* **Producers** — Publish key/value messages
* **Consumers** — Read messages using offsets
* **Consumer Groups** — Track consumer progress
* **Offsets** — Sequential message positions
* **Durable Logs** — Persist messages in append-only `.log` files
* **CLI** — Interact with the broker from the terminal

## Architecture

![Architecture](docs/images/architecture.png)

```text
Producer → Broker → Topic → Log
                    ↓
              Consumer Group
                    ↓
                 Consumer
```

## Project Structure

```text
.
├── cmd/kafkaclone/
├── data/
│   ├── orders.log
│   └── payments.log
├── internal/
│   ├── broker/
│   ├── models/
│   └── storage/
├── docs/images/
├── go.mod
└── README.md
```

## Run

```bash
go mod tidy
go run ./cmd/kafkaclone
```

## Purpose

Built to learn how **Kafka-style topics, offsets, consumer groups, and persistent message logs** work internally.

## License

MIT
