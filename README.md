# streaming-04-visualization

[![API Reference](https://img.shields.io/badge/API--Utils-datafun--streaming-purple)](https://denisecase.github.io/datafun-streaming/api/)
[![Workflow Guide](https://img.shields.io/badge/Pro--Guide-pro--analytics--02-green)](https://denisecase.github.io/pro-analytics-02/workflow-b-apply-example-project/)
[![Python 3.14](https://img.shields.io/badge/python-3.14%2B-blue?logo=python)](./pyproject.toml)
[![MIT](https://img.shields.io/badge/license-see%20LICENSE-yellow.svg)](./LICENSE)

> Streaming data analytics: visualize messages as they arrive.

Streaming analytics requires working with data in motion
and distributed, scalable systems.
This course builds capabilities through working projects.
In the age of generative AI, durable skills are grounded in real work:
setting up a professional environment,
reading and running code,
understanding the logic,
and pushing work to a shared repository.
Each project follows the structure of professional Python projects.
We learn by doing.

## This Project

This project focuses on visualizing streaming data as it is consumed.

The project uses Kafka to move sales messages from a producer to a consumer.
The consumer reads each message, validates required fields, computes derived values,
writes processed records to CSV, and updates a live chart.

This module adds live visualization to the streaming workflow.

The goal is to see how a chart can change as new messages arrive,
making the stream easier to observe and interpret.

## Working Files

You'll work with just these areas:

- **data/** - input data and generated output files
- **docs/** - the project narrative and documentation
- **src/streaming/** - producer, consumer, and supporting code
- **pyproject.toml** - update authorship & links
- **zensical.toml** - update authorship & links

## Instructions

Follow the
[step-by-step workflow guide](https://denisecase.github.io/pro-analytics-02/workflow-b-apply-example-project/)
to complete:

1. Phase 1. **Start & Run**
2. Phase 2. **Change Authorship**
3. Phase 3. **Read & Understand**
4. Phase 4. **Modify**
5. Phase 5. **Apply**

## Challenges

Challenges are expected.
Sometimes instructions may not quite match your operating system.
When issues occur, share screenshots, error messages, and details about what you tried.
Working through issues is part of implementing professional projects.

## Success

After completing Phase 1. **Start & Run**, you'll have your own GitHub project
running with Kafka.

Use four named terminals:

1. **kafka** - keep the Kafka message broker running
2. **topics** - create, list, or reset Kafka topics
3. **producer** - run the project and producer
4. **consumer** - run the consumer

After the producer and consumer run successfully, you should see:

```shell
========================
Consumer executed successfully!
========================
```

A new file `project.log` will appear in the root project folder
and processed data will appear in data/output/.

## Command Reference

The commands below are used in the workflow guide above.
They are provided here for convenience.

**Important:** the first few times you run a project,
follow the guide with the **complete instructions**.

<details>
<summary>Show command reference</summary>

### In a machine terminal (open in your `Repos` folder)

After you get a copy of this repo in your own GitHub account,
open a machine terminal in your `Repos` folder:

```bash
# Replace username with YOUR GitHub username.
git clone https://github.com/username/streaming-04-visualization

cd streaming-04-visualization
code .
```

### In VS Code Terminal 1: Start Kafka (kafka)

For full instructions see
[**start kafka**](https://denisecase.github.io/pro-analytics-02/kafka/start-kafka/).

If any command fails,
repeat the steps at
[**install kafka**](https://denisecase.github.io/pro-analytics-02/kafka/install-kafka/)
until starting up is reliable.

Open a new VS Code terminal. Rename it `kafka`.
If running Windows, specify the terminal type as **wsl** or
type `wsl`.
Run the commands one at a time.

Step 1. Verify Java and PATH

```bash
echo "$JAVA_HOME"

"$JAVA_HOME/bin/java" --version
```

Step 2. Rebuild ClusterID (as needed)

```bash
cd ~/kafka

rm -rf /tmp/kraft-combined-logs

KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

echo "Cluster ID: $KAFKA_CLUSTER_ID"

bin/kafka-storage.sh format --standalone -t "$KAFKA_CLUSTER_ID" -c config/server.properties
```

Step 3. Start kafka server (keep running)

```bash
cd ~/kafka

bin/kafka-server-start.sh config/server.properties
```

### In VS Code terminal 2: Create Topic (topics)

For full instructions see
[**create topic**](https://denisecase.github.io/pro-analytics-02/kafka/create-topic/).

The topic name must match the name defined in your
`.env` file (copy `.env.example` to `.env`).

Open another VS Code terminal. Rename it `topics`.
If running Windows, specify the terminal type as **wsl** or
type `wsl`.
Run the commands one at a time.

```bash
cd ~/kafka

bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --topic streaming-04-visualization-case
```

### In VS Code Terminal 3: Run Project and Producer (producer)

Open another VS Code terminal. Rename it `producer`.
If running Windows, use **PowerShell**.
Run the commands one at a time.

```shell
# reset uv cache only if/when you start getting strange dependency errors
# uv cache clean

uv self update
uv python pin 3.14
uv sync --extra dev --extra docs --upgrade

uvx pre-commit install

git add -A
uvx pre-commit run --all-files
# repeat if changes were made
git add -A
uvx pre-commit run --all-files

# run the producer
clear
uv run python -m streaming.kafka_producer_case

# do chores
uv run ruff format .
uv run ruff check . --fix
uv run python -m pyright
uv run python -m pytest
uv run python -m zensical build

# save progress
git add -A
git commit -m "your message here"
git push -u origin main
```

### In VS Code Terminal 4: Run Consumer (consumer)

Open another VS Code terminal. Rename it `consumer`.
If running Windows, use **PowerShell**.
Run the commands one at a time.
Clear the terminal, then start the consumer.

```shell
clear
uv run python -m streaming.kafka_consumer_case
```

To start fresh, see
[manage topics](https://denisecase.github.io/pro-analytics-02/kafka/manage-topics/)
to delete the topic and recreate it.

</details>

## Notes

- Use the **UP ARROW** and **DOWN ARROW** in the terminal to scroll through past commands.
- Use `CTRL+f` to find (and replace) text within a file.
- You do not need to add to or modify `tests/`. They are provided for example only.
- Many files are silent helpers. Explore as you like, but nothing is required.
- You do NOT not to understand everything; understanding builds naturally over time.

## Troubleshooting >>> or

If you see something like this in your terminal: `>>>` or `...`
You accidentally started Python interactive mode.
It happens.
Press `Ctrl+c` (both keys together) or `Ctrl+Z` then `Enter` on Windows.

## Example Producer Output

The example producer output is unchanged from previous projects.

## Example Consumer Output

Look for the word `chart`:

```text
| C04 | ========================
| C04 | START consumer main()
| C04 | ========================
| C04 | ROOT_DIR = .
| C04 | DATA_DIR = data
| C04 | OUTPUT_CSV = data\output\consumed_sales.csv
| C04 | OUTPUT_CHART = data\output\sales_chart_case.png
| C04 | REGIONS_CSV = data\regions.csv
| C04 | PRODUCTS_CSV = data\products.csv
| C04 | CURRENCIES_CSV = data\currencies.csv
| C04 | DISCOUNT_CODES_CSV = data\discount_codes.csv
| C04 | ========================
| C04 | SECTION A. Acquire
| C04 | ========================
| C04 | Loading settings from .env...
| C04 | KAFKA_BOOTSTRAP_SERVERS = localhost:9092
| C04 | KAFKA_TOPIC = streaming-04-visualization-case
| C04 | KAFKA_GROUP_ID = streaming-consumer-group-A
| C04 | CONSUMER_TIMEOUT_SECONDS = 10.0
| C04 | CONSUMER_MAX_MESSAGES = 1000
| C04 | Verifying Kafka connection...
| C04 | Kafka port is reachable.
| C04 | Verifying Kafka topic...
%3|1778432180.742|FAIL|rdkafka#producer-1| [thrd:localhost:9092/1]: localhost:9092/1: Connect to ipv4#127.0.0.1:9092 failed: Unknown error (after 2017ms in state CONNECT)
%3|1778432182.792|FAIL|rdkafka#producer-1| [thrd:localhost:9092/bootstrap]: localhost:9092/bootstrap: Connect to ipv4#127.0.0.1:9092 failed: Unknown error (after 2049ms in state CONNECT)
%3|1778432182.840|FAIL|rdkafka#producer-1| [thrd:localhost:9092/1]: localhost:9092/1: Connect to ipv4#127.0.0.1:9092 failed: Unknown error (after 2032ms in state CONNECT, 1 identical error(s) suppressed)
| C04 | Topic 'streaming-04-visualization-case' exists.
| C04 | Found 3 message(s) available.
| C04 | Creating Kafka consumer...
| C04 | Subscribed to topic: 'streaming-04-visualization-case' (reading from beginning)
| C04 | ========================
| C04 | SECTION C. Consume and Process Messages
| C04 | ========================
| C04 | Initializing output...
| C04 | Output CSV cleared: consumed_sales.csv
| C04 | Live chart initialized.
| C04 | Loading enrichment reference data...
| C04 | Found 6 region tax rates.
| C04 | Consuming messages...
| C04 | Waiting for up to 1000 message(s).
| C04 | Press CTRL+C to stop early.

| C04 | {'currency_code': 'USD', 'customer_id': 'CUST-4150', 'customer_note': 'Gift for my team', 'datetime': '2026-05-04T08:11:00Z', 'device_type': 'tablet', 'discount_code': '', 'is_new_customer': 'false', 'is_online': 'true', 'order_id': 'e7324981-a9f0-419f-b708-d0a333451fff', 'payment_method': 'paypal', 'product_id': 'PY-STREAM-005', 'quantity': '3', 'referral_source': 'paid_search', 'region_id': 'US-TX', 'unit_price': '59.99', '\_kafka_key': 'US-TX', '\_kafka_partition': 0, '\_kafka_offset': 0}
| C04 | subtotal=179.97
| C04 | tax=14.85
| C04 | total=194.82
| C04 | running_total=194.82
| C04 | MESSAGE ACCEPTED
| C04 | order=e7324981-a9f0-419f-b708-d0a333451fff
| C04 | total=$194.82
| C04 | consumed=1
| C04 | RUNNING STATS
| C04 | total_sales=$194.82
| C04 | average=$194.82
| C04 | min=$194.82
| C04 | max=$194.82
| C04 | {'currency_code': 'USD', 'customer_id': 'CUST-1106', 'customer_note': 'Gift for my team', 'datetime': '2026-05-04T08:23:00Z', 'device_type': 'mobile', 'discount_code': '', 'is_new_customer': 'false', 'is_online': 'true', 'order_id': 'd61943e0-f543-4b5f-9c9a-18605ea4cfe5', 'payment_method': 'paypal', 'product_id': 'PY-DATA-002', 'quantity': '1', 'referral_source': 'paid_search', 'region_id': 'US-TX', 'unit_price': '49.99', '\_kafka_key': 'US-TX', '\_kafka_partition': 0, '\_kafka_offset': 1}
| C04 | subtotal=49.99
| C04 | tax=4.12
| C04 | total=54.11
| C04 | running_total=248.93
| C04 | MESSAGE ACCEPTED
| C04 | order=d61943e0-f543-4b5f-9c9a-18605ea4cfe5
| C04 | total=$54.11
| C04 | consumed=2
| C04 | RUNNING STATS
| C04 | total_sales=$248.93
| C04 | average=$124.47
| C04 | min=$54.11
| C04 | max=$194.82
| C04 | {'currency_code': 'CAD', 'customer_id': 'CUST-2133', 'customer_note': 'Learning at my own pace', 'datetime': '2026-05-04T08:28:00Z', 'device_type': 'desktop', 'discount_code': '', 'is_new_customer': 'false', 'is_online': 'true', 'order_id': '14da1915-8e74-47be-9e10-f7275d31af46', 'payment_method': 'paypal', 'product_id': 'PY-NLP-006', 'quantity': '1', 'referral_source': 'organic', 'region_id': 'CA-QC', 'unit_price': '54.99', '\_kafka_key': 'CA-QC', '\_kafka_partition': 0, '\_kafka_offset': 2}
| C04 | subtotal=54.99
| C04 | tax=8.23
| C04 | total=63.22
| C04 | running_total=312.15
| C04 | MESSAGE ACCEPTED
| C04 | order=14da1915-8e74-47be-9e10-f7275d31af46
| C04 | total=$63.22
| C04 | consumed=3
| C04 | RUNNING STATS
| C04 | total_sales=$312.15
| C04 | average=$104.05
| C04 | min=$54.11
| C04 | max=$194.82
| C04 | No message received within 10.0s timeout.
| C04 | Producer finished or paused. Stopping consumer.
| C04 | Kafka consumer closed.
| C04 | Saving artifacts...
| C04 | WROTE OUTPUT_CHART = data\output\sales_chart_case.png
| C04 | WROTE OUTPUT_CSV = data\output\consumed_sales.csv
| C04 | Live chart closed.
| C04 | ========================
| C04 | SECTION E. Exit
| C04 | ========================
| C04 | Summary:
| C04 | Consumed 3 message(s) from topic 'streaming-04-visualization-case'.
| C04 | Skipped 0 message(s).
| C04 | OUTPUT_CSV = data\output\consumed_sales.csv
| C04 | OUTPUT_CHART = data\output\sales_chart_case.png
| C04 | Total sales: $312.15
| C04 | Average sale: $104.05
| C04 | Minimum sale: $54.11
| C04 | Maximum sale: $194.82
| C04 | ========================
| C04 | Consumer executed successfully!
| C04 | ========================
```

![Example Chart after 3 messages](./docs/images/chart_after_3_messages.png)
