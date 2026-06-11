FROM confluentinc/cp-kafka-connect-base:7.5.0

# Install Debezium PostgreSQL Source Connector
RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:2.5.4

# Install Debezium MongoDB Source Connector
RUN confluent-hub install --no-prompt debezium/debezium-connector-mongodb:2.5.4

# Install ClickHouse Sink Connector
RUN confluent-hub install --no-prompt clickhouse/clickhouse-kafka-connect:latest
