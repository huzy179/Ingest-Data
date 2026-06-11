FROM confluentinc/cp-kafka-connect-base:7.5.0

# Install Debezium PostgreSQL Source Connector
RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:2.5.4

# Install Debezium MongoDB Source Connector
RUN confluent-hub install --no-prompt debezium/debezium-connector-mongodb:2.5.4

# Install Amazon S3 Sink Connector (for MinIO)
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.1
