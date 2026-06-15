# Real-time CDC Data Lakehouse Pipeline: PostgreSQL & MongoDB -> Kafka -> Delta Lake -> ClickHouse

Dự án này xây dựng một hệ thống tích hợp dữ liệu quy mô lớn kết hợp giữa xử lý thời gian thực (**Streaming CDC**) và theo lô (**Batch Ingestion**) theo kiến trúc **Data Lakehouse (Medallion)** hiện đại. 

Hệ thống sử dụng các công nghệ tiên tiến như **Avro Serialization**, **Confluent Schema Registry** để quản lý cấu trúc dữ liệu, **Delta Lake** cho tầng lưu trữ bất biến hỗ trợ ACID, và **ClickHouse** làm tầng phục vụ phân tích (Serving Layer) tối ưu thông qua động cơ `DeltaLake` engine.

---

## 🏗️ Kiến trúc Hệ thống (Data Pipeline Architecture)

```text
                               ┌───────────────────┐
                               │  Live Simulator   │
                               └─────────┬─────────┘
                                         │ (Ghi Real-time)
                                         ▼
    ┌───────────────────────┐          ┌───────────────────────┐
    │ PostgreSQL (Relational)│          │    MongoDB (NoSQL)    │
    └───────────┬───────────┘          └───────────┬───────────┘
                │ (Logical Replication)            │ (Replica Set Oplog)
                ▼                                  ▼
       ┌─────────────────┐                ┌─────────────────┐
       │ Debezium Source │                │ Debezium Source │
       └────────┬────────┘                └────────┬────────┘
                │                                  │
                └───────────────┬──────────────────┘
                                │ (Avro + Schema Registry)
                                ▼
       ┌─────────────────────────────────────────────────────┐
       │             Apache Kafka Message Broker             │
       └────────────────────────┬────────────────────────────┘
         (Streaming Path)       │
         ┌──────────────────────┴──────────────────────┐
         ▼ (spark.readStream)                          ▼ (S3 Sink Connector)
  ┌──────────────┐                              ┌──────────────┐
  │ Spark Stream │                              │ S3 Cold Sink │
  └──────┬───────┘                              └──────┬───────┘
         │                                             │ (Ghi Parquet)
         │ (Ghi Delta)                                 ▼
         │                              ┌──────────────────────────────┐
         │                              │ MinIO Bronze (Raw Archive)   │
         │                              │ - topics/ (CDC Parquet)      │
         │                              │ - batch/ (load_date Parquet) │

         │                              └──────────────────────────────┘
         ▼
  ┌────────────────────────────────────────────────────────────┐
  │              MinIO Silver Delta Tables (ACID)              │
  │  - silver/delta/ (Dữ liệu đã chuẩn hóa, khử trùng, Upsert) │
  └─────────────────────────────┬──────────────────────────────┘
                                │
                 ┌──────────────┴──────────────┐
                 │ (External DeltaLake Engine) │ (Đọc đầu vào)
                 ▼                             ▼
  ┌──────────────────────────────┐      ┌──────────────┐
  │   ClickHouse Silver Tables   │      │ Spark Batch  │
  │ (Query trực tiếp từ MinIO)   │      │ (Gold Job)   │
  └──────────────────────────────┘      └──────┬───────┘
                                               │ (Ghi JDBC)
                                               ▼
  ┌────────────────────────────────────────────────────────────┐
  │                 ClickHouse Serving Database                │
  │   - analytics.gold_fraud_analysis (MergeTree)              │
  │   - analytics.gold_user_behavior_summary (MergeTree)       │
  └────────────────────────────────────────────────────────────┘
```

---

## 📁 Cấu trúc Thư mục Dự án

```text
├── data/                           # Thư mục chứa các tệp CSV gốc (Không đẩy lên Git)
├── dags/                           # Airflow DAGs điều phối hệ thống
│   ├── generate_data_dag.py        # DAG giả lập sinh dữ liệu live (chạy mỗi 5 phút)
│   ├── batch_ingestion_dag.py      # DAG chạy Batch Ingestion PostgreSQL/MongoDB -> MinIO
│   └── spark_orchestration_dag.py  # DAG submit các PySpark Jobs (chạy mỗi 30 phút)
├── spark_apps/                     # Scripts xử lý dữ liệu PySpark
│   ├── batch_ingest_postgres.py    # Batch Ingest Postgres -> MinIO Bronze (partition theo load_date)
│   ├── batch_ingest_mongodb.py     # Batch Ingest MongoDB -> MinIO Bronze (partition theo load_date)
│   ├── transform_silver.py         # Batch Transform: Bronze (MinIO) -> Delta Lake (Silver)
│   ├── stream_transform_silver.py  # Stream Transform: Kafka (CDC Avro) -> Delta Lake (Silver)
│   └── transform_gold.py           # Gold Transform: Delta Lake (Silver) -> ClickHouse Gold (JDBC)
├── scripts/                        # Các helper scripts phục vụ vận hành
│   ├── init_clickhouse.py          # Khởi tạo DB analytics với DeltaLake Engine cho Silver tables
│   ├── generate_live_data.py       # Script chạy tay giả lập giao dịch từ host
│   ├── register_connectors.py      # Tự động đăng ký Debezium và S3 Sink với Kafka Connect
│   ├── import_users.py             # Nạp khách hàng ban đầu từ CSV vào database
│   ├── import_cards.py             # Nạp thẻ ngân hàng ban đầu
│   └── import_transactions.py      # Nạp giao dịch lịch sử ban đầu
├── infrastructure/                 # Hạ tầng Docker stack
│   ├── docker-compose.yml          # Định nghĩa toàn bộ 10 dịch vụ trong stack
│   ├── connect/                    # Dockerfile cho Kafka Connect (tải plugins)
│   └── connectors/                 # Cấu hình JSON đăng ký Kafka Connectors (Avro + S3 Parquet)
├── init_db.py                      # Khởi tạo Postgres tables và Mongo collections trống
├── requirements.txt                # Thư viện Python chạy tại máy Host
└── README.md                       # Tài liệu hướng dẫn sử dụng
```

---

## 🛠️ Chi tiết các Dịch vụ Hạ tầng (Docker Services)

Hệ thống chạy trên Docker với 10 dịch vụ liên kết chặt chẽ:
1. **`postgres` (cổng 5434):** Cơ sở dữ liệu nghiệp vụ chính (`banking_core`), bật logical replication.
2. **`mongo` (cổng 27017):** Kho lưu trữ phi cấu trúc (`banking_events`), bật Replica Set (`rs0`) để phục vụ CDC.
3. **`kafka` (cổng 9092):** Message queue trung chuyển dữ liệu dạng sự kiện (Event Streaming).
4. **`schema-registry` (cổng 8081):** Quản lý Schema Evolution và mã hóa/giải mã Avro.
5. **`connect` (cổng 8083):** Kafka Connect tích hợp Debezium Postgres/Mongo Source và S3 Sink.
6. **`minio` (cổng 9005 API / 9001 Web UI):** Hồ lưu trữ đối tượng chứa dữ liệu Bronze thô và Silver Delta Tables.
7. **`clickhouse` (cổng 8123 HTTP / 9000 TCP):** Data Warehouse phục vụ truy vấn phân tích tốc độ cao.
8. **`spark` (Local Mode):** Động cơ tính toán phân tán chạy các biến đổi PySpark.
9. **`spark-streaming`:** Container chạy Spark Structured Streaming consume liên tục từ Kafka Broker.
10. **`airflow-webserver` & `airflow-scheduler` (cổng 8080):** Bộ điều phối và giám sát toàn bộ pipeline.

---

## ⚡ Thiết kế Bảng ClickHouse (DeltaLake Engine & MergeTree)

*   **Tầng Silver (External Delta Tables):** Được cấu hình trỏ trực tiếp đến đường dẫn Delta Table trên MinIO bằng động cơ **`DeltaLake` Engine**, ClickHouse đóng vai trò là View Engine phục vụ truy vấn trực tiếp không cần lưu dữ liệu vật lý trùng lặp và loại bỏ hoàn toàn bottleneck ghi JDBC của Spark:
    *   `silver_postgres_customers`
    *   `silver_postgres_cards`
    *   `silver_postgres_transactions`
    *   `silver_mongo_login_events` / `silver_mongo_device_events`
    *   `silver_mongo_fraud_events`
    *   `silver_mongo_notification_logs`
*   **Tầng Gold (Serving Layer):** Cấu hình vật lý tối ưu bằng **`MergeTree` Engine** kèm theo các khóa chỉ mục sắp xếp (`ORDER BY`) và tính năng nullable key để đạt tốc độ truy vấn phân tích tối đa:
    *   `gold_fraud_analysis`: `ORDER BY (customer_id, transaction_date)`
    *   `gold_user_behavior_summary`: `ORDER BY customer_id`

---

## 🚀 Hướng dẫn Vận hành Hệ thống từ A - Z

### 1. Chuẩn bị Môi trường
Tạo môi trường ảo Python trên máy host và cài đặt các thư viện cần thiết:
```bash
python -m venv .venv
.venv\Scripts\activate      # Trên Windows
source .venv/bin/activate   # Trên macOS/Linux
pip install -r requirements.txt
```

### 2. Khởi chạy Hạ tầng Docker Stack
```bash
docker compose -f infrastructure/docker-compose.yml up -d --build
```
*Đợi khoảng 30-45 giây để Kafka Connect, Schema Registry, MinIO, ClickHouse và Airflow khởi động hoàn chỉnh.*

### 3. Tạo Schema & Nạp dữ liệu Lịch sử ban đầu
Đặt các tệp `users.csv`, `cards.csv`, `transactions.csv` vào thư mục `data/` và khởi chạy quy trình nạp:
```bash
# Khởi tạo bảng và collection trống
python init_db.py

# Nạp dữ liệu ban đầu
python scripts/import_users.py
python scripts/import_cards.py
python scripts/import_transactions.py
```

### 4. Đăng ký CDC & S3 Sink Connectors
Đăng ký các Kafka Connectors để bắt đầu truyền dữ liệu tự động xuống MinIO dưới dạng file nén Parquet:
```bash
python scripts/register_connectors.py
```
*Bạn có thể truy cập http://localhost:9001 (minio_admin/minio_password) để kiểm tra các file Parquet thô bắt đầu xuất hiện trong bucket `banking-lakehouse/topics/`.*

### 5. Khởi tạo Bảng ClickHouse
Tạo các database `raw`, `analytics` và các bảng ngoài kết nối đến Delta Lake trên ClickHouse:
```bash
python scripts/init_clickhouse.py
```

### 6. Quản lý trên Airflow Webserver
Truy cập **[http://localhost:8080](http://localhost:8080)** (Tài khoản: `admin` / Mật khẩu: `admin`):
* Bật (Unpause) DAG **`live_data_generator_dag`** để bắt đầu sinh giao dịch live ngẫu nhiên vào hệ thống mỗi 5 phút.
* Bật (Unpause) DAG **`lakehouse_batch_ingestion`** để chạy thử luồng Batch Ingestion trích xuất từ DB nguồn lên MinIO phân vùng theo load_date.
* Bật (Unpause) DAG **`lakehouse_spark_orchestration`** để chạy các PySpark batch jobs biến đổi dữ liệu định kỳ.

---

## 📊 Phân tích Dữ liệu Gold Layer trên ClickHouse

Để truy cập giao diện viết SQL trực quan của ClickHouse, vào trình duyệt: **[http://localhost:8123/play](http://localhost:8123/play)** (User: `default` / Password: `admin`).

Một số câu hỏi phân tích ví dụ:
* **Xem thông tin chi tiết các giao dịch gian lận kèm trạng thái gửi SMS/Email cảnh báo:**
  ```sql
  SELECT customer_name, card_brand, amount, merchant_name, risk_score, alert_type, alert_status 
  FROM analytics.gold_fraud_analysis 
  WHERE is_fraud = 'Yes' 
  LIMIT 10;
  ```
* **Tổng hợp hành vi người dùng (Top 5 khách hàng chi tiêu nhiều nhất):**
  ```sql
  SELECT customer_name, total_transactions, total_amount_spent, average_transaction_amount
  FROM analytics.gold_user_behavior_summary
  ORDER BY total_amount_spent DESC
  LIMIT 5;
  ```
