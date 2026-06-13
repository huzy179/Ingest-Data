# Real-time CDC Data Lakehouse Pipeline: PostgreSQL & MongoDB -> MinIO -> Spark -> ClickHouse

Dự án này xây dựng một hệ thống ống dẫn dữ liệu thời gian thực (CDC - Change Data Capture) quy mô lớn theo kiến trúc **Data Lakehouse** hiện đại. Dữ liệu thay đổi từ PostgreSQL và MongoDB được thu thập tự động qua Debezium, lưu trữ thô tại tầng Bronze (MinIO), sau đó được xử lý, làm sạch qua Spark (Silver & Gold Layers) và lưu trữ vật lý tối ưu trong ClickHouse phục vụ phân tích.

---

## 🏗️ Kiến trúc Hệ thống (Data Pipeline Architecture)

```text
                               ┌───────────────────┐
                               │  Live Simulator   │
                               └─────────┬─────────┘
                                         │ (Ghi Real-time)
                                         ▼
   ┌───────────────────────┐           ┌───────────────────────┐
   │ PostgreSQL (Relational)│           │    MongoDB (NoSQL)    │
   └───────────┬───────────┘           └───────────┬───────────┘
               │ (Logical Replication)             │ (Replica Set Oplog)
               ▼                                   ▼
      ┌─────────────────┐                 ┌─────────────────┐
      │ Debezium Source │                 │ Debezium Source │
      └────────┬────────┘                 └────────┬────────┘
               │                                   │
               ▼                                   ▼
      ┌─────────────────────────────────────────────────────┐
      │                Apache Kafka (KRaft)                 │
      └──────────────────────────┬──────────────────────────┘
                                 │
                                 ▼ (S3 Sink Connector)
      ┌─────────────────────────────────────────────────────┐
      │     MinIO S3 Object Storage (Bronze - Raw JSON)     │
      └──────────────────────────┬──────────────────────────┘
                                 │
       ┌─────────────────────────┼─────────────────────────┐
       │ (Đọc dữ liệu thô)       │ (Query trực tiếp)       │
       ▼                         ▼                         │
┌──────────────┐         ┌──────────────┐                  │
│ PySpark Job  │         │  ClickHouse  │                  │
│ (Silver/Gold)│         │   Raw DB     │                  │
└──────┬───────┘         │ (S3 Engine)  │                  │
       │                 └──────────────┘                  │
       │ (Ghi qua JDBC)                                    │
       ▼                                                   ▼
┌──────────────────────────────────────────────────────────┐
│             ClickHouse Analytics Database                │
│   - Tầng Silver (Dữ liệu sạch & chuẩn hóa MergeTree)      │
│   - Tầng Gold (Dữ liệu tổng hợp, tối ưu chỉ mục)         │
└──────────────────────────────────────────────────────────┘
```

---

## 📁 Cấu trúc Thư mục Dự án

```text
├── data/                           # Thư mục chứa các tệp CSV gốc (Không đẩy lên Git)
│   ├── users.csv
│   ├── cards.csv
│   └── transactions.csv
├── dags/                           # Airflow DAGs điều phối hệ thống
│   ├── generate_data_dag.py        # DAG giả lập sinh dữ liệu live (chạy mỗi 5 phút)
│   └── spark_orchestration_dag.py  # DAG submit các PySpark Jobs (chạy mỗi 30 phút)
├── spark_apps/                     # Scripts xử lý dữ liệu PySpark
│   ├── transform_silver.py         # Đọc Bronze (MinIO) -> Làm sạch & Ép kiểu -> Silver ClickHouse
│   └── transform_gold.py           # Đọc Silver -> Phép Join nghiệp vụ -> Gold ClickHouse
├── scripts/                        # Các helper scripts phục vụ vận hành
│   ├── init_clickhouse.py          # Khởi tạo DB raw/analytics & tối ưu chỉ mục ClickHouse
│   ├── generate_live_data.py       # Script chạy tay giả lập giao dịch từ host
│   ├── register_connectors.py      # Tự động đăng ký Debezium và S3 Sink với Kafka Connect
│   ├── import_users.py             # Nạp khách hàng ban đầu từ CSV vào database
│   ├── import_cards.py             # Nạp thẻ ngân hàng ban đầu
│   └── import_transactions.py      # Nạp giao dịch lịch sử ban đầu
├── infrastructure/                 # Hạ tầng Docker stack
│   ├── docker-compose.yml          # Định nghĩa toàn bộ 9 dịch vụ trong stack
│   ├── connect/                    # Dockerfile cho Kafka Connect (tải plugins)
│   └── connectors/                 # Cấu hình JSON đăng ký Kafka Connectors
├── init_db.py                      # Khởi tạo Postgres tables và Mongo collections trống
├── requirements.txt                # Thư viện Python chạy tại máy Host
└── README.md                       # Tài liệu hướng dẫn sử dụng
```

---

## 🛠️ Chi tiết các Dịch vụ Hạ tầng (Docker Services)

Hệ thống chạy trên Docker với 9 dịch vụ liên kết chặt chẽ:
1. **`postgres` (cổng 5434):** Cơ sở dữ liệu nghiệp vụ chính (`banking_core`), bật logical replication.
2. **`mongo` (cổng 27017):** Kho lưu trữ phi cấu trúc (`banking_events`), bật Replica Set (`rs0`) để phục vụ CDC.
3. **`kafka` (cổng 9092):** Message queue trung chuyển dữ liệu dạng sự kiện (Event Streaming).
4. **`connect` (cổng 8083):** Kafka Connect tích hợp Debezium Postgres/Mongo Source và S3 Sink.
5. **`minio` (cổng 9005 API / 9001 Web UI):** Hồ lưu trữ đối tượng chứa dữ liệu Bronze thô.
6. **`clickhouse` (cổng 8123 HTTP / 9000 TCP):** Data Warehouse phục vụ truy vấn phân tích tốc độ cao.
7. **`spark` (Local Mode):** Động cơ tính toán phân tán chạy các biến đổi PySpark.
8. **`airflow-webserver` & `airflow-scheduler` (cổng 8080):** Bộ điều phối và giám sát toàn bộ pipeline.

---

## ⚡ Thiết kế Chỉ mục Vật lý ClickHouse (Lớp Silver & Gold)

Tất cả các bảng trong database `analytics` đều được cấu hình tối ưu hóa bằng **MergeTree Engine** kèm theo các khóa chỉ mục sắp xếp (`ORDER BY`) và tính năng nullable key để đạt tốc độ truy vấn phân tích tối đa:

* **`silver_postgres_customers`:** `ORDER BY id`
* **`silver_postgres_cards`:** `ORDER BY (customer_id, card_index)`
* **`silver_postgres_transactions`:** `ORDER BY (card_id, transaction_date)`
* **`silver_mongo_login_events` / `silver_mongo_device_events`:** `ORDER BY (user_id, timestamp)`
* **`silver_mongo_fraud_events`:** `ORDER BY (customer_id, transaction_date)`
* **`silver_mongo_notification_logs`:** `ORDER BY (customer_id, timestamp)`
* **`gold_fraud_analysis`:** `ORDER BY (customer_id, transaction_date)`
* **`gold_user_behavior_summary`:** `ORDER BY customer_id`
* **Table Settings:** Bật `SETTINGS allow_nullable_key = 1` giúp ClickHouse chấp nhận các cột `Nullable` làm khóa sắp xếp sơ cấp.

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
docker compose -f infrastructure/docker-compose.yml up -d
```
*Đợi khoảng 30-45 giây để Kafka Connect, MongoDB, PostgreSQL, MinIO và Airflow khởi động hoàn chỉnh.*

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
Đăng ký các Kafka Connectors để bắt đầu truyền dữ liệu tự động xuống MinIO:
```bash
python scripts/register_connectors.py
```
*Bạn có thể truy cập http://localhost:9001 (minio_admin/minio_password) để kiểm tra các file JSON thô bắt đầu xuất hiện trong bucket `banking-lakehouse`.*

### 5. Khởi tạo Bảng ClickHouse
Tạo các database `raw`, `analytics` và toàn bộ 21 bảng cấu hình tối ưu chỉ mục:
```bash
python scripts/init_clickhouse.py
```

### 6. Quản lý trên Airflow Webserver
Truy cập **[http://localhost:8080](http://localhost:8080)** (Tài khoản: `admin` / Mật khẩu: `admin`):
* Bật (Unpause) DAG **`live_data_generator_dag`** để bắt đầu sinh giao dịch live ngẫu nhiên vào hệ thống mỗi 5 phút.
* Bật (Unpause) DAG **`lakehouse_spark_orchestration`** để bắt đầu chạy các tác vụ PySpark biến đổi dữ liệu thô (Bronze -> Silver -> Gold) ghi vào ClickHouse định kỳ.

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
