# Kế hoạch Triển khai Chi tiết: Hệ thống CDC Ingestion (PostgreSQL + MongoDB -> ClickHouse)

Tài liệu này trình bày kế hoạch từng bước để xây dựng hệ thống thu thập và xử lý dữ liệu thời gian thực (CDC) từ PostgreSQL và MongoDB vào ClickHouse, có tích hợp dbt và Airflow.

---

## 📌 Tổng quan luồng dữ liệu (Data Flow)

1. **Sinh dữ liệu gốc (Source):** Dữ liệu giao dịch được ghi vào **PostgreSQL** trước.
2. **Biến tấu/Đồng bộ nguồn:** Một backend script/service (FastAPI) hoặc Simulator script (`generate_live_data.py`) đọc dữ liệu, chuyển đổi cấu trúc (ví dụ: chuyển từ dạng bảng sang dạng document JSON lồng nhau) rồi lưu vào **MongoDB**.
3. **Giả lập dữ liệu liên tục:** Sử dụng script `generate_live_data.py` để sinh giao dịch mới, sự kiện đăng nhập, sự kiện thiết bị, thông báo,... liên tục nhằm mô phỏng dòng dữ liệu thời gian thực của môi trường production.
4. **Change Data Capture (CDC):**
   - **Debezium Postgres Connector** bắt các sự kiện thay đổi (Insert/Update/Delete) từ WAL của PostgreSQL và đẩy vào Kafka.
   - **Debezium MongoDB Connector** bắt các sự kiện thay đổi từ Change Streams của MongoDB và đẩy vào Kafka.
5. **Ingestion (Nạp dữ liệu):** **Kafka Connect ClickHouse Sink** tự động kéo dữ liệu từ các topics trong Kafka về các bảng thô (raw tables) trong ClickHouse.
6. **Transformation (Biến đổi):** **dbt** (dbt-clickhouse) chạy các model SQL để làm sạch, trích xuất dữ liệu JSON và thực hiện `JOIN` dữ liệu từ các nguồn thành các bảng Analytics hoàn chỉnh.
7. **Orchestration (Điều phối):** **Apache Airflow** lập lịch trigger dbt chạy định kỳ.

---

## 🛠️ Kế hoạch thực hiện từng bước

### Bước 1: Khởi dựng Hạ tầng Docker Compose (Local)
* **Mục tiêu:** Cài đặt toàn bộ môi trường chạy local để kiểm thử.
* **Chi tiết công việc:**
  1. Tạo file `docker-compose.yml` định nghĩa các dịch vụ:
     - PostgreSQL (bật logical replication: `wal_level = logical`).
     - MongoDB (cấu hình dưới dạng Single-Node Replica Set để bật Change Streams).
     - Kafka & Zookeeper (Broker quản lý tin nhắn).
     - Kafka Connect (Dockerfile cài Debezium Postgres, Debezium MongoDB và ClickHouse Sink Connector).
     - ClickHouse Server (Data Warehouse).
     - Apache Airflow (Dùng chế độ `standalone` để tiết kiệm RAM tối đa khi chạy local).

### Bước 2: Nạp dữ liệu tĩnh & Giả lập dòng dữ liệu Live
* **Mục tiêu:** Thiết lập dữ liệu nguồn sạch và xây dựng bộ mô phỏng dữ liệu thay đổi liên tục.
* **Chi tiết công việc:**
  1. Viết và chạy `init_db.py` để khởi tạo cấu trúc bảng PostgreSQL (`customers`, `cards`, `transactions`) và các bộ sưu tập MongoDB (`customers`, `login_events`, `fraud_events`, `audit_logs`, `notification_logs`, `device_events`).
  2. Viết các script nhập dữ liệu sạch (`import_users.py`, `import_cards.py`, `import_transactions.py`) từ CSV thô.
  3. Viết script mô phỏng `generate_live_data.py` để tự động sinh giao dịch mới và các sự kiện đi kèm (đăng nhập, tương tác thiết bị, SMS cảnh báo,...) theo thời gian thực hoặc theo lô nhằm liên tục làm mới dữ liệu nguồn.

### Bước 3: Cấu hình và Kích hoạt Debezium CDC Connectors
* **Mục tiêu:** Đưa dữ liệu thay đổi từ các bảng Postgres và collection Mongo lên Kafka topics.
* **Chi tiết công việc:**
  1. Đăng ký Connector PostgreSQL với Kafka Connect qua REST API. Sử dụng Single Message Transform (SMT) `ExtractNewRecordState` để chuyển đổi cấu trúc thông điệp về dạng gọn nhẹ.
  2. Đăng ký Connector MongoDB với Kafka Connect.
  3. Xác minh các topics trong Kafka đã tự động nhận tin nhắn mới mỗi khi chạy script giả lập `generate_live_data.py`.

### Bước 4: Thiết lập Ingest từ Kafka vào ClickHouse Raw Tables
* **Mục tiêu:** Đổ tự động dữ liệu từ Kafka vào ClickHouse.
* **Chi tiết công việc:**
  1. Khởi tạo database `raw` và các bảng thô tương ứng với cấu trúc Postgres (`raw.postgres_customers`, `raw.postgres_cards`, `raw.postgres_transactions`) và MongoDB (`raw.mongodb_customers`, `raw.mongodb_login_events`, `raw.mongodb_fraud_events`,...) trong ClickHouse.
  2. Đăng ký ClickHouse Sink Connector trên Kafka Connect để tự động kéo dữ liệu từ Kafka topics về các bảng thô ClickHouse.
  3. Cấu hình batching (gom cụm 5-10 giây) để tối ưu hiệu năng ghi của ClickHouse.

### Bước 5: Cấu hình và Viết dự án dbt (dbt-clickhouse)
* **Mục tiêu:** Biến đổi dữ liệu thô thành các bảng Analytics phục vụ BI.
* **Chi tiết công việc:**
  1. Khởi tạo dự án dbt mới trong thư mục `dbt_project/`.
  2. Cấu hình file `profiles.yml` kết nối với ClickHouse.
  3. Viết staging models để chuẩn hóa kiểu dữ liệu thô (ví dụ: bóc tách dữ liệu JSON lồng nhau từ MongoDB).
  4. Viết marts model thực hiện JOIN dữ liệu từ Postgres và MongoDB để phân tích mối tương quan giữa giao dịch, hành vi đăng nhập và thiết bị của khách hàng.

### Bước 6: Điều phối tự động bằng Apache Airflow
* **Mục tiêu:** Lập lịch tự động chạy dbt.
* **Chi tiết công việc:**
  1. Viết một Airflow DAG để trigger lệnh `dbt run` và `dbt test` trong dự án dbt định kỳ.
  2. Kiểm tra xem dữ liệu báo cáo trên ClickHouse được cập nhật tự động khi Airflow DAG chạy.

---

## 📈 Kế hoạch Xác minh (Verification Plan)
1. **Kiểm tra chèn dữ liệu live:** Chạy script `generate_live_data.py` và theo dõi sự thay đổi dòng ghi trong Postgres và MongoDB.
2. **Kiểm tra Kafka topics:** Xem các topic có cập nhật tin nhắn thay đổi tương ứng thời gian thực không.
3. **Kiểm tra ClickHouse Raw:** Chạy query SELECT bảng thô trên ClickHouse xem dữ liệu đã tự động đồng bộ về chưa.
4. **Kiểm tra dbt & Airflow:** Kích hoạt Airflow DAG chạy dbt và kiểm chứng kết quả trong bảng phân tích cuối cùng.
