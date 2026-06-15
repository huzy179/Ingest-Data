# KỊCH BẢN PHÂN CHIA THUYẾT TRÌNH NHÓM 3 NGƯỜI
## Đề tài: Real-Time & Batch Hybrid Data Lakehouse Pipeline

Để buổi thuyết trình đạt kết quả cao nhất, nội dung được chia đều theo mô hình: **Nạp dữ liệu (Ingestion) $\rightarrow$ Lưu trữ & Xử lý (Processing) $\rightarrow$ Phục vụ & Vận hành (Serving & Production)**.

---

### 👤 NGƯỜI 1: TẦNG NGUỒN & THU THẬP DỮ LIỆU (SOURCES & INGESTION LAYER)
* **Thời lượng đề xuất:** 30% thời gian.
* **Vai trò trong nhóm:** Data Ingestion Engineer.
* **Nội dung thuyết trình chính:**

1.  **Đặt vấn đề & Tầng Nguồn (Source Layer):**
    *   Giới thiệu bài toán thực tế của hệ thống ngân hàng: Dữ liệu bị phân mảnh giữa **SQL (PostgreSQL)** lưu thông tin thẻ/giao dịch chính thức và **NoSQL (MongoDB)** lưu log thiết bị, log đăng nhập, SMS thông báo.
    *   Mục tiêu: Cần hợp nhất hai nguồn dữ liệu này để phân tích mà không gây ảnh hưởng đến hiệu năng hệ thống app đang chạy (OLTP).
2.  **Luồng Nạp lô (Batch Ingestion):**
    *   Giải thích cơ chế nạp Batch sử dụng **Apache Airflow** điều phối các job PySpark.
    *   Tác vụ `batch_ingest_postgres.py` (qua JDBC) và `batch_ingest_mongodb.py` (qua Mongo-Spark Connector).
    *   Điểm nhấn thực tế: Dữ liệu thô (Bronze) được ghi xuống MinIO dưới định dạng **Parquet** tối ưu và được **phân vùng (Partition) theo ngày nạp (`load_date=YYYY-MM-DD`)** để đảm bảo tính bất biến (Immutable Archive).
3.  **Luồng Nạp thời gian thực (Streaming CDC):**
    *   Giải thích cơ chế hoạt động của **CDC (Change Data Capture)** thông qua **Debezium Connectors** (đọc trực tiếp WAL log của Postgres và Change Streams của MongoDB).
    *   Tại sao dữ liệu đẩy lên Kafka được mã hóa dạng **Avro nhị phân** kết hợp **Confluent Schema Registry**? (Giúp nén dữ liệu đến 80% và tự động quản lý cấu trúc bảng khi DB nguồn thay đổi - Schema Evolution).
    *   **Cold Storage:** Đăng ký Confluent S3 Sink để chuyển Avro thành file Parquet thô lưu trữ vĩnh viễn trên MinIO (Bronze Layer).

---

### 👤 NGƯỜI 2: TẦNG LƯU TRỮ & XỬ LÝ DỮ LIỆU (STORAGE & PROCESSING LAYER)
* **Thời lượng đề xuất:** 35% thời gian.
* **Vai trò trong nhóm:** Data Platform & Processing Engineer.
* **Nội dung thuyết trình chính:**

1.  **Kiến trúc Medallion (Bronze $\rightarrow$ Silver $\rightarrow$ Gold) trên MinIO:**
    *   Giải thích các tầng dữ liệu: Bronze (dữ liệu thô Parquet), Silver (dữ liệu đã làm sạch, chuẩn hóa kiểu dữ liệu), Gold (dữ liệu tổng hợp nghiệp vụ).
2.  **Xử lý dữ liệu Stream từ Kafka Broker:**
    *   Giải thích tại sao Spark Structured Streaming (`stream_transform_silver.py`) **không** đọc file từ MinIO mà đọc trực tiếp từ Kafka Broker (`spark.readStream.format("kafka")`) để giảm thiểu tối đa độ trễ (latency).
    *   Chi tiết kỹ thuật: Giải mã dữ liệu Avro trên Spark bằng cách cắt bỏ 5-byte header của Confluent (magic byte + schema ID) rồi gọi hàm `from_avro`.
3.  **Xây dựng tầng Silver với Delta Lake:**
    *   Tại sao lại chọn **Delta Lake** thay vì Parquet thông thường cho tầng Silver? (Hỗ trợ ACID Transactions để tránh lỗi ghi dở dang, hỗ trợ kiểm soát phiên bản dữ liệu).
    *   **Giải quyết xung đột Batch & Stream (Delta Merge):** Giải thích cách viết hàm `DeltaTable.merge()` để thực hiện **Upsert (Merge)** dựa trên khóa chính (`id` hoặc `_id`). Nhờ đó, cả luồng batch và stream cùng đổ vào 1 bảng Silver Delta mà không bao giờ bị trùng lặp dữ liệu.
    *   Xử lý sự kiện xóa vật lý từ nguồn (Delete CDC).

---

### 👤 NGƯỜI 3: TẦNG PHỤC VỤ TRUY VẤN (SERVING LAYER) & VẬN HÀNH THỰC TẾ
* **Thời lượng đề xuất:** 35% thời gian.
* **Vai trò trong nhóm:** BI & Analytics Engineer / Infrastructure Lead.
* **Nội dung thuyết trình chính:**

1.  **Thiết kế Tầng Serving (ClickHouse):**
    *   **Tận dụng ClickHouse DeltaLake Engine ở tầng Silver:** Giải thích điểm sáng kiến trúc: ClickHouse tạo bảng ngoài trỏ trực tiếp đến thư mục Delta trên MinIO. Spark chỉ cần ghi vào Delta Lake, ClickHouse tự động đọc mà không cần ghi JDBC $\rightarrow$ Loại bỏ hoàn toàn bottleneck dual-sink.
    *   **Tầng Gold vật lý:** Giải thích job `transform_gold.py` đọc từ Silver Delta Lake, thực hiện các phép Join nghiệp vụ và ghi kết quả vào ClickHouse `MergeTree` vật lý để phục vụ dashboard BI siêu tốc.
2.  **Các cải tiến cho môi trường Production thực tế:**
    *   **Compaction:** Chạy job định kỳ `OPTIMIZE` Delta table để giải quyết lỗi quá nhiều file nhỏ (Small File Problem).
    *   **ClickHouse Materialized Views:** Tạo view vật lý đồng bộ từ Delta sang MergeTree SSD cho các bảng query tần suất cao.
    *   **Data Quality & Security:** Sử dụng kiểm tra chất lượng dữ liệu (DLQ); Mã hóa lưu trữ MinIO; Thực hiện băm (Hashing SHA-256/Masking) thông tin nhạy cảm của khách hàng (PII) ngay tại Silver layer.
    *   **Hạ tầng HA:** Đề xuất mô hình 3 Kafka brokers, Cluster Schema Registry.
3.  **Demo & Kết quả phân tích (ClickHouse SQL):**
    *   Show kết quả chạy query phân tích gian lận (Gold Table) trên Clickhouse Play UI.
    *   Show kết quả tổng hợp hành vi người dùng (Gold Table).
