# TÀI LIỆU PHÂN TÍCH KIẾN TRÚC & LỰA CHỌN CÔNG NGHỆ DỰ ÁN
## Real-Time & Batch Hybrid Data Lakehouse Pipeline

Tài liệu này phân tích chi tiết về kiến trúc hệ thống, lý do cần thiết, các luồng dữ liệu và lý do lựa chọn các công nghệ cụ thể trong dự án tích hợp dữ liệu từ **PostgreSQL + MongoDB** qua **Debezium, Kafka, MinIO, Spark** đến **ClickHouse**.

---

## 1. Sơ đồ Luồng Dữ liệu Chi tiết (Detailed Data Flow Diagram)

Hệ thống được thiết kế tích hợp chặt chẽ giữa hai luồng xử lý dữ liệu: **Real-time (Streaming)** qua CDC và **Batch (Theo lô)** qua JDBC/Mongo Connector, tổ chức dữ liệu theo kiến trúc **Medallion (Bronze -> Silver -> Gold)**.

### 1.1. Sơ đồ kiến trúc Mermaid

```mermaid
flowchart TD
    subgraph Sources ["1. TẦNG NGUỒN (OPERATIONAL DATABASES)"]
        PG[(PostgreSQL\n- banking_core\n- WAL: logical)]
        MG[(MongoDB\n- banking_events\n- replica set: rs0)]
    end

    subgraph Streaming_Ingest ["2. NẠP DỮ LIỆU CDC & COLD STORAGE"]
        DebeziumPG[Debezium Postgres Connector\n- SMT: ExtractNewRecordState]
        DebeziumMG[Debezium MongoDB Connector\n- SMT: ExtractNewDocumentState]
        SR[Confluent Schema Registry\n- Port 8081]
        Kafka{Apache Kafka Broker\n- Port 9092\n- Avro Serialized}
        S3Sink[Confluent S3 Sink Connector]
        
        PG -->|Logical WAL Log| DebeziumPG
        MG -->|Replica Oplog Change| DebeziumMG
        DebeziumPG <-->|Đăng ký/Xác thực Schema| SR
        DebeziumMG <-->|Đăng ký/Xác thực Schema| SR
        DebeziumPG -->|Đẩy Avro Messages| Kafka
        DebeziumMG -->|Đẩy Avro Messages| Kafka
        
        %% Nhánh Cold Storage độc lập để lưu trữ Raw Archive
        Kafka -->|Lưu trữ thô/Backup| S3Sink
    end

    subgraph Batch_Ingest ["3. NẠP BATCH (BATCH INGESTION)"]
        Airflow[Apache Airflow Scheduler\n- Port 8080]
        SparkBatch[PySpark Ingestion Jobs\n- batch_ingest_postgres.py\n- batch_ingest_mongodb.py]
        
        Airflow -->|Trigger hàng ngày| SparkBatch
        PG -->|Đọc JDBC Driver\n- port 5432| SparkBatch
        MG -->|Đọc Mongo Spark Connector\n- port 27017| SparkBatch
    end

    subgraph Storage_Bronze_Silver ["4. TẦNG LƯU TRỮ VÀ XỬ LÝ (MINIO DATA LAKEHOUSE)"]
        MinIO[(MinIO Object Storage\n- Bucket: banking-lakehouse\n- Port 9005)]
        
        %% Bronze Layer
        subgraph Bronze_Layer ["Bronze Layer (Raw Archive)"]
            SparkBatch -->|Ghi đè file Parquet\n- batch/| MinIO
        end
        
        %% Silver Delta Tables
        subgraph Silver_Delta ["Silver Layer (Delta Tables)"]
            Delta[(MinIO Silver Delta Tables\n- silver/delta/)]
        end
    end

    subgraph Processing_Layer ["5. TẦNG TÍNH TOÁN SPARK (PROCESSING ENGINE)"]
        SparkStream[Spark Structured Streaming\n- stream_transform_silver.py\n- Chạy liên tục]
        SparkBatchTrans[Spark Batch Transformations\n- transform_silver.py\n- transform_gold.py]
        
        %% Spark Streaming consume TRỰC TIẾP từ Kafka
        Kafka -->|Đọc Stream trực tiếp\n- spark.readStream.format kafka| SparkStream
        
        %% Spark Batch đọc từ Bronze thô
        MinIO -->|Đọc Batch Parquet| SparkBatchTrans
        
        %% Spark Stream & Batch chỉ ghi xuống Silver Delta (Không ghi ClickHouse JDBC để tránh bottleneck)
        SparkStream -->|Ghi Delta format| Delta
        SparkBatchTrans -->|Ghi Delta format| Delta
        
        %% Tầng Gold (transform_gold.py) đọc dữ liệu nguồn từ Silver Delta
        Delta -->|Đọc Silver Delta làm đầu vào| SparkBatchTrans
    end

    subgraph Serving_Layer ["6. TẦNG PHỤC VỤ TRUY VẤN (SERVING - CLICKHOUSE)"]
        CH[(ClickHouse Server\n- Port 8123/9000)]
        
        %% ClickHouse trỏ trực tiếp Delta Lake qua DeltaLake Table Engine
        Delta -.->|Đọc trực tiếp qua\nDeltaLake Table Engine| CH
        
        %% Spark chỉ ghi JDBC cho tầng Gold kết quả phục vụ BI
        SparkBatchTrans -->|Ghi qua ClickHouse JDBC\n- Gold Tables| CH
    end

    classDef pgStyle fill:#336791,stroke:#fff,stroke-width:2px,color:#fff;
    classDef mgStyle fill:#4db33d,stroke:#fff,stroke-width:2px,color:#fff;
    classDef kafkaStyle fill:#000,stroke:#fff,stroke-width:2px,color:#fff;
    classDef sparkStyle fill:#e25a2a,stroke:#fff,stroke-width:2px,color:#fff;
    classDef chStyle fill:#fc0,stroke:#111,stroke-width:2px,color:#111;
    classDef minioStyle fill:#c7254e,stroke:#fff,stroke-width:2px,color:#fff;
    
    class PG pgStyle;
    class MG mgStyle;
    class Kafka kafkaStyle;
    class SparkBatch,SparkStream,SparkBatchTrans sparkStyle;
    class CH chStyle;
    class MinIO Delta minioStyle;
```

---

### 1.2. Đặc tả chi tiết từng bước chuyển dịch dữ liệu

#### **A. Luồng Streaming CDC (Đường dẫn Real-time)**
1.  **Sự kiện nguồn:** Người dùng thực hiện một hành động (ví dụ: thực hiện giao dịch ghi vào PostgreSQL, hoặc một thiết bị gửi log đăng nhập ghi vào MongoDB).
2.  **Bắt sự kiện (CDC):**
    *   **PostgreSQL:** Debezium Postgres Source Connector đọc tập tin ghi chép WAL (`wal_level=logical`) qua plugin `pgoutput`. Sử dụng Single Message Transform (SMT) `ExtractNewRecordState` để bóc tách trạng thái dòng mới chèn/cập nhật.
    *   **MongoDB:** Debezium MongoDB Source Connector bắt stream thay đổi từ Oplog của replica set `rs0` qua SMT `ExtractNewDocumentState`.
3.  **Đăng ký Schema:** Cả hai Connectors gửi cấu hình Schema của dòng dữ liệu đến **Confluent Schema Registry** (port `8081`) để lấy mã Schema ID.
4.  **Truyền tải (Kafka):** Dữ liệu được mã hóa ở dạng **Avro nhị phân** siêu nén và gửi vào các topic tương ứng trong Kafka (ví dụ: `postgres.public.transactions`).
5.  **Ghi lưu trữ thô (Cold Storage):** **Confluent S3 Sink Connector** liên tục tiêu thụ dữ liệu từ các topics của Kafka, giải mã cấu trúc dữ liệu qua Schema Registry, sau đó lưu trữ xuống **MinIO Object Storage** dưới dạng các tệp **Parquet** nén theo cột tại đường dẫn `s3a://banking-lakehouse/topics/`. Đây đóng vai trò là kho **Raw Archive (Backup/Cold Storage)** để phục vụ kiểm toán hoặc replay khi cần thiết.
6.  **Xử lý thời gian thực (Spark Streaming):** Container `banking-spark-streaming` chạy ứng dụng Spark Structured Streaming (`stream_transform_silver.py`) **consume trực tiếp từ Kafka Broker** thông qua giao thức Kafka (`spark.readStream.format("kafka")`):
    *   Đọc luồng dữ liệu thời gian thực từ các topic trực tiếp.
    *   Giải mã dữ liệu Avro nhị phân sử dụng Spark-Avro (`from_avro` function) kết hợp cắt bỏ 5-byte header của Confluent.
    *   Ép kiểu dữ liệu, chuẩn hóa thời gian và bóc tách các trường JSON.
    *   **Ghi một nơi (Single-Sink):** Để tối ưu hóa tài nguyên mạng và tránh hiện tượng nghẽn (backpressure) của JDBC ClickHouse, Spark Streaming chỉ thực hiện ghi dữ liệu sạch trực tiếp xuống các bảng **Delta Lake** trong phân vùng Silver trên MinIO (`s3a://banking-lakehouse/silver/delta/<table_name>/`).
    *   **Truy vấn (ClickHouse Serving):** ClickHouse tận dụng thế mạnh của mình bằng cách tạo ra các **External Tables** sử dụng `DeltaLake` engine trỏ trực tiếp đến thư mục Delta của MinIO để phục vụ phân tích thời gian thực với hiệu năng tối đa mà không gây tải thêm cho Spark.

#### **B. Luồng Ingestion Batch (Đường dẫn Batch)**
1.  **Lập lịch (Airflow):** Apache Airflow định kỳ kích hoạt DAG `lakehouse_batch_ingestion`.
2.  **Trích xuất (PySpark Batch Ingest):**
    *   Job `batch_ingest_postgres.py` kết nối trực tiếp đến Postgres bằng JDBC để lấy toàn bộ dữ liệu hoặc dữ liệu gia tăng.
    *   Job `batch_ingest_mongodb.py` kết nối trực tiếp đến Mongo bằng `Mongo Spark Connector`.
3.  **Lưu trữ thô (MinIO Bronze Batch):** Spark ghi đè (mode `overwrite`) dữ liệu dưới dạng Parquet thô vào MinIO tại đường dẫn: `s3a://banking-lakehouse/batch/<database_type>/<table_name>/`.
4.  **Xử lý tổng hợp (Spark Batch Transform):** Airflow trigger DAG `lakehouse_spark_orchestration`:
    *   `transform_silver.py` đọc từ thư mục batch trong MinIO, chuẩn hóa dữ liệu và chỉ ghi vào Delta Lake Silver trên MinIO (ClickHouse Silver tự động tham chiếu dữ liệu mới này thông qua `DeltaLake` engine).
    *   **Tạo Tầng Gold:** Job `transform_gold.py` đọc dữ liệu đầu vào trực tiếp từ **Tầng Delta Lake Silver trên MinIO** (thành phần duy nhất làm Source of Truth). Sau khi thực hiện các phép Join và tổng hợp chỉ số nghiệp vụ, kết quả Gold được ghi vào các bảng Gold vật lý của ClickHouse (qua JDBC) phục vụ trực tiếp cho BI Dashboard và người dùng cuối.


---

## 2. Tại sao lại cần hệ thống này? (Business & Technical Drivers)

Trong kỷ nguyên số, dữ liệu của một doanh nghiệp (đặc biệt là ngành tài chính/ngân hàng) không nằm ở một nơi duy nhất và có các nhu cầu khai thác khác nhau:

1.  **Sự phân mảnh dữ liệu (SQL vs. NoSQL):**
    *   **PostgreSQL (SQL):** Lưu trữ dữ liệu cấu trúc có tính nhất quán cao (ACID) như thông tin khách hàng, tài khoản thẻ, và các giao dịch tài chính chính thức.
    *   **MongoDB (NoSQL):** Lưu trữ dữ liệu bán cấu trúc, tần suất ghi cao và có cấu trúc linh hoạt như nhật ký thiết bị (device events), log đăng nhập (login events), SMS thông báo, và log cảnh báo gian lận.
    *   *Vấn đề:* Để có bức tranh toàn cảnh (ví dụ: phát hiện một giao dịch có phải gian lận hay không dựa trên thiết bị đăng nhập và vị trí), ta bắt buộc phải liên kết (Join) hai nguồn dữ liệu này lại với nhau.
2.  **Yêu cầu xử lý Thời gian thực (Real-time) kết hợp Lô (Batch):**
    *   **Thời gian thực:** Cần phát hiện gian lận (Fraud Detection) ngay khi giao dịch vừa xảy ra để ngăn chặn tổn thất.
    *   **Batch (Theo lô):** Cần tổng hợp báo cáo tài chính cuối ngày/tháng, phân tích hành vi người dùng dài hạn.
3.  **Tránh ảnh hưởng hệ thống vận hành (OLTP):**
    *   Nếu trực tiếp chạy các câu lệnh query phân tích phức tạp (như Join bảng hàng triệu dòng) trực tiếp trên PostgreSQL và MongoDB đang phục vụ khách hàng, hệ thống app của người dùng sẽ bị treo hoặc chậm nghiêm trọng. Hệ thống này giúp tách biệt hoàn toàn môi trường vận hành (OLTP) và môi trường phân tích (OLAP).

---

## 3. Tại sao lại lựa chọn kiến trúc và công nghệ này?

### 3.1. Kênh Thu thập Dữ liệu (Ingestion Layer)

#### **Change Data Capture (CDC) với Debezium & Kafka**
*   **Tại sao chọn Debezium?** Debezium hoạt động bằng cách đọc trực tiếp log ghi chép thay đổi của hệ quản trị cơ sở dữ liệu (WAL của Postgres và Oplog của MongoDB) thay vì chạy các lệnh `SELECT` tuần tự. Điều này giúp:
    *   **Độ trễ gần như bằng 0 (Near-realtime).**
    *   **Không gây tải cho Database nguồn.**
    *   **Bắt được mọi sự kiện biến động (kể cả sự kiện DELETE vật lý).**
*   **Tại sao chọn Apache Kafka?** Kafka đóng vai trò là một "bộ đệm trung chuyển" (Message Broker) chịu tải cực cao. Nó giúp cô lập các ứng dụng nguồn khỏi các ứng dụng đích (Decoupling). Nếu hệ thống ClickHouse hoặc Spark gặp sự cố tạm thời, dữ liệu vẫn an toàn nằm trong Kafka và sẽ được xử lý tiếp khi hệ thống hoạt động trở lại mà không lo mất mát dữ liệu.
*   **Tại sao chọn Avro + Confluent Schema Registry?**
    *   Tiết kiệm băng thông và dung lượng lưu trữ (Avro nén dữ liệu nhị phân không kèm key trong message).
    *   Quản lý vòng đời cấu trúc bảng (Schema Evolution) chặt chẽ, ngăn ngừa lỗi sập pipeline khi DB nguồn thay đổi cấu trúc bảng.

---

### 3.2. Kênh Lưu trữ thô (Bronze Layer)

#### **MinIO Object Storage**
*   **Tại sao dùng MinIO thay vì ghi trực tiếp vào ClickHouse?**
    *   **Khả năng Replay (Chạy lại dữ liệu):** Kafka thường cấu hình thời gian lưu giữ ngắn (ví dụ: 7 ngày). Nếu logic nghiệp vụ thay đổi và bạn cần tính toán lại dữ liệu của 1 năm trước, bạn không thể tìm lại trên Kafka. MinIO đóng vai trò là kho lưu trữ dữ liệu thô (Bronze Layer) vĩnh viễn với chi phí siêu rẻ.
    *   **Sử dụng chuẩn S3 API:** Cho phép dễ dàng chuyển đổi hoặc mở rộng lên AWS S3, Google Cloud Storage sau này mà không cần sửa code.
    *   **Bảo toàn dữ liệu gốc (Immutable Raw Data):** Dữ liệu thô từ nguồn được giữ nguyên trạng, tránh việc lỗi logic biến đổi làm hỏng dữ liệu gốc.
*   **Tại sao lưu dạng Parquet?** Parquet là định dạng lưu trữ dạng cột (Columnar format) được nén rất tốt, giảm dung lượng MinIO và giúp Spark đọc dữ liệu nhanh hơn gấp nhiều lần so với JSON/CSV thông thường.

---

### 3.3. Kênh Tính toán & Xử lý (Processing Layer)

#### **Apache Spark (PySpark & Structured Streaming)**
*   **Tại sao chọn Apache Spark?**
    *   **Hỗ trợ hợp nhất (Unified Engine):** Spark cung cấp cùng một tập API (DataFrame) cho cả xử lý theo lô (Batch) và xử lý luồng (Structured Streaming). Lập trình viên chỉ cần viết code một lần và có thể chạy được cho cả 2 luồng.
    *   **Xử lý phân tán (Distributed Computing):** Đảm bảo khả năng mở rộng ngang (Scale-out) khi dung lượng dữ liệu tăng lên hàng Terabyte.
    *   **Cơ chế Checkpoint mạnh mẽ:** Đảm bảo nguyên tắc xử lý chính xác một lần (**Exactly-once processing**) ngay cả khi hệ thống mạng hoặc node tính toán bị sập giữa chừng.
*   **Tại sao tích hợp Delta Lake?**
    *   Delta Lake cung cấp tính năng **ACID Transactions** trên hồ lưu trữ đối tượng (S3/MinIO). Điều này tránh tình trạng Spark đang ghi dữ liệu giữa chừng mà bị lỗi làm hỏng dữ liệu hoặc trả về kết quả sai lệch cho người đọc.
    *   Tính năng **Schema Enforcement & Evolution** ngăn chặn việc ghi sai định dạng dữ liệu vào Silver layer.

---

### 3.4. Kho Dữ liệu Phân tích (Serving Layer)

#### **ClickHouse**
*   **Tại sao chọn ClickHouse thay vì PostgreSQL làm Data Warehouse?**
    *   ClickHouse là hệ quản trị cơ sở dữ liệu hướng cột (**Column-oriented DBMS**) tối ưu hoàn toàn cho phân tích (OLAP).
    *   **Tốc độ truy vấn vượt trội:** Với các câu lệnh tổng hợp (Aggregate), ClickHouse nhanh hơn cơ sở dữ liệu dòng truyền thống (như Postgres/MySQL) từ **100 đến 1000 lần** nhờ cơ chế nén cột và tính toán vector hóa.
    *   **MergeTree Engine:** Hỗ trợ cơ chế sắp xếp vật lý thông minh giúp tăng tốc tối đa các truy vấn phân tích theo thời gian và khách hàng.

---

### 3.5. Hệ thống Điều phối (Orchestration Layer)

#### **Apache Airflow**
*   **Tại sao chọn Airflow?**
    *   Quản lý luồng công việc dưới dạng code (Workflow-as-Code) trực quan qua DAGs.
    *   Tự động quản lý phụ thuộc giữa các tác vụ (Ví dụ: Chỉ chạy tổng hợp Gold Layer sau khi luồng Silver Layer hoàn thành chạy Batch).
    *   Cung cấp cơ chế tự động thử lại (Retry) và cảnh báo trực quan khi xảy ra lỗi.

---

## 4. So sánh hai Luồng: Batch và Streaming trong Dự án

| Tiêu chí so sánh | Luồng Batch Ingestion & Processing | Luồng Streaming Ingestion & Processing |
| :--- | :--- | :--- |
| **Cơ chế nạp (Ingestion)** | Quét định kỳ trực tiếp từ bảng/collection nguồn (Postgres JDBC / Mongo Client) | Đọc liên tục file log (WAL/Oplog) qua Debezium CDC đẩy lên Kafka |
| **Định dạng tại MinIO Bronze** | Parquet (.parquet) | Parquet (.parquet) |
| **Tần suất chạy** | Định kỳ (hàng giờ, hàng ngày) qua Airflow | Chạy liên tục 24/7 dưới dạng service |
| **Độ trễ (Latency)** | Cao (Minutes đến Hours) | Thấp (Seconds đến Milliseconds) |
| **Tải lên DB nguồn** | Có thể gây tải cao khi thực hiện `SELECT` toàn bộ bảng lớn | Cực kỳ nhẹ, không ảnh hưởng hiệu năng hoạt động của DB |
| **Mục đích sử dụng** | Đồng bộ dữ liệu lịch sử, báo cáo tài chính định kỳ, đối soát dữ liệu cuối ngày | Cảnh báo tức thời (giao dịch bất thường, thiết bị lạ), cập nhật dashboard thời gian thực |
| **Khả năng phục hồi lỗi** | Đơn giản chỉ cần chạy lại (re-run) task Airflow | Dựa vào Checkpoint và Transaction Logs của Delta Lake để xử lý tiếp |

---

## 5. Kết luận

Kiến trúc **Postgres/Mongo $\rightarrow$ CDC $\rightarrow$ Kafka $\rightarrow$ MinIO $\rightarrow$ Spark $\rightarrow$ ClickHouse** là sự kết hợp hoàn hảo giữa **độ trễ thấp** của luồng stream và **tính toàn vẹn dữ liệu** của luồng batch. Đây là kiến trúc chuẩn mực được các doanh nghiệp công nghệ lớn áp dụng để xây dựng hệ thống **Data Lakehouse** hiện đại, giúp doanh nghiệp đưa ra các quyết định kinh doanh chính xác và nhanh chóng nhất.

---

## 6. Các Thách thức Vận hành Thực tế & Giải pháp Thiết kế Chi tiết (Production Engineering & Data Integrity)

Khi đưa kiến trúc này vào môi trường Production của các ngân hàng thương mại lớn, có 5 vấn đề cốt lõi liên quan đến tính toàn vẹn dữ liệu, hiệu năng truy vấn và bảo mật cần được xử lý triệt để:

### 6.1. Xung đột ghi đồng thời (Concurrency) & Trùng lặp dữ liệu giữa luồng Batch và Stream
*   **Thách thức:** Cả hai luồng Spark Streaming (CDC từ Kafka) và Spark Batch (định kỳ nạp lại) cùng ghi vào một bảng Silver Delta. Nếu dùng `.mode("append")` thông thường, dữ liệu sẽ bị nhân đôi. Nếu dùng `.mode("overwrite")` trong luồng batch, dữ liệu real-time mới cập nhật từ CDC sẽ bị xóa sạch.
*   **Giải pháp thiết kế thực tế (Delta Upsert/Merge):**
    *   Sử dụng cú pháp **`MERGE INTO` (Upsert)** của Delta Lake trong cả hai luồng xử lý thông qua cơ chế `foreachBatch`.
    *   **Logic Spark xử lý:**
        ```python
        from delta.tables import DeltaTable

        def upsert_to_delta(micro_batch_df, batch_id, delta_path):
            # Khởi tạo đối tượng Delta Table từ đường dẫn
            spark = micro_batch_df.sparkSession
            if DeltaTable.isDeltaTable(spark, delta_path):
                delta_table = DeltaTable.forPath(spark, delta_path)
                
                # Thực hiện MERGE dựa trên khóa chính id và chỉ ghi đè khi dữ liệu nguồn mới hơn (check cột timestamp)
                delta_table.alias("target").merge(
                    source = micro_batch_df.alias("source"),
                    condition = "target.id = source.id"
                ).whenMatchedUpdate(
                    # Chỉ cập nhật nếu timestamp của bản ghi mới lớn hơn bản ghi cũ
                    condition = "source.created_at > target.created_at",
                    set = {col: f"source.{col}" for col in micro_batch_df.columns}
                ).whenNotMatchedInsertAll().execute()
            else:
                # Nếu bảng chưa tồn tại, thực hiện ghi lần đầu
                micro_batch_df.write.format("delta").mode("overwrite").save(delta_path)
        ```
    *   **Xử lý sự kiện DELETE (CDC):** Khi Debezium ghi nhận một dòng bị xóa (trường `op` = `"d"`), Spark Streaming sẽ chuyển đổi dòng đó thành một lệnh xóa vật lý tương ứng trên Delta Table thay vì chỉ ghi nhận các trường null.

### 6.2. Phân định ranh giới "Real-time Alert" và "Near Real-time Analytics"
*   **Làm rõ thiết kế:** Pipeline hiện tại (Kafka -> Spark Streaming -> Delta -> ClickHouse) thực chất là luồng **Near Real-time Analytics** (độ trễ từ 5-30 giây), tối ưu hóa để hiển thị dashboard giám sát gian lận hoặc đối soát dữ liệu nhanh. Luồng này **không** thể đáp ứng việc chặn giao dịch tức thì (inline transaction block) tại thời điểm khách hàng quẹt thẻ.
*   **Kiến trúc mở rộng khi có nhu cầu chặn giao dịch Real-time:**
    *   **Nhánh 1 (Serving/Analytics - Hiện tại):** Spark ghi vào Delta và phục vụ phân tích trên ClickHouse.
    *   **Nhánh 2 (Real-time Alerting - Inline):** Triển khai một service độc lập (như **Flink CEP** hoặc service Java/Go rule-engine) consume trực tiếp từ Kafka topic. Khi phát hiện `risk_score > 80`, lập tức đẩy tín hiệu chặn giao dịch về core banking qua gRPC/REST API trong thời gian < 100ms.

### 6.3. Đảm bảo tính Immutable (Bất biến) cho tầng Bronze Batch
*   **Thách thức:** Hiện tại luồng Batch Ingestion đang ghi đè (`.mode("overwrite")`) vào đường dẫn `batch/postgres/<table_name>/`. Điều này vi phạm nguyên tắc cốt lõi của Data Lake: Bronze Layer phải là kho lưu trữ dữ liệu thô **bất biến (immutable)**. Nếu ghi đè, ta sẽ mất lịch sử dữ liệu cũ của các ngày trước và không thể chạy lại (replay) dữ liệu khi logic biến đổi ở tầng Silver bị lỗi.
*   **Giải pháp:** Bổ sung cơ chế **Partition theo ngày nạp** cho luồng Batch. Dữ liệu nạp của ngày nào sẽ được lưu trữ riêng trong thư mục của ngày đó:
    *   *Đường dẫn lưu trữ:* `s3a://banking-lakehouse/batch/postgres/<table_name>/load_date=YYYY-MM-DD/`
    *   Mỗi khi cần chạy lại dữ liệu lịch sử, Spark chỉ cần đọc đúng thư mục partition của ngày cần replay.

### 6.4. Giải quyết bài toán Small Files & Tối ưu hóa hiệu năng ClickHouse (Delta Engine)
*   **Thách thức (Small Files Problem):** Spark Streaming hoạt động theo cơ chế Micro-batch ghi dữ liệu liên tục xuống Delta Lake. Hậu quả là sinh ra hàng ngàn file Parquet cực nhỏ (vài KB đến vài trăm KB) mỗi ngày. Điều này làm phình to thư mục `_delta_log` (Metadata Overhead) và gây nghẽn cổ chai I/O cực kỳ nặng nề (Read Bottleneck) khi ClickHouse hoặc Spark Batch đọc Delta Table, do phải mở/đóng quá nhiều file nhỏ.
*   **Giải pháp thiết kế và triển khai thực tế:**
    1.  **Thiết lập Trigger Time (Phòng thủ từ xa):** Thay vì để Spark ghi liên tục, ta nâng cấu hình `.trigger(processingTime='5 minutes')` trong `stream_transform_silver.py`. Spark sẽ gom dữ liệu trên RAM trong 5 phút trước khi ghi xuống Delta Lake thành 1 file Parquet lớn hơn, giúp giảm 90% số lượng file rác ban đầu.
    2.  **Kích hoạt Auto-Compaction & Optimize Write (Tự động hóa của Delta):** Cấu hình thêm thuộc tính Spark Session:
        ```python
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        ```
        Giúp Delta Lake ngầm gom các file nhỏ lại thành file to hơn trong lúc ghi mà không làm gián đoạn luồng streaming.
    3.  **Lập lịch Bảo trì định kỳ (Daily OPTIMIZE & VACUUM - Trị tận gốc):** Lập lịch qua Airflow DAG `delta_lake_maintenance` gọi Spark Job chạy script `delta_maintenance.py` hàng ngày vào lúc nửa đêm:
        *   **`OPTIMIZE <table_name>`**: Quét toàn bộ bảng Delta, gộp tất cả các file Parquet nhỏ còn lại thành các file chuẩn kích thước (~1GB) và ghi commit mới.
        *   **`VACUUM <table_name> RETAIN 168 HOURS`**: Xóa vật lý toàn bộ các file rác Parquet cũ không còn sử dụng đã quá 7 ngày khỏi MinIO để giải phóng dung lượng ổ cứng, chỉ giữ lại dữ liệu lịch sử phục vụ Time Travel trong vòng 7 ngày.
    4.  **Materialization (ClickHouse Materialized Views):** Đối với các bảng Silver có tần suất truy vấn rất cao cho Dashboard, ClickHouse sẽ tạo một bảng vật lý `ReplacingMergeTree` trên ClickHouse và sử dụng **Materialized View** để đồng bộ tự động dữ liệu từ Delta Lake External Table sang bảng vật lý này. Cách này giúp tăng tốc độ query lên gấp 10 lần nhờ khai thác tối đa lưu trữ SSD cục bộ của ClickHouse.

### 6.5. Đảm bảo an toàn thông tin (Data Security) và Chất lượng dữ liệu (Data Quality)
*   **Kiểm soát chất lượng dữ liệu (Data Quality Layer):** Trước khi chuyển dữ liệu từ Silver sang Gold, cần chạy các kiểm tra chất lượng dữ liệu (sử dụng thư viện như **Great Expectations** hoặc Spark Assertions) để lọc các bản ghi lỗi cấu trúc, trùng khóa chính, hoặc lệch dữ liệu nghiệp vụ (ví dụ: giao dịch có số tiền âm). Các bản ghi lỗi sẽ được đưa vào một thư mục riêng (Dead Letter Queue - DLQ) để điều tra.
*   **Mã hóa thông tin nhạy cảm (PII Masking):** Dữ liệu ngân hàng chứa thông tin cá nhân nhạy cảm như Tên, Số điện thoại, Email, Số thẻ tín dụng.
    *   Tầng Bronze (Raw) được mã hóa ở mức lưu trữ (Encryption at Rest trên MinIO).
    *   Tại tầng Silver, Spark tiến hành **hashing/masking** dữ liệu nhạy cảm (ví dụ: đổi `1234-5678-9012` thành `XXXX-XXXX-9012` hoặc băm SHA-256) trước khi ghi vào Delta và đồng bộ sang ClickHouse. Chỉ những người có thẩm quyền đặc biệt mới được cấp quyền truy cập vào bảng chứa dữ liệu giải mã (Decrypt).
*   **Hạ tầng Production HA (High Availability):**
    *   Mô hình Kafka trong thực tế tối thiểu cần **3 Kafka Brokers** trải trên 3 Availability Zones khác nhau với tham số `replication.factor=3` và `min.insync.replicas=2` để chống mất dữ liệu khi 1 node sập.
    *   Schema Registry cần cấu hình tối thiểu 2 nodes (Active-Passive) sử dụng chung bộ lưu trữ schemas trên Kafka topic nội bộ (`_schemas`).