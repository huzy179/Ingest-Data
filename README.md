# Banking CDC Ingestion Pipeline: PostgreSQL & MongoDB -> ClickHouse

Dự án này xây dựng một hệ thống Ingestion Pipeline truyền tải dữ liệu thời gian thực (CDC - Change Data Capture) từ hai nguồn cơ sở dữ liệu (PostgreSQL và MongoDB) về ClickHouse Data Warehouse, sử dụng các công cụ Apache Kafka, Debezium, dbt và Airflow.

Hiện tại, dự án đã hoàn thành **Bước 2: Xây dựng cấu trúc dữ liệu nguồn, làm sạch dữ liệu và nạp dữ liệu mẫu chất lượng cao**.

---

## 📁 Cấu trúc Thư mục

```text
├── data/                       # Thư mục chứa các tệp CSV gốc (Không đẩy lên Git)
│   ├── users.csv
│   ├── cards.csv
│   └── transactions.csv
├── infrastructure/             # Cấu hình docker-compose của hạ tầng
│   └── docker-compose.yml      # Chạy PostgreSQL (cổng 5434) và MongoDB (cổng 27017)
├── scripts/                    # Scripts nạp dữ liệu sạch
│   ├── import_users.py         # Nạp khách hàng & tạo profile chính trong Mongo + sự kiện ban đầu
│   ├── import_cards.py         # Nạp thẻ ngân hàng, map vào Postgres + lồng vào Mongo
│   └── import_transactions.py  # Nạp 150 giao dịch mới nhất/user + đồng bộ 5 event collections
├── init_db.py                  # Khởi tạo schema trống cho PostgreSQL & MongoDB
├── requirements.txt            # Thư viện Python cần thiết
├── .gitignore                  # Cấu hình bỏ qua các file thừa và file CSV dung lượng lớn
└── README.md                   # Hướng dẫn dự án này
```

---

## 🛠️ Hạ tầng Kỹ thuật & Thiết kế Dữ liệu

### 1. PostgreSQL (Cổng 5434)
Đóng vai trò là Cơ sở dữ liệu quan hệ lõi của ứng dụng ngân hàng (`banking_core`), lưu trữ dữ liệu dạng bảng quan hệ:
* `customers`: Thông tin cá nhân khách hàng.
* `cards`: Thông tin thẻ của khách hàng (Khóa ngoại kết nối với `customers.id`).
* `transactions`: Giao dịch thanh toán của thẻ (Khóa ngoại kết nối với `cards.id`).

### 2. MongoDB (Cổng 27017)
Đóng vai trò là kho lưu trữ NoSQL (`banking_events`) để quản lý Profile khách hàng lồng nhau và các dòng sự kiện phi cấu trúc:
* `customers`: Hồ sơ khách hàng lồng nhau (gồm thông tin cá nhân, danh sách các thẻ của họ, và mảng 10 giao dịch gần đây nhất được sắp xếp theo đúng thứ tự thời gian).
* `login_events`: Nhật ký đăng nhập thành công/thất bại của người dùng.
* `fraud_events`: Danh sách các giao dịch bị đánh dấu gian lận (`is_fraud == 'Yes'`) kèm theo lý do và điểm rủi ro.
* `audit_logs`: Nhật ký kiểm toán các thao tác dữ liệu (`customer_created`, `card_created`).
* `notification_logs`: Nhật ký thông báo gửi đi cho người dùng (SMS cảnh báo gian lận, Email cảnh báo thẻ bị lộ trên Dark Web).
* `device_events`: Nhật ký tương tác thiết bị ứng dụng của người dùng.

---

## 🚀 Hướng dẫn Bắt đầu Nhanh

### 1. Khởi chạy Hạ tầng Docker
Khởi động cơ sở dữ liệu PostgreSQL và MongoDB chạy ngầm:
```bash
docker compose -f infrastructure/docker-compose.yml up -d
```

### 2. Thiết lập Môi trường Python
Tạo môi trường ảo và cài đặt các thư viện cần thiết:
```bash
# Tạo môi trường ảo
python -m venv .venv

# Kích hoạt môi trường ảo (Windows)
.venv\Scripts\activate

# Kích hoạt môi trường ảo (macOS/Linux)
source .venv/bin/activate

# Cài đặt thư viện
pip install -r requirements.txt
```

### 3. Chuẩn bị tệp dữ liệu CSV gốc
Đặt các tệp dữ liệu CSV của bạn vào thư mục `data/` trong thư mục gốc của dự án:
* `data/users.csv`
* `data/cards.csv`
* `data/transactions.csv` (Tệp này có dung lượng ~2.19 GB và đã được tự động thêm vào `.gitignore` để tránh đẩy lên GitHub).

### 4. Khởi tạo Cơ sở dữ liệu
Chạy script để tạo các bảng trống trong PostgreSQL và các bộ sưu tập (collections) trống trong MongoDB:
```bash
python init_db.py
```

### 5. Nạp dữ liệu nguồn mẫu sạch
Chạy tuần tự 3 script sau để nạp dữ liệu sạch vào hệ thống:
```bash
# 1. Nạp khách hàng và khởi tạo tài liệu MongoDB ban đầu
python scripts/import_users.py

# 2. Nạp thẻ ngân hàng và đồng bộ hóa thời gian tạo tài khoản khách hàng
python scripts/import_cards.py

# 3. Nạp giao dịch (Sắp xếp thời gian & trích chọn 150 giao dịch mới nhất/User)
python scripts/import_transactions.py
```

---

## 🧹 Các Quy tắc làm sạch Dữ liệu đã Áp dụng
* **Mã CVV và Zipcode:** Đệm đầy đủ số 0 ở đầu (CVV đủ 3 chữ số, Zipcode đủ 5 chữ số), ngăn Pandas tự ép kiểu số làm mất dữ liệu.
* **Số phòng căn hộ (Apartment):** Xử lý triệt để các đuôi thập phân dạng `.0` do Pandas tự ép kiểu float khi gặp giá trị trống (`NaN`).
* **Số điện thoại:** Sinh dữ liệu số điện thoại ngẫu nhiên theo đúng định dạng di động Mỹ (`XXX-XXX-XXXX`), đồng bộ hoàn toàn với dữ liệu địa lý của người dùng.
* **Đồng bộ thời gian:** Đồng bộ hóa trường ngày tạo tài khoản của khách hàng (`created_at`) khớp với thời điểm mở thẻ đầu tiên của họ.
* **Đồng bộ sự kiện giao dịch:** Các sự kiện thiết bị (`device_events`) và đăng nhập (`login_events`) được sinh ra tự động trước thời điểm thực hiện giao dịch vài phút để tăng tính thực tế cho dòng dữ liệu phân tích.
