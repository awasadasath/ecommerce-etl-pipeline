# ใช้ Base Image เวอร์ชัน 3.1.3 ตามที่พี่ขอ
FROM apache/airflow:3.1.3

# สลับเป็น Root เพื่อลง package พื้นฐาน (ถ้าจำเป็น)
USER root
RUN apt-get update && apt-get install -y \
  build-essential \
  default-libmysqlclient-dev \
  && apt-get clean

# สลับกลับมาเป็น User airflow เพื่อลง Python Library
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt