# FROM apache/airflow:2.7.3

# # install extra requirements
# COPY requirements.txt /
# RUN pip install --user --no-cache-dir -r /requirements.txt

FROM apache/airflow:2.9.1

# Install extra requirements
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt