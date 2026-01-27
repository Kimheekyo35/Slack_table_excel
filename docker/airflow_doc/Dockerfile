FROM apache/airflow:3.1.0

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
libglib2.0-0 \
libnss3 \
libnspr4 \
libatk1.0-0 \
libatk-bridge2.0-0 \
libcups2 \
libdrm2 \
libxkbcommon0 \
libxcomposite1 \
libxdamage1 \
libxfixes3 \
libxrandr2 \
libgbm1 \
libasound2 \
libpangocairo-1.0-0 \
libpango-1.0-0 \
libcairo2 \
libx11-6 \
libx11-xcb1 \
libxcb1 \
libxext6 \
libxrender1 \
ca-certificates \
fonts-liberation \
&& rm -rf /var/lib/apt/lists/*

USER airflow

COPY docker/airflow_doc/requirements_a.txt /tmp/requirements_a.txt
COPY docker/airflow_doc/requirements_b.txt /tmp/requirements_b.txt

RUN pip install --no-cache-dir -r /tmp/requirements_a.txt && \
pip install --no-cache-dir -r /tmp/requirements_b.txt && \
pip install --no-cache-dir playwright && \
python -m playwright install chromium
