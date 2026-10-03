FROM python:3.12-slim

# System dependencies for psycopg2-binary
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
        unixodbc \
        curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies (pyodbc is included but SQL Server needs
# the ODBC driver installed separately on amd64 hosts)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1
CMD ["python", "main.py"]
