FROM python:3.11-slim

WORKDIR /app

COPY webapp/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY wallet-filter/filter_wallets.py /app/wallet-filter/filter_wallets.py
COPY webapp/ /app/webapp/

# Hugging Face Spaces runs as a non-root user; the data dir must be writable.
RUN mkdir -p /app/webapp/data && chmod -R 777 /app/webapp/data

ENV PYTHONUNBUFFERED=1 \
    DATA_DIR=/app/webapp/data \
    PORT=7860

EXPOSE 7860
WORKDIR /app/webapp
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT:-7860}"]
