# One image for consumer (default CMD) and exporter (compose overrides the command).
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN useradd --create-home --uid 1000 app
USER app
# Bake httpfs into the image so new pods start without downloading it during scale-out.
RUN python -c "import duckdb; duckdb.sql('INSTALL httpfs')"

COPY src/ /app/
ENV PYTHONUNBUFFERED=1

CMD ["python", "consumer.py"]
