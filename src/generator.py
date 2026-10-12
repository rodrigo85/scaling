#!/usr/bin/env python3
"""
TPC-H Synthetic Data Generator for Ingestion Autoscaling Simulation.
Generates synthetic orders/lineitem data via DuckDB and uploads batches in Parquet to LocalStack S3.
"""

import os
import sys
import tempfile

import duckdb

from common import RAW_BUCKET, client


def generate_tpch_batches(scale_factor=0.05, batch_size=500, upload_to_s3=True):
    print(f"[TPC-H] Gerando dados sintéticos com Scale Factor = {scale_factor}...")
    con = duckdb.connect()
    con.execute("INSTALL tpch; LOAD tpch;")
    con.execute(f"CALL dbgen(sf={scale_factor});")

    total_orders = con.execute("SELECT count(*) FROM orders").fetchone()[0]
    print(f"[TPC-H] Total de pedidos gerados na memória: {total_orders}")

    s3 = client("s3") if upload_to_s3 else None

    # Particiona e exporta em arquivos Parquet
    num_batches = (total_orders + batch_size - 1) // batch_size
    print(f"[TPC-H] Exportando em {num_batches} lotes de até {batch_size} registros...")

    batch_manifest = []

    with tempfile.TemporaryDirectory() as tmpdir:
        for batch_idx in range(num_batches):
            offset = batch_idx * batch_size
            parquet_path = os.path.join(tmpdir, f"orders_batch_{batch_idx:05d}.parquet")
            
            con.execute(f"""
                COPY (
                    SELECT 
                        o.o_orderkey,
                        o.o_custkey,
                        o.o_orderstatus,
                        o.o_totalprice,
                        o.o_orderdate,
                        o.o_orderpriority,
                        o.o_clerk,
                        o.o_shippriority,
                        o.o_comment,
                        l.l_partkey,
                        l.l_suppkey,
                        l.l_linenumber,
                        l.l_quantity,
                        l.l_extendedprice,
                        l.l_discount,
                        l.l_tax
                    FROM orders o
                    LEFT JOIN lineitem l ON o.o_orderkey = l.l_orderkey
                    WHERE o.o_orderkey IN (
                        SELECT o_orderkey FROM orders ORDER BY o_orderkey LIMIT {batch_size} OFFSET {offset}
                    )
                ) TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION ZSTD);
            """)

            file_size = os.path.getsize(parquet_path)
            s3_key = f"raw/orders/batch_{batch_idx:05d}.parquet"

            if upload_to_s3:
                s3.upload_file(parquet_path, RAW_BUCKET, s3_key)

            batch_manifest.append({
                "batch_id": batch_idx,
                "s3_key": s3_key,
                "bucket": RAW_BUCKET,
                "file_size": file_size,
                "records": batch_size
            })

            if (batch_idx + 1) % 10 == 0 or (batch_idx + 1) == num_batches:
                print(f"[TPC-H] Lotes processados: {batch_idx + 1}/{num_batches}")

    print(f"[TPC-H] Concluído! {len(batch_manifest)} arquivos enviados para s3://{RAW_BUCKET}/")
    return batch_manifest


if __name__ == "__main__":
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 0.05
    generate_tpch_batches(scale_factor=sf)
