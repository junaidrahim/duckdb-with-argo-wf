import ibis
from queries import lineitem_query, orders_query
import argparse
from dataclasses import dataclass
import time
from pyspark.sql import SparkSession


@dataclass
class Args:
    chunk_number: int
    data_prefix: str
    output_prefix: str
    engine: str


def setup_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--chunk-number",
        type=int,
        help="Chunk number to process",
        default=0,
        required=False,
    )
    parser.add_argument("--data-prefix", help="S3 Data Prefix", required=True)
    parser.add_argument("--output-prefix", help="Output Prefix", required=True)
    parser.add_argument(
        "--engine", help="Ibis Processing Engine", default="duckdb", required=False
    )

    return parser


def process(orders: ibis.Table, lineitem: ibis.Table, output_prefix: str):
    orders_summary = orders_query(orders)
    lineitem_summary = lineitem_query(lineitem)

    print(f"orders: {orders_summary.count().to_pandas()}")
    print(f"lineitem: {lineitem_summary.count().to_pandas()}")

    orders_summary.to_parquet(f"{output_prefix}/orders_summary.parquet")
    lineitem_summary.to_parquet(f"{output_prefix}/lineitem_summary.parquet")


def main(
    engine: str,
    chunk_number: int,
    data_prefix: str,
    output_prefix: str,
):
    orders_chunk = f"{data_prefix}/orders/{chunk_number:04}.parquet"
    lineitem_chunk = f"{data_prefix}/lineitem/{chunk_number:04}.parquet"

    conn = None
    if engine == "duckdb":
        conn = ibis.duckdb.connect(
            "/tmp/database.duckdb",
            temp_directory="/tmp",
            threads=2,
            memory_limit="2GB",
            extensions=["httpfs"],
        )
    elif engine == "polars":
        conn = ibis.polars.connect()
    elif engine == "pyspark":
        spark = (
            SparkSession.builder.config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:2.10.2,org.apache.hadoop:hadoop-client:2.10.2",
            )
            .config("spark.jars.excludes", "com.google.guava:guava")
            .getOrCreate()
        )
        conn = ibis.pyspark.connect(session=spark)

    print(f"Ibis Processing Engine: {ibis.get_backend().name}")

    start = time.time()

    print(f"Loading {orders_chunk}")
    orders = conn.read_parquet(orders_chunk, "orders")

    print(f"Loading {lineitem_chunk}")
    lineitem = conn.read_parquet(lineitem_chunk, "lineitem")

    process(orders=orders, lineitem=lineitem, output_prefix=output_prefix)

    duration = time.time() - start
    print(f"Duration: {duration} seconds")

    with open(f"{output_prefix}/time.txt", "w") as f:
        f.write(str(duration))


if __name__ == "__main__":
    parser = setup_arg_parser()
    args = Args(**vars(parser.parse_args()))
    main(
        engine=args.engine,
        chunk_number=args.chunk_number,
        data_prefix=args.data_prefix,
        output_prefix=args.output_prefix,
    )
