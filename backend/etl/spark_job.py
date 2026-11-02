from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType


def parse_args() -> argparse.Namespace:
    """Разбирает параметры запуска ETL-задачи.

    Returns:
        argparse.Namespace: Параметры источника, хранилища и Spark.
    """
    parser = argparse.ArgumentParser(description="Ekover batch ETL pipeline")
    parser.add_argument("--input-key", required=True, help="Ключ исходного CSV внутри Data Lake")
    parser.add_argument("--dataset-id", default=None, help="Логический идентификатор датасета")
    parser.add_argument("--storage-backend", choices=("hdfs", "local"), default=os.getenv("STORAGE_BACKEND", "hdfs"))
    parser.add_argument("--hdfs-root", default=os.getenv("HDFS_ROOT", "/ekover"))
    parser.add_argument("--hdfs-namenode", default=os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000"))
    parser.add_argument("--local-root", default="/app/data/lake")
    parser.add_argument("--delimiter", default=",")
    parser.add_argument("--encoding", default="UTF-8")
    return parser.parse_args()


def normalize_column_names(dataframe: DataFrame) -> DataFrame:
    """Нормализует имена колонок и устраняет коллизии.

    Args:
        dataframe: Исходный Spark DataFrame.

    Returns:
        DataFrame: DataFrame с безопасными уникальными именами колонок.
    """
    used: set[str] = set()
    renamed: list[str] = []
    for original in dataframe.columns:
        base = re.sub(r"[^a-zA-Z0-9_]+", "_", original.strip().lower()).strip("_")
        base = base or "column"
        if base[0].isdigit():
            base = f"col_{base}"
        candidate = base
        suffix = 2
        while candidate in used:
            candidate = f"{base}_{suffix}"
            suffix += 1
        used.add(candidate)
        renamed.append(candidate)
    return dataframe.toDF(*renamed)


def standardize_values(dataframe: DataFrame) -> DataFrame:
    """Очищает строковые значения от пробелов и пустых строк.

    Args:
        dataframe: DataFrame с исходными значениями.

    Returns:
        DataFrame: DataFrame с нормализованными строковыми полями.
    """
    result = dataframe
    for field in result.schema.fields:
        if field.dataType.simpleString() == "string":
            result = result.withColumn(
                field.name,
                F.when(F.trim(F.col(field.name)) == "", F.lit(None)).otherwise(F.trim(F.col(field.name))),
            )
    return result


def build_column_profile(dataframe: DataFrame, dataset_id: str, batch_id: str) -> DataFrame:
    """Строит агрегированное профилирование по каждой колонке.

    Args:
        dataframe: Очищенный Silver DataFrame.
        dataset_id: Идентификатор датасета.
        batch_id: Идентификатор текущего ETL-запуска.

    Returns:
        DataFrame: Метрики null, distinct и числовые статистики по колонкам.
    """
    total_rows = dataframe.count()
    expressions = []
    for field in dataframe.schema.fields:
        column = F.col(field.name)
        expressions.extend(
            [
                F.sum(F.when(column.isNull(), 1).otherwise(0)).alias(f"{field.name}__nulls"),
                F.approx_count_distinct(column).alias(f"{field.name}__distinct"),
            ]
        )
        if isinstance(field.dataType, NumericType):
            expressions.extend(
                [
                    F.min(column).alias(f"{field.name}__min"),
                    F.max(column).alias(f"{field.name}__max"),
                    F.avg(column).alias(f"{field.name}__avg"),
                ]
            )
    aggregate = dataframe.agg(*expressions).first().asDict() if expressions else {}
    rows: list[dict[str, Any]] = []
    for field in dataframe.schema.fields:
        row: dict[str, Any] = {
            "dataset_id": dataset_id,
            "batch_id": batch_id,
            "column_name": field.name,
            "data_type": field.dataType.simpleString(),
            "row_count": total_rows,
            "null_count": int(aggregate.get(f"{field.name}__nulls") or 0),
            "distinct_count_approx": int(aggregate.get(f"{field.name}__distinct") or 0),
            "null_rate": float((aggregate.get(f"{field.name}__nulls") or 0) / total_rows) if total_rows else 0.0,
            "min_value": None,
            "max_value": None,
            "avg_value": None,
        }
        if isinstance(field.dataType, NumericType):
            row["min_value"] = aggregate.get(f"{field.name}__min")
            row["max_value"] = aggregate.get(f"{field.name}__max")
            average = aggregate.get(f"{field.name}__avg")
            row["avg_value"] = float(average) if average is not None else None
        rows.append(row)
    schema = "dataset_id string, batch_id string, column_name string, data_type string, row_count long, null_count long, distinct_count_approx long, null_rate double, min_value string, max_value string, avg_value double"
    normalized_rows = [
        {**row, "min_value": None if row["min_value"] is None else str(row["min_value"]),
         "max_value": None if row["max_value"] is None else str(row["max_value"])}
        for row in rows
    ]
    return dataframe.sparkSession.createDataFrame(normalized_rows, schema=schema)


def build_quality_report(
    source_rows: int,
    bronze_rows: int,
    silver_rows: int,
    dataframe: DataFrame,
    dataset_id: str,
    batch_id: str,
) -> DataFrame:
    """Формирует сводный отчёт о качестве обработки.

    Args:
        source_rows: Количество строк во входном CSV.
        bronze_rows: Количество строк после сохранения Bronze.
        silver_rows: Количество строк после очистки Silver.
        dataframe: Очищенный Silver DataFrame.
        dataset_id: Идентификатор датасета.
        batch_id: Идентификатор ETL-запуска.

    Returns:
        DataFrame: Одна строка с основными показателями качества.
    """
    null_expressions = [
        F.sum(F.when(F.col(field.name).isNull(), 1).otherwise(0))
        for field in dataframe.schema.fields
    ]
    null_cells = dataframe.agg(*null_expressions).first() if null_expressions else None
    null_count = sum(int(value or 0) for value in null_cells) if null_cells else 0
    duplicate_rows = max(bronze_rows - silver_rows, 0)
    payload = {
        "dataset_id": dataset_id,
        "batch_id": batch_id,
        "source_rows": source_rows,
        "bronze_rows": bronze_rows,
        "silver_rows": silver_rows,
        "duplicate_rows_removed": duplicate_rows,
        "null_cells": null_count,
        "column_count": len(dataframe.columns),
        "duplicate_rate": float(duplicate_rows / bronze_rows) if bronze_rows else 0.0,
        "status": "success" if silver_rows > 0 else "warning_empty_dataset",
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }
    return dataframe.sparkSession.createDataFrame([payload])


def resolve_path(args: argparse.Namespace, object_key: str) -> str:
    """Преобразует ключ Data Lake в URI, понятный Spark.

    Args:
        args: Параметры ETL-запуска.
        object_key: Относительный ключ объекта.

    Returns:
        str: Абсолютный URI исходного или результирующего объекта.
    """
    safe_key = object_key.strip("/")
    if ".." in safe_key.split("/"):
        raise ValueError("Путь не должен содержать '..'.")
    if args.storage_backend == "hdfs":
        return f"{args.hdfs_namenode.rstrip('/')}{args.hdfs_root.rstrip('/')}/{safe_key}"
    return f"file://{args.local_root.rstrip('/')}/{safe_key}"


def run_pipeline(args: argparse.Namespace) -> dict[str, Any]:
    """Выполняет полный batch ETL из Raw в Bronze, Silver и Gold.

    Args:
        args: Параметры источника и конфигурация Data Lake.

    Returns:
        dict[str, Any]: Идентификаторы запуска и пути созданных слоёв.

    Raises:
        RuntimeError: Если Spark не смог прочитать или обработать источник.
        ValueError: Если ключ источника содержит небезопасный путь.
    """
    input_uri = resolve_path(args, args.input_key)
    dataset_id = args.dataset_id or args.input_key.split("/")[-1].rsplit(".", 1)[0]
    dataset_id = re.sub(r"[^a-zA-Z0-9_-]+", "_", dataset_id).strip("_")
    if not dataset_id:
        raise ValueError("Не удалось определить корректный dataset_id.")
    batch_id = uuid.uuid4().hex
    ingest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    spark = (
        SparkSession.builder.appName(f"ekover-etl-{dataset_id}-{batch_id[:8]}")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        source = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .option("mode", "PERMISSIVE")
            .option("encoding", args.encoding)
            .option("sep", args.delimiter)
            .csv(input_uri)
        )
        source_rows = source.count()
        bronze = source.withColumn("_dataset_id", F.lit(dataset_id))
        bronze = bronze.withColumn("_batch_id", F.lit(batch_id))
        bronze = bronze.withColumn("_ingested_at", F.current_timestamp())
        bronze = bronze.withColumn("_source_file", F.input_file_name())
        bronze_path = resolve_path(args, f"bronze/{dataset_id}/ingest_date={ingest_date}/batch_id={batch_id}")
        bronze.write.mode("overwrite").parquet(bronze_path)
        bronze_rows = bronze.count()

        silver = standardize_values(normalize_column_names(source))
        silver = silver.dropDuplicates()
        silver = silver.withColumn("_dataset_id", F.lit(dataset_id))
        silver = silver.withColumn("_batch_id", F.lit(batch_id))
        silver = silver.withColumn("_processed_at", F.current_timestamp())
        silver_path = resolve_path(args, f"silver/{dataset_id}/ingest_date={ingest_date}/batch_id={batch_id}")
        silver.write.mode("overwrite").parquet(silver_path)
        silver_rows = silver.count()

        business_data = silver.drop("_dataset_id", "_batch_id", "_processed_at")
        profile = build_column_profile(business_data, dataset_id, batch_id)
        profile_path = resolve_path(args, f"gold/column_profile/ingest_date={ingest_date}/batch_id={batch_id}")
        profile.write.mode("overwrite").parquet(profile_path)

        quality = build_quality_report(source_rows, bronze_rows, silver_rows, business_data, dataset_id, batch_id)
        quality_path = resolve_path(args, f"gold/data_quality/ingest_date={ingest_date}/batch_id={batch_id}")
        quality.write.mode("overwrite").parquet(quality_path)

        result = {
            "dataset_id": dataset_id,
            "batch_id": batch_id,
            "source_uri": input_uri,
            "bronze_uri": bronze_path,
            "silver_uri": silver_path,
            "column_profile_uri": profile_path,
            "data_quality_uri": quality_path,
            "source_rows": source_rows,
            "bronze_rows": bronze_rows,
            "silver_rows": silver_rows,
            "status": "success" if silver_rows else "warning_empty_dataset",
        }
        manifest_path = resolve_path(args, f"gold/etl_runs/ingest_date={ingest_date}/batch_id={batch_id}")
        spark.createDataFrame([(json.dumps(result, ensure_ascii=False),)], ["manifest_json"]).write.mode("overwrite").text(manifest_path)
        return result
    finally:
        spark.stop()


def main() -> int:
    """Запускает ETL и выводит итоговый JSON в stdout.

    Returns:
        int: Код завершения процесса: 0 при успехе, 1 при ошибке.
    """
    args = parse_args()
    try:
        result = run_pipeline(args)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except Exception as error:
        print(json.dumps({"status": "failed", "error": str(error)}, ensure_ascii=False), file=sys.stderr)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
