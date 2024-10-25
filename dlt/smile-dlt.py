# Databricks notebook source
import dlt
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StringType

volume_path = spark.conf.get("volume_path")

@dlt.table(
    name = "bronze_raw",
    comment = "This table contains all raw smile requests and samples as they arrive as *_request.json or *_sample.json files on the landing volume."
)
def bronze_raw():
        bronze_df = (
            spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "text")
                .option("cloudFiles.allowOverwrites", True)
                .option("wholetext", True)
                .load(volume_path)
                .withColumn("inputFilename", col("_metadata.file_name"))
                .withColumn("fullFilePath", col("_metadata.file_path"))
                .withColumn("fileMetadata", col("_metadata"))
                .select(
                    "fullFilePath"
                    ,lit(volume_path).alias("datasource")
                    ,"inputFileName"
                    ,date_format(from_utc_timestamp(current_timestamp(), "EST"), "yyyy-MM-dd HH:mm:ss").alias("ingestTime")
                    ,from_utc_timestamp(current_timestamp(), "EST").cast("date").alias("ingestDate")
                    ,"value"
                    ,"fileMetadata"
                )
        )
        return bronze_df


# this schema allows us to get to the igo request id & samples list as a string from request json
json_request_schema = StructType([
    StructField("igoRequestId", StringType(), True),
    StructField("samples", ArrayType(StringType(), True))
])

@dlt.table(
    name = "bronze_requests",
    comment = "This table contains all smile requests as they arrive as *_request.json files on the landing volume."
)
def bronze_requests():
    bronze_data = dlt.read_stream("bronze_raw")
    bronze_requests = (bronze_data
        .filter(col("inputFileName").endswith("request.json"))
        .withColumn("parsed_json", from_json(col("value"), json_request_schema))
        .select(
            col("parsed_json.igoRequestId").alias("IGO_REQUEST_ID"),
            col("value").alias("REQUEST_JSON"),
            col("ingestTime").alias("INGEST_TIME")
        ))
    return bronze_requests

dlt.create_streaming_table(
    name = "silver_requests",
    comment = "This table contains upserted smile requests as they arrive as *_request.json files on the landing volume."
)

dlt.apply_changes(
    target = "silver_requests",
    source = "bronze_requests",
    keys = ["IGO_REQUEST_ID"],
    stored_as_scd_type = "1",
    sequence_by = "INGEST_TIME"
)

# this schema is for parsing samples
json_sample_schema = StructType([
    StructField("additionalProperties", StructType([
        StructField("igoRequestId", StringType(), True),   
    ])),
    StructField("sampleName", StringType(), True),
    StructField("cmoSampleName", StringType(), True),
    StructField("cfDNA2dBarcode", StringType(), True),
    StructField("cmoPatientId", StringType(), True),
])

@dlt.table(
    name = "silver_samples_from_request_json_files",
    comment = "This table contains all smile samples as they arrive in *_request.json files on the landing volume."
)
def silver_samples():
    silver_requests = dlt.read_stream("silver_requests")
    silver_samples = (silver_requests
        .withColumn("INGEST_TIME", col("INGEST_TIME"))
        .withColumn("parsed_request_json", from_json(col("REQUEST_JSON"), json_request_schema))
        .select(
            explode(col("parsed_request_json.samples")).alias("sample_json_as_string"),
            from_json(col("sample_json_as_string"), json_sample_schema).alias("sample_json"),
            "INGEST_TIME"
        ).select(
            col("sample_json.additionalProperties.igoRequestId").alias("IGO_REQUEST_ID"),
            col("sample_json.sampleName").alias("IGO_SAMPLE_NAME"),
            col("sample_json.cmoSampleName").alias("CMO_SAMPLE_NAME"),
            col("sample_json.cfDNA2dBarcode").alias("CFDNA2DBARCODE"),
            col("sample_json.cmoPatientID").alias("CMO_PATIENT_ID"),
            col("sample_json_as_string").alias("SAMPLE_JSON"),
            "INGEST_TIME"
        )
    )
    return silver_samples

@dlt.table(
    name = "bronze_samples",
    comment = "This table contains all smile samples as thery arrive in *_sample.json files on the landing volume."
)
def bronze_samples():
    bronze_data = dlt.read_stream("bronze_raw")
    bronze_samples = (bronze_data
        .filter(col("inputFileName").endswith("sample.json"))
        .withColumn("parsed_json", from_json(col("value"), json_sample_schema))
        .select(
            col("parsed_json.additionalProperties.igoRequestId").alias("IGO_REQUEST_ID"),
            col("parsed_json.sampleName").alias("IGO_SAMPLE_NAME"),
            col("parsed_json.cmoSampleName").alias("CMO_SAMPLE_NAME"),
            col("parsed_json.cfDNA2dBarcode").alias("CFDNA2DBARCODE"),
            col("parsed_json.cmoPatientID").alias("CMO_PATIENT_ID"),
            col("value").alias("SAMPLE_JSON"),
            col("ingestTime").alias("INGEST_TIME")
        ))
    return bronze_samples

dlt.create_streaming_table(
    name = "silver_samples_from_sample_json_files",
    comment = "This table contains upserted samples as they arrive as *_request.json files on the landing volume."
)
dlt.apply_changes(
    target = "silver_samples_from_sample_json_files",
    source = "bronze_samples",
    keys = ["IGO_REQUEST_ID", "IGO_SAMPLE_NAME"],
    stored_as_scd_type = "1",
    sequence_by = "INGEST_TIME"
)

dlt.create_streaming_table (
    name = "silver_samples_combined_json",
    comment = "This table contains samples from both *_request.json and *_sample.json files.  It may contain records with matching IGO_REQUEST_ID and IGO_SAMPLE_NAME.",
)

@dlt.append_flow(target = "silver_samples_combined_json")
def append_request_samples():
    return dlt.read_stream("silver_samples_from_request_json_files")

@dlt.append_flow(target = "silver_samples_combined_json")
def append_json_samples():
    return dlt.read_stream("silver_samples_from_sample_json_files")

@dlt.table(
    name = "silver_samples",
    comment = "This table contains samples from both *_request.json and *_sample.json files.  When records match on IGO_REQUEST_ID and IGO_SAMPLE_NAME, only the record with the most recent INGEST_TIME is saved."
)
def dedup_samples_combined():
    samples = dlt.read("silver_samples_combined_json")
    samples = samples.withColumn("INGEST_TIME", col("INGEST_TIME").cast("timestamp"))
    w = Window.partitionBy(["IGO_REQUEST_ID", "IGO_SAMPLE_NAME"]).orderBy(desc("INGEST_TIME"))
    samples = samples.withColumn('Rank', dense_rank().over(w))
    return samples.filter(samples.Rank == 1).drop(samples.Rank)
