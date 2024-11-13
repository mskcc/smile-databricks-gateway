# Source of smile-dlt, a Delta Live Tables pipeline to process request an sample records published by SMILE.
import dlt
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StringType

volume_path = spark.conf.get("volume_path")

###########################################################################
## request and sample jsons dropped into s3 get read into bronze_raw

@dlt.table(
    name = "bronze_raw",
    comment = "This table contains all raw smile requests and samples as they arrive as *_request.json or *_sample.json files on the landing volume (s3)."
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

###########################################################################
## process requests

# this schema allows us to get to the igo request id from request json
json_request_schema = StructType([
    StructField("igoRequestId", StringType(), True),
])

@dlt.table(
    name = "bronze_requests",
    comment = "This table contains all smile requests as they arrive as *_request.json files on the landing volume (s3)."
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
    comment = "This table contains upserted requests via bronze_requests."
)

dlt.apply_changes(
    target = "silver_requests",
    source = "bronze_requests",
    keys = ["IGO_REQUEST_ID"],
    stored_as_scd_type = "1",
    sequence_by = "INGEST_TIME"
)

###########################################################################
## process samples

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
    name = "bronze_samples",
    comment = "This table contains all smile samples as thery arrive in *_sample.json files on the landing volume (s3)."
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
    name = "silver_samples",
    comment = "This table contains upserted samples via bronze_samples."
)
dlt.apply_changes(
    target = "silver_samples",
    source = "bronze_samples",
    keys = ["IGO_REQUEST_ID", "IGO_SAMPLE_NAME"],
    stored_as_scd_type = "1",
    sequence_by = "INGEST_TIME"
)
