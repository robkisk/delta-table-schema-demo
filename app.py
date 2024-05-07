# COMMAND ----------
from datetime import date, datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import polars as pl
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_date, lit, sum, when
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from utils.get_spark import GetSpark

PROFILE = "feaz"

spark = GetSpark(profile=PROFILE).init_spark(eager=True)


# COMMAND ----------


def df_generator(my_spark) -> DataFrame:
    schema = StructType(
        [
            StructField("AirportCode", StringType(), False),
            StructField("Date", DateType(), False),
            StructField("TempHighF", IntegerType(), False),
            StructField("TempLowF", IntegerType(), False),
        ]
    )

    data = [
        ["BLI", date(2021, 4, 3), 52, 43],
        ["BLI", date(2021, 4, 2), 50, 38],
        ["BLI", date(2021, 4, 1), 52, 41],
        ["PDX", date(2021, 4, 3), 64, 45],
        ["PDX", date(2021, 4, 2), 61, 41],
        ["PDX", date(2021, 4, 1), 66, 39],
        ["SEA", date(2021, 4, 3), 57, 43],
        ["SEA", date(2021, 4, 2), 54, 39],
        ["SEA", date(2021, 4, 1), 56, 41],
    ]

    return my_spark.createDataFrame(data, schema)


# COMMAND ----------
df = df_generator(spark)
df.show()

# # COMMAND ----------
people_df = spark.read.load(
    "/databricks-datasets/learning-spark-v2/people/people-10m.delta"
)
table_name = "people_10m_delta_api"
people_df.write.format("delta").mode("overwrite").saveAsTable(
    f"robkisk.demos.{table_name}", format="delta", mode="overwrite"
)

# COMMAND ----------
table_name = "people_10m_delta_api"
spark.read.table(f"robkisk.demos.{table_name}").show()

# COMMAND ----------
spark.sql("describe table extended robkisk.demos.people_10m_delta_api").show(20, False)

# COMMAND ----------
from delta.tables import DeltaTable

(
    DeltaTable.createOrReplace(spark)
    .tableName(f"robkisk.demos.{table_name}")
    .addColumn("id", "INT")
    .addColumn("firstName", "STRING")
    .addColumn("middleName", "STRING")
    .addColumn("lastName", "STRING", comment="surname")
    .addColumn("gender", "STRING")
    .addColumn("birthDate", "TIMESTAMP")
    .addColumn("ssn", "STRING")
    .addColumn("salary", "INT")
    .execute()
)

# COMMAND ----------
my_schema = StructType(
    [
        StructField("id", IntegerType()),
        StructField("firstName", StringType()),
        StructField("middleName", StringType()),
        StructField("lastName", StringType()),
        StructField("gender", StringType()),
        StructField("birthDate", TimestampType()),
        StructField("ssn", StringType()),
        StructField("salary", IntegerType()),
    ]
)

# COMMAND ----------
(
    DeltaTable.createOrReplace(spark)
    .tableName(f"robkisk.demos.{table_name}_schema_inject")
    .addColumns(my_schema)
    .execute()
)

# COMMAND ----------
spark.sql(f"describe table extended robkisk.demos.{table_name}_schema_inject").show(
    20, False
)

# In Databricks Runtime 13.3 LTS and above, you can use CREATE TABLE LIKE to create a new empty Delta table that
# duplicates the schema and table properties for a source Delta table. This can be especially useful when promoting
# tables from a development environment into production, such as in the following code example:
# COMMAND ----------
spark.sql(
    "CREATE TABLE robkisk.demos.people10m_like LIKE robkisk.demos.people_10m_delta_api_schema_inject"
)

# ------------------------------------------------------------------------------------------------ #
#     Adding schema to Dataframe during creation followed by inferred schema for delta writing     #
# ------------------------------------------------------------------------------------------------ #
# COMMAND ----------
data = [
    ("Robert", "Baratheon", "Baratheon", "Storms End", 48),
    ("Eddard", "Stark", "Stark", "Winterfell", 46),
    ("Jamie", "Lannister", "Lannister", "Casterly Rock", 29),
]
schema = StructType(
    [
        StructField("firstname", StringType(), True),
        StructField("lastname", StringType(), True),
        StructField("house", StringType(), True),
        StructField("location", StringType(), True),
        StructField("age", IntegerType(), True),
    ]
)

sample_dataframe = spark.createDataFrame(data=data, schema=schema)
sample_dataframe.write.mode(saveMode="overwrite").format("delta").saveAsTable(
    "robkisk.demos.schema_predefined"
)
# COMMAND ----------


# COMMAND ----------
spark.sql(f"describe table extended robkisk.demos.schema_predefined").show(20, False)
