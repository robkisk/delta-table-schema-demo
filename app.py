# COMMAND ----------
from datetime import datetime, timedelta

import pandas as pd
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import pyspark.sql.functions as F
from pyspark.sql.functions import col, current_date, lit, sum, when
from pyspark.sql.window import Window

from get_spark import GetSpark

PROFILE = "feaz"

spark = GetSpark(profile=PROFILE).init_spark(eager=True)


# COMMAND ----------
def test():
    data = [
        {"Category": "A", "ID": 1, "Value": 121.44, "Truth": True},
        {"Category": "B", "ID": 2, "Value": 300.01, "Truth": False},
        {"Category": "C", "ID": 3, "Value": 10.99, "Truth": None},
        {"Category": "E", "ID": 4, "Value": 33.87, "Truth": True},
    ]
    return spark.createDataFrame(data)


# COMMAND ----------
test().show()
