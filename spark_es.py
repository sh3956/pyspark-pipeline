## This script is used to upload preprocessed data into elastic search.

from pyspark.sql.types import *
from pyspark.sql.functions import udf


def get_grade(value):
    """ Get PM2.5 grade level based on its value
    params:
        value(float): pm2.5 value
    """
    if value <= 50 and value >= 0:
        return "Healthy"
    elif value <= 100:
        return "Medium"
    elif value <= 150:
        return "Unhealthy to sensitive groups"
    elif value <= 200:
        return "Unhealthy"
    elif value <= 300:
        return "Very unhealthy"
    else:
        return "Hazardous"

data2017 = spark.read.format("csv").option("header","true").option("inferSchema","true").load("hdfs:///data/Beijing_2017_HourlyPM25_created20170803.csv")

grade_function_udf = udf(get_grade, StringType())
group2017 = data2017.withColumn("Grade", grade_function_udf(data2017["Value"])).groupby("Grade").count()
result2017 = group2017.select("Grade", "count", group2017['count']/data2017.count())

# write data to elastic search
result2017.write.format("org.elasticsearch.spark.sql").option("es.nodes", "192.168.199.102:9200").mode("overwrite").save("weaes/weather")

