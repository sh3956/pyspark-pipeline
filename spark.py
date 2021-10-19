from pyspark.sql import SparkSession
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


if __name__ == "__main__":
    spark = SparkSession.builder.appName("project").getOrCreate()
    data2017 = spark.read.format("csv").option("header","true").option("inferSchema","true").load("data/Beijing_2017_HourlyPM25_created20170803.csv")
    data2016 = spark.read.format("csv").option("header","true").option("inferSchema","true").load("data/Beijing_2017_HourlyPM25_created20170201.csv")
    data2015 = spark.read.format("csv").option("header","true").option("inferSchema","true").load("data/Beijing_2015_HourlyPM25_created20160201.csv")

    grade_function_udf = udf(get_grade, StringType())
    group2017 = data2017.withColumn("Grade", grade_function_udf(data2017["Value"])).groupby("Grade").count()
    group2016 = data2016.withColumn("Grade", grade_function_udf(data2016["Value"])).groupby("Grade").count()
    group2015 = data2015.withColumn("Grade", grade_function_udf(data2015["Value"])).groupby("Grade").count()


    group2017.select("Grade", "count", group2017['count']/data2017.count())
    group2016.select("Grade", "count", group2016['count']/data2016.count())
    group2015.select("Grade", "count", group2015['count']/data2015.count())