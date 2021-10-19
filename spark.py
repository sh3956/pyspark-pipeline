from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("project").getOrCreate()
    data2017 = spark.read.format("csv").load("data/Beijing_2017_HourlyPM25_created20170803.csv")
    data2016 = spark.read.format("csv").load("data/Beijing_2017_HourlyPM25_created20170201.csv")
    data2015 = spark.read.format("csv").load("data/Beijing_2015_HourlyPM25_created20160201.csv")

    df.show()
