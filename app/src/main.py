from pyspark.sql import SparkSession
import pydeequ
from data_quality.profiling import DataProfiler
from data_quality.output import DataQualityOutput

spark = (SparkSession.builder
         .config("spark.jars.packages", pydeequ.deequ_maven_coord)
         .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
         .getOrCreate())

df = (spark.read
      .format("csv")
      .option("header", "true")
      .load("datasets\\weather_dataset\\weather.csv"))

# df.show()

# print(df.dtypes)

df.createOrReplaceTempView("weather")

df2 = spark.sql("select STATION,\
                CountryRegion,\
                DATE(DATE),\
                INT(Year),\
                INT(Month),\
                INT(Day),\
                DOUBLE(PRCP),\
                DOUBLE(TAVG),\
                DOUBLE(TMAX),\
                DOUBLE(TMIN),\
                DOUBLE(SNOW) \
                from weather")

test = DataProfiler(spark, df2)

df_profiling = test.run()

output = DataQualityOutput(df_profiling)

output.dataframe()
