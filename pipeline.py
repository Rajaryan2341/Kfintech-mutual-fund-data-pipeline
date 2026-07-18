from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    to_date, year, month, quarter,
    avg, sum, desc, col
)
from pyspark.sql.window import Window
from pyspark.sql.functions import rank
spark = SparkSession.builder \
    .appName("KFintech_MutualFund_ETL_Advanced") \
df = spark.read.csv(
    "input/Mutual_Funds.csv",
    header=True,
    inferSchema=True
)

print(" TOTAL RECORDS ")
print(df.count())
df = df.fillna({
    "Scheme_Name": "Unknown",
    "Scheme_Type": "Unknown",
    "Scheme_Category": "Unknown",
    "Fund_House": "Unknown",
    "NAV": 0
})
df = df.dropDuplicates()
df = df.filter(df.NAV > 0)
df = df.withColumn("Date", to_date("Date", "dd-MM-yyyy"))
df = df.withColumn("Year", year("Date")) \
       .withColumn("Month", month("Date")) \
       .withColumn("Quarter", quarter("Date"))
df = df.cache()
 
print("  NULL CHECK ")
df.filter(col("NAV").isNull()).show()

print(" INVALID DATA CHECK ")
df.filter(col("NAV") <= 0).show()
 
dim_scheme = df.select(
    "Scheme_Code",
    "Scheme_Name",
    "Scheme_Type",
    "Scheme_Category",
    "Fund_House"
).dropDuplicates()

dim_date = df.select(
    "Date",
    "Year",
    "Month",
    "Quarter"
).dropDuplicates()
 
fact_nav = df.select(
    "Scheme_Code",
    "Date",
    "NAV"
)
 
aum_by_scheme = df.groupBy("Scheme_Name") \
    .agg(sum("NAV").alias("AUM")) \
    .orderBy(desc("AUM"))

print(" AUM BY SCHEME ")
aum_by_scheme.show(10, truncate=False)
 
fund_house_kpi = df.groupBy("Fund_House") \
    .agg(avg("NAV").alias("Avg_NAV")) \
    .orderBy(desc("Avg_NAV"))

print(" FUND HOUSE KPI ")
fund_house_kpi.show()
 
top_schemes = df.groupBy("Scheme_Name") \
    .agg(avg("NAV").alias("Avg_NAV")) \
    .orderBy(desc("Avg_NAV"))

print("TOP SCHEMES")
top_schemes.show(10, truncate=False)
 
window_spec = Window.orderBy(desc("NAV"))

ranked_df = df.withColumn("rank", rank().over(window_spec))

print(" RANKED SCHEMES ")
ranked_df.show(10, truncate=False)
 
dim_scheme.write.mode("overwrite").parquet("output/dim_scheme")

dim_date.write.mode("overwrite").parquet("output/dim_date")

fact_nav.write \
    .mode("overwrite") \
    .partitionBy("Date") \
    .parquet("output/fact_nav")

aum_by_scheme.write.mode("overwrite").parquet("output/aum_by_scheme")
fund_house_kpi.write.mode("overwrite").parquet("output/fund_house_kpi")
 
print(" ETL PIPELINE COMPLETED SUCCESSFULLY ")

spark.stop()
