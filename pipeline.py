from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    to_date, year, month, quarter,
    avg, sum, desc, col
)

from pyspark.sql.window import Window
from pyspark.sql.functions import rank

# ==================================================
# 1. SPARK SESSION
# ==================================================
spark = SparkSession.builder \
    .appName("KFintech_MutualFund_ETL_Advanced") \
    .getOrCreate()

# ==================================================
# 2. EXTRACT
# ==================================================
df = spark.read.csv(
    "input/Mutual_Funds.csv",
    header=True,
    inferSchema=True
)

print("\n===== TOTAL RECORDS =====")
print(df.count())

# ==================================================
# 3. BASIC CLEANING
# ==================================================

# Handle missing values (better than only dropna)
df = df.fillna({
    "Scheme_Name": "Unknown",
    "Scheme_Type": "Unknown",
    "Scheme_Category": "Unknown",
    "Fund_House": "Unknown",
    "NAV": 0
})

# Remove duplicates
df = df.dropDuplicates()

# Remove invalid NAV values
df = df.filter(df.NAV > 0)

# ==================================================
# 4. DATE TRANSFORMATION
# ==================================================
df = df.withColumn("Date", to_date("Date", "dd-MM-yyyy"))

# ==================================================
# 5. FEATURE ENGINEERING
# ==================================================
df = df.withColumn("Year", year("Date")) \
       .withColumn("Month", month("Date")) \
       .withColumn("Quarter", quarter("Date"))

# ==================================================
# 6. PERFORMANCE OPTIMIZATION
# ==================================================
df = df.cache()

# ==================================================
# 7. DATA QUALITY CHECKS
# ==================================================
print("\n===== NULL CHECK =====")
df.filter(col("NAV").isNull()).show()

print("\n===== INVALID DATA CHECK =====")
df.filter(col("NAV") <= 0).show()

# ==================================================
# 8. DIMENSION TABLES
# ==================================================

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

# ==================================================
# 9. FACT TABLE
# ==================================================
fact_nav = df.select(
    "Scheme_Code",
    "Date",
    "NAV"
)

# ==================================================
# 10. KPI 1 - TOTAL AUM BY SCHEME
# ==================================================
aum_by_scheme = df.groupBy("Scheme_Name") \
    .agg(sum("NAV").alias("AUM")) \
    .orderBy(desc("AUM"))

print("\n===== AUM BY SCHEME =====")
aum_by_scheme.show(10, truncate=False)

# ==================================================
# 11. KPI 2 - FUND HOUSE PERFORMANCE
# ==================================================
fund_house_kpi = df.groupBy("Fund_House") \
    .agg(avg("NAV").alias("Avg_NAV")) \
    .orderBy(desc("Avg_NAV"))

print("\n===== FUND HOUSE KPI =====")
fund_house_kpi.show()

# ==================================================
# 12. KPI 3 - TOP SCHEMES
# ==================================================
top_schemes = df.groupBy("Scheme_Name") \
    .agg(avg("NAV").alias("Avg_NAV")) \
    .orderBy(desc("Avg_NAV"))

print("\n===== TOP SCHEMES =====")
top_schemes.show(10, truncate=False)

# ==================================================
# 13. WINDOW FUNCTION - RANKING SCHEMES
# ==================================================
window_spec = Window.orderBy(desc("NAV"))

ranked_df = df.withColumn("rank", rank().over(window_spec))

print("\n===== RANKED SCHEMES =====")
ranked_df.show(10, truncate=False)

# ==================================================
# 14. LOAD (PARQUET WITH PARTITIONING)
# ==================================================

dim_scheme.write.mode("overwrite").parquet("output/dim_scheme")

dim_date.write.mode("overwrite").parquet("output/dim_date")

fact_nav.write \
    .mode("overwrite") \
    .partitionBy("Date") \
    .parquet("output/fact_nav")

aum_by_scheme.write.mode("overwrite").parquet("output/aum_by_scheme")
fund_house_kpi.write.mode("overwrite").parquet("output/fund_house_kpi")

# ==================================================
# 15. END PIPELINE
# ==================================================
print("\n===== ETL PIPELINE COMPLETED SUCCESSFULLY =====")

spark.stop()