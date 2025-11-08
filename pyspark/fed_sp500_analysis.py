"""
Wealth Management Analysis - Fed Cuts Impact on S&P 500
PySpark Implementation - Converted from SAS
Analysis of Federal Reserve Rate Cuts and S&P 500 Performance
Period: 2020-2025
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lag, lead, avg, stddev, min as spark_min, max as spark_max,
    sum as spark_sum, count, when, lit, datediff, months_between,
    date_format, to_date, expr, round as spark_round, udf, abs as spark_abs
)
from pyspark.sql.types import DoubleType, StringType, IntegerType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.stat import Correlation
import pandas as pd
import numpy as np

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("FedRatesSP500Analysis") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

print("="*80)
print("WEALTH MANAGEMENT ANALYSIS - PYSPARK IMPLEMENTATION")
print("Converting SAS procedures to PySpark")
print("="*80)

# ============================================================================
# CUSTOM UDFs (User Defined Functions)
# These replace SAS custom functions and PROC SQL operations
# ============================================================================

@udf(returnType=StringType())
def classify_rate_change(rate_change):
    """
    Custom UDF to classify rate changes
    Replaces SAS IF-THEN logic
    """
    if rate_change is None:
        return "Hold"
    elif rate_change < -0.25:
        return "Significant Cut"
    elif rate_change < 0:
        return "Minor Cut"
    elif rate_change > 0.25:
        return "Significant Hike"
    elif rate_change > 0:
        return "Minor Hike"
    else:
        return "Hold"

@udf(returnType=DoubleType())
def calculate_volatility_score(volatility, avg_volatility):
    """
    Custom UDF for volatility scoring
    Replaces SAS custom function
    """
    if volatility is None or avg_volatility is None:
        return 0.0
    return (volatility - avg_volatility) / avg_volatility * 100

# ============================================================================
# DATA IMPORT - Replaces SAS PROC IMPORT
# ============================================================================

print("\n[1/10] Loading datasets (SAS PROC IMPORT equivalent)...")

# Read Federal Funds Rate Data
fed_rates_df = spark.read.csv(
    "/home/user/makkena.github.io/data/fed_rates_2020_2025.csv",
    header=True,
    inferSchema=True
)

# Read S&P 500 Daily Data
sp500_daily_df = spark.read.csv(
    "/home/user/makkena.github.io/data/sp500_daily_2020_2025.csv",
    header=True,
    inferSchema=True
)

print(f"   Fed Rates Records: {fed_rates_df.count()}")
print(f"   S&P 500 Records: {sp500_daily_df.count()}")

# ============================================================================
# DATA PREPARATION - Replaces SAS DATA STEP
# ============================================================================

print("\n[2/10] Data preparation and transformation (SAS DATA STEP equivalent)...")

# Convert date strings to date type
fed_rates_clean = fed_rates_df \
    .withColumn("sas_date", to_date(col("Date"), "yyyy-MM-dd")) \
    .withColumn("year_month", date_format(col("sas_date"), "yyyyMM"))

sp500_clean = sp500_daily_df \
    .withColumn("sas_date", to_date(col("Date"), "yyyy-MM-dd")) \
    .withColumn("year_month", date_format(col("sas_date"), "yyyyMM"))

# Calculate daily returns - Replaces SAS LAG function
window_spec = Window.orderBy("sas_date")

sp500_clean = sp500_clean \
    .withColumn("prev_close", lag("Close", 1).over(window_spec)) \
    .withColumn("daily_return",
                when(col("prev_close").isNotNull(),
                     ((col("Close") - col("prev_close")) / col("prev_close") * 100))
                .otherwise(0.0))

# ============================================================================
# MONTHLY AGGREGATION - Replaces SAS PROC MEANS
# ============================================================================

print("\n[3/10] Calculating monthly statistics (SAS PROC MEANS equivalent)...")

sp500_monthly = sp500_clean \
    .groupBy("year_month") \
    .agg(
        avg("Close").alias("avg_close"),
        avg("daily_return").alias("avg_daily_return"),
        stddev("daily_return").alias("volatility"),
        spark_min("Close").alias("min_close"),
        spark_max("Close").alias("max_close"),
        spark_sum("Volume").alias("total_volume"),
        count("*").alias("trading_days")
    )

# ============================================================================
# DATA MERGING - Replaces SAS PROC SQL JOIN
# ============================================================================

print("\n[4/10] Merging Fed rates with S&P 500 data (SAS PROC SQL equivalent)...")

merged_analysis = fed_rates_clean.join(
    sp500_monthly,
    on="year_month",
    how="left"
)

# Calculate month-over-month change in S&P 500
window_monthly = Window.orderBy("sas_date")
merged_analysis = merged_analysis \
    .withColumn("prev_avg_close", lag("avg_close", 1).over(window_monthly)) \
    .withColumn("sp500_monthly_change",
                when(col("prev_avg_close").isNotNull(),
                     ((col("avg_close") - col("prev_avg_close")) / col("prev_avg_close") * 100))
                .otherwise(0.0))

# ============================================================================
# IDENTIFY RATE CUT EVENTS - Replaces SAS DATA STEP with WHERE clause
# ============================================================================

print("\n[5/10] Identifying rate cut events (SAS WHERE/IF-THEN equivalent)...")

rate_cut_events = merged_analysis \
    .filter((col("Change_Type") == "Cut") | (col("Change_Type") == "Emergency Cut")) \
    .withColumn("significant_cut",
                when(spark_abs(col("Basis_Points")) >= 25, 1).otherwise(0))

# ============================================================================
# FORWARD RETURNS ANALYSIS - Replaces SAS PROC SQL with INTNX
# ============================================================================

print("\n[6/10] Calculating forward returns after rate cuts (SAS INTNX equivalent)...")

# Create window for forward looking returns
# This is more complex in PySpark than SAS INTNX
# We'll use a self-join approach

rate_cut_base = rate_cut_events.select(
    col("sas_date").alias("cut_date"),
    col("Federal_Funds_Rate"),
    col("Basis_Points"),
    col("avg_close").alias("sp500_at_cut")
)

# Helper function to calculate forward returns
def calculate_forward_returns(base_df, merged_df, months_ahead, return_col_name):
    """
    Custom function to calculate forward returns
    Replaces SAS INTNX and PROC SQL date arithmetic
    """
    return base_df.alias("a").join(
        merged_df.alias("b"),
        expr(f"datediff(b.sas_date, a.cut_date) >= {months_ahead * 28} AND "
             f"datediff(b.sas_date, a.cut_date) <= {months_ahead * 35}"),
        how="left"
    ).select(
        col("a.cut_date"),
        col("a.Federal_Funds_Rate"),
        col("a.Basis_Points"),
        col("a.sp500_at_cut"),
        col("b.avg_close").alias(f"sp500_{months_ahead}m_later")
    ).groupBy("cut_date", "Federal_Funds_Rate", "Basis_Points", "sp500_at_cut") \
     .agg(avg(f"sp500_{months_ahead}m_later").alias(f"sp500_{months_ahead}m_later"))

# Calculate 1, 3, and 6 month forward returns
cut_impact_1m = calculate_forward_returns(rate_cut_base, merged_analysis, 1, "return_1m")
cut_impact_3m = calculate_forward_returns(rate_cut_base, merged_analysis, 3, "return_3m")
cut_impact_6m = calculate_forward_returns(rate_cut_base, merged_analysis, 6, "return_6m")

# Combine all forward returns
cut_impact_analysis = cut_impact_1m \
    .join(cut_impact_3m, on=["cut_date", "Federal_Funds_Rate", "Basis_Points", "sp500_at_cut"], how="left") \
    .join(cut_impact_6m, on=["cut_date", "Federal_Funds_Rate", "Basis_Points", "sp500_at_cut"], how="left")

# Calculate actual returns
cut_impact_analysis = cut_impact_analysis \
    .withColumn("return_1m",
                when(col("sp500_1m_later").isNotNull(),
                     ((col("sp500_1m_later") - col("sp500_at_cut")) / col("sp500_at_cut") * 100))
                .otherwise(None)) \
    .withColumn("return_3m",
                when(col("sp500_3m_later").isNotNull(),
                     ((col("sp500_3m_later") - col("sp500_at_cut")) / col("sp500_at_cut") * 100))
                .otherwise(None)) \
    .withColumn("return_6m",
                when(col("sp500_6m_later").isNotNull(),
                     ((col("sp500_6m_later") - col("sp500_at_cut")) / col("sp500_at_cut") * 100))
                .otherwise(None))

# ============================================================================
# CORRELATION ANALYSIS - Replaces SAS PROC CORR
# ============================================================================

print("\n[7/10] Performing correlation analysis (SAS PROC CORR equivalent)...")

# Prepare data for correlation (remove nulls)
corr_data = merged_analysis.select(
    "Federal_Funds_Rate",
    "Rate_Change",
    "Basis_Points",
    "avg_close",
    "avg_daily_return",
    "volatility",
    "sp500_monthly_change"
).na.drop()

# Create correlation matrix using VectorAssembler
feature_cols = ["Federal_Funds_Rate", "Rate_Change", "Basis_Points"]
target_cols = ["avg_close", "avg_daily_return", "volatility", "sp500_monthly_change"]
all_cols = feature_cols + target_cols

assembler = VectorAssembler(inputCols=all_cols, outputCol="features")
vector_df = assembler.transform(corr_data).select("features")

# Calculate Pearson correlation matrix
correlation_matrix = Correlation.corr(vector_df, "features", "pearson").head()[0]
corr_array = correlation_matrix.toArray()

# Create correlation results DataFrame
correlation_results = []
for i, feat in enumerate(feature_cols):
    for j, tgt in enumerate(target_cols):
        correlation_results.append({
            "Feature": feat,
            "Target": tgt,
            "Pearson_Correlation": float(corr_array[i][len(feature_cols) + j])
        })

correlation_df = spark.createDataFrame(correlation_results)

print("\n   Correlation Results:")
correlation_df.show(truncate=False)

# ============================================================================
# REGRESSION ANALYSIS - Replaces SAS PROC REG
# ============================================================================

print("\n[8/10] Building regression model (SAS PROC REG equivalent)...")

# Prepare data for regression
reg_data = merged_analysis.select(
    "sp500_monthly_change",
    "Rate_Change",
    "Basis_Points",
    "Federal_Funds_Rate"
).na.drop()

# Assemble features
reg_assembler = VectorAssembler(
    inputCols=["Rate_Change", "Basis_Points", "Federal_Funds_Rate"],
    outputCol="features"
)

reg_assembled = reg_assembler.transform(reg_data)

# Split data for training (using all data for this analysis)
train_data = reg_assembled

# Build Linear Regression Model
lr = LinearRegression(
    featuresCol="features",
    labelCol="sp500_monthly_change",
    predictionCol="predicted_change"
)

# Train model
lr_model = lr.fit(train_data)

# Get model statistics
print(f"\n   Model Statistics:")
print(f"   R-Squared: {lr_model.summary.r2:.4f}")
print(f"   Adjusted R-Squared: {lr_model.summary.r2adj:.4f}")
print(f"   RMSE: {lr_model.summary.rootMeanSquaredError:.4f}")
print(f"   MAE: {lr_model.summary.meanAbsoluteError:.4f}")

print(f"\n   Regression Coefficients:")
print(f"   Intercept: {lr_model.intercept:.4f}")
coefficients = lr_model.coefficients
print(f"   Rate_Change: {coefficients[0]:.4f}")
print(f"   Basis_Points: {coefficients[1]:.4f}")
print(f"   Federal_Funds_Rate: {coefficients[2]:.4f}")

# Make predictions
predictions = lr_model.transform(reg_assembled)

# Calculate residuals
predictions = predictions.withColumn(
    "residual",
    col("sp500_monthly_change") - col("predicted_change")
)

# ============================================================================
# SUMMARY STATISTICS - Replaces SAS PROC MEANS with CLASS
# ============================================================================

print("\n[9/10] Calculating summary statistics (SAS PROC MEANS with CLASS equivalent)...")

cut_summary = rate_cut_events \
    .groupBy("significant_cut") \
    .agg(
        count("*").alias("n"),
        avg("Basis_Points").alias("mean_basis_points"),
        avg("avg_daily_return").alias("mean_return"),
        avg("volatility").alias("mean_volatility"),
        avg("sp500_monthly_change").alias("mean_monthly_change"),
        stddev("Basis_Points").alias("std_basis_points")
    )

print("\n   Summary by Cut Significance:")
cut_summary.show()

# ============================================================================
# EXPORT RESULTS - Replaces SAS PROC EXPORT
# ============================================================================

print("\n[10/10] Exporting results (SAS PROC EXPORT equivalent)...")

# Convert to Pandas for export (for small datasets)
# For large datasets, use df.write.csv()

merged_analysis.toPandas().to_csv(
    "/home/user/makkena.github.io/results/pyspark_merged_analysis.csv",
    index=False
)

cut_impact_analysis.toPandas().to_csv(
    "/home/user/makkena.github.io/results/pyspark_cut_impact_analysis.csv",
    index=False
)

correlation_df.toPandas().to_csv(
    "/home/user/makkena.github.io/results/pyspark_correlation_results.csv",
    index=False
)

# Create regression parameters DataFrame
reg_params = pd.DataFrame({
    "Parameter": ["Intercept", "Rate_Change", "Basis_Points", "Federal_Funds_Rate"],
    "Estimate": [lr_model.intercept] + list(lr_model.coefficients),
    "R_Squared": [lr_model.summary.r2] * 4,
    "Adj_R_Squared": [lr_model.summary.r2adj] * 4,
    "RMSE": [lr_model.summary.rootMeanSquaredError] * 4
})

reg_params.to_csv(
    "/home/user/makkena.github.io/results/pyspark_regression_parameters.csv",
    index=False
)

predictions.toPandas().to_csv(
    "/home/user/makkena.github.io/results/pyspark_predictions.csv",
    index=False
)

# ============================================================================
# ANALYSIS SUMMARY
# ============================================================================

print("\n" + "="*80)
print("PYSPARK ANALYSIS COMPLETE")
print("="*80)
print("\nKey Insights:")
print(f"1. Total rate cut events analyzed: {rate_cut_events.count()}")
print(f"2. Correlation (Rate Change vs S&P Monthly Change): {correlation_results[1]['Pearson_Correlation']:.4f}")
print(f"3. Regression R-Squared: {lr_model.summary.r2:.4f}")
print(f"4. Average return 1-month after cuts: {cut_impact_analysis.agg(avg('return_1m')).first()[0]:.2f}%")
print("\nAll results exported to /home/user/makkena.github.io/results/")
print("="*80)

# Stop Spark session
spark.stop()
