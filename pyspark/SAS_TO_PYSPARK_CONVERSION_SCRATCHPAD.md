# SAS to PySpark Conversion Scratchpad
## Wealth Management Analysis - Federal Reserve & S&P 500

---

## 📋 Overview

This document details the conversion of SAS code to PySpark for analyzing the correlation between Federal Reserve rate cuts and S&P 500 performance. It includes library mappings, custom UDFs, and implementation notes.

**Analysis Period:** 2020-2025
**Source Language:** SAS
**Target Language:** PySpark (Python)
**Conversion Date:** November 2025

---

## 🔄 Major SAS Procedure to PySpark Mappings

### 1. Data Import and Export

| SAS Procedure | PySpark Equivalent | Notes |
|--------------|-------------------|-------|
| `PROC IMPORT` | `spark.read.csv()` | PySpark uses SparkSession reader with schema inference |
| `PROC EXPORT` | `df.write.csv()` or `.toPandas().to_csv()` | For large data use native Spark write, for small data convert to Pandas |
| `LIBNAME` statement | Not needed | PySpark reads directly from file paths or data sources |

**Example Conversion:**

```sas
/* SAS */
proc import datafile="/path/to/fed_rates.csv"
    out=feddata.fed_rates
    dbms=csv
    replace;
    getnames=yes;
run;
```

```python
# PySpark
fed_rates_df = spark.read.csv(
    "/path/to/fed_rates.csv",
    header=True,
    inferSchema=True
)
```

---

### 2. Data Manipulation

| SAS Procedure | PySpark Equivalent | Notes |
|--------------|-------------------|-------|
| `DATA` step | `.withColumn()`, `.select()`, `.filter()` | Chained transformations on DataFrame |
| `SET` statement | DataFrame variable | DataFrames are immutable in Spark |
| `LAG()` function | `lag()` with Window | Requires Window specification |
| `RETAIN` statement | Window functions with cumulative operations | More complex in PySpark |
| `IF-THEN-ELSE` | `when().otherwise()` | Conditional column creation |
| `INPUT()` for date conversion | `to_date()`, `date_format()` | PySpark has dedicated date functions |
| `PUT()` for formatting | `date_format()`, `format_string()` | Various formatting functions available |

**Example Conversion:**

```sas
/* SAS - Calculate daily returns with LAG */
data sp500_returns;
    set sp500_clean;
    daily_return = (Close - lag(Close)) / lag(Close) * 100;
run;
```

```python
# PySpark
from pyspark.sql.functions import lag, col
from pyspark.sql.window import Window

window_spec = Window.orderBy("date")

sp500_returns = sp500_clean.withColumn(
    "prev_close",
    lag("Close", 1).over(window_spec)
).withColumn(
    "daily_return",
    ((col("Close") - col("prev_close")) / col("prev_close") * 100)
)
```

---

### 3. Aggregation and Summary Statistics

| SAS Procedure | PySpark Equivalent | Notes |
|--------------|-------------------|-------|
| `PROC MEANS` | `.groupBy().agg()` | Multiple aggregations in one call |
| `PROC SUMMARY` | `.groupBy().agg()` | Same as PROC MEANS |
| `PROC FREQ` | `.groupBy().count()` | For frequency tables |
| `NWAY` option | Standard behavior in PySpark | All combinations by default |
| `CLASS` statement | `.groupBy()` columns | Specify grouping variables |
| `VAR` statement | Aggregation functions | Applied in `.agg()` |
| `OUTPUT OUT=` | Result DataFrame | Automatic in PySpark |

**Example Conversion:**

```sas
/* SAS - Monthly aggregation */
proc means data=sp500_clean noprint nway;
    class year_month;
    var Close daily_return Volume;
    output out=sp500_monthly
        mean(Close)=avg_close
        std(daily_return)=volatility
        sum(Volume)=total_volume;
run;
```

```python
# PySpark
from pyspark.sql.functions import avg, stddev, sum as spark_sum

sp500_monthly = sp500_clean.groupBy("year_month").agg(
    avg("Close").alias("avg_close"),
    stddev("daily_return").alias("volatility"),
    spark_sum("Volume").alias("total_volume")
)
```

---

### 4. SQL Operations

| SAS Procedure | PySpark Equivalent | Notes |
|--------------|-------------------|-------|
| `PROC SQL` | `.join()`, `.filter()`, `.select()` | DataFrame API or Spark SQL |
| `SELECT` statement | `.select()` | Column selection |
| `CREATE TABLE AS` | DataFrame assignment | Immutable DataFrames |
| `WHERE` clause | `.filter()` or `.where()` | Both work identically |
| `ORDER BY` | `.orderBy()` or `.sort()` | Both work identically |
| `GROUP BY` | `.groupBy()` | Same syntax |
| `JOIN` operations | `.join()` | Support for all join types |
| `QUIT;` | Not needed | No explicit quit |

**Example Conversion:**

```sas
/* SAS - Join with aggregation */
proc sql;
    create table merged_analysis as
    select
        a.year_month,
        a.Federal_Funds_Rate,
        b.avg_close,
        (b.avg_close - lag(b.avg_close)) / lag(b.avg_close) * 100 as sp500_change
    from fed_rates a
    left join sp500_monthly b
    on a.year_month = b.year_month
    order by a.date;
quit;
```

```python
# PySpark
merged_analysis = fed_rates.join(
    sp500_monthly,
    on="year_month",
    how="left"
).orderBy("date")

# Add lagged calculation
window_spec = Window.orderBy("date")
merged_analysis = merged_analysis.withColumn(
    "prev_avg_close",
    lag("avg_close", 1).over(window_spec)
).withColumn(
    "sp500_change",
    ((col("avg_close") - col("prev_avg_close")) / col("prev_avg_close") * 100)
)
```

---

### 5. Statistical Analysis

| SAS Procedure | PySpark Equivalent | Library/Module | Notes |
|--------------|-------------------|---------------|-------|
| `PROC CORR` | `Correlation.corr()` | `pyspark.ml.stat` | Requires VectorAssembler |
| `PROC REG` | `LinearRegression` | `pyspark.ml.regression` | ML library for regression |
| `PROC ANOVA` | Statistical functions or MLlib | `pyspark.ml` | Limited native support |
| `PROC TIMESERIES` | Window functions + custom logic | `pyspark.sql.functions` | No direct equivalent |
| Pearson correlation | `Correlation.corr(..., "pearson")` | `pyspark.ml.stat` | Matrix correlation |
| Spearman correlation | `Correlation.corr(..., "spearman")` | `pyspark.ml.stat` | Rank correlation |

**Example Conversion:**

```sas
/* SAS - Correlation analysis */
proc corr data=merged_analysis pearson;
    var Federal_Funds_Rate Rate_Change;
    with avg_close avg_daily_return;
run;
```

```python
# PySpark
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation

# Prepare features
assembler = VectorAssembler(
    inputCols=["Federal_Funds_Rate", "Rate_Change", "avg_close", "avg_daily_return"],
    outputCol="features"
)
vector_df = assembler.transform(merged_analysis).select("features")

# Calculate correlation
correlation_matrix = Correlation.corr(vector_df, "features", "pearson")
```

---

```sas
/* SAS - Linear Regression */
proc reg data=merged_analysis;
    model sp500_change = Rate_Change Basis_Points Federal_Funds_Rate;
    output out=predictions predicted=pred_change residual=resid;
quit;
```

```python
# PySpark
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler

# Prepare features
assembler = VectorAssembler(
    inputCols=["Rate_Change", "Basis_Points", "Federal_Funds_Rate"],
    outputCol="features"
)
train_data = assembler.transform(merged_analysis)

# Build model
lr = LinearRegression(
    featuresCol="features",
    labelCol="sp500_change",
    predictionCol="pred_change"
)

# Train and predict
lr_model = lr.fit(train_data)
predictions = lr_model.transform(train_data)

# Get statistics
print(f"R-Squared: {lr_model.summary.r2}")
print(f"Coefficients: {lr_model.coefficients}")
```

---

### 6. Visualization

| SAS Procedure | PySpark Equivalent | Notes |
|--------------|-------------------|-------|
| `PROC SGPLOT` | Export to Pandas + Matplotlib/Plotly | No native plotting in PySpark |
| `SERIES` statement | `.plot()` on Pandas DataFrame | Convert to Pandas first |
| `SCATTER` statement | Matplotlib `scatter()` | Use after conversion |
| `VBAR/HBAR` | Bar plots in Matplotlib | Use after conversion |
| `ODS HTML` | Export to CSV/JSON then visualize | Multi-step process |

**Approach:**

```python
# Convert small results to Pandas for visualization
import matplotlib.pyplot as plt

pandas_df = spark_df.toPandas()
pandas_df.plot(x='date', y='sp500_close', kind='line')
plt.show()
```

---

## 🔧 Custom UDFs (User Defined Functions)

### Why UDFs are Needed

PySpark doesn't have direct equivalents for some SAS custom functions and complex IF-THEN logic. UDFs fill this gap but should be used sparingly due to performance overhead.

### Custom UDF #1: Rate Change Classification

**SAS Code:**
```sas
data classified;
    set fed_rates;
    if rate_change < -0.25 then change_category = "Significant Cut";
    else if rate_change < 0 then change_category = "Minor Cut";
    else if rate_change > 0.25 then change_category = "Significant Hike";
    else if rate_change > 0 then change_category = "Minor Hike";
    else change_category = "Hold";
run;
```

**PySpark UDF:**
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def classify_rate_change(rate_change):
    """Classify rate changes into categories"""
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

# Usage
df = df.withColumn("change_category", classify_rate_change(col("rate_change")))
```

**Note:** This UDF could be replaced with nested `when().otherwise()` for better performance:

```python
from pyspark.sql.functions import when, col

df = df.withColumn(
    "change_category",
    when(col("rate_change") < -0.25, "Significant Cut")
    .when(col("rate_change") < 0, "Minor Cut")
    .when(col("rate_change") > 0.25, "Significant Hike")
    .when(col("rate_change") > 0, "Minor Hike")
    .otherwise("Hold")
)
```

---

### Custom UDF #2: Volatility Scoring

**SAS Code:**
```sas
proc sql;
    select avg(volatility) into :avg_vol from sp500_data;
quit;

data scored;
    set sp500_data;
    volatility_score = (volatility - &avg_vol) / &avg_vol * 100;
run;
```

**PySpark UDF:**
```python
@udf(returnType=DoubleType())
def calculate_volatility_score(volatility, avg_volatility):
    """Calculate normalized volatility score"""
    if volatility is None or avg_volatility is None:
        return 0.0
    return (volatility - avg_volatility) / avg_volatility * 100

# Calculate average volatility first
avg_volatility = sp500_data.agg(avg("volatility")).first()[0]

# Apply UDF
df = df.withColumn(
    "volatility_score",
    calculate_volatility_score(col("volatility"), lit(avg_volatility))
)
```

**Better Alternative (Avoiding UDF):**
```python
from pyspark.sql.functions import avg, lit

# Calculate average
avg_volatility = sp500_data.agg(avg("volatility")).first()[0]

# Use native functions
df = df.withColumn(
    "volatility_score",
    ((col("volatility") - lit(avg_volatility)) / lit(avg_volatility) * 100)
)
```

---

### Custom UDF #3: Forward Date Calculation (INTNX Equivalent)

**SAS Code:**
```sas
data future_dates;
    set base_dates;
    date_1m = intnx('month', date, 1);
    date_3m = intnx('month', date, 3);
    date_6m = intnx('month', date, 6);
    format date_1m date_3m date_6m date9.;
run;
```

**PySpark Approach:**
```python
from pyspark.sql.functions import add_months

# No UDF needed - use built-in function
df = df.withColumn("date_1m", add_months(col("date"), 1)) \
       .withColumn("date_3m", add_months(col("date"), 3)) \
       .withColumn("date_6m", add_months(col("date"), 6))
```

---

## 📊 Library Conversion Table

| SAS Component | PySpark Library | Import Statement |
|--------------|----------------|------------------|
| Base SAS | `pyspark.sql` | `from pyspark.sql import SparkSession` |
| PROC SQL | `pyspark.sql.functions` | `from pyspark.sql.functions import *` |
| PROC MEANS/SUMMARY | `pyspark.sql.functions` | `from pyspark.sql.functions import avg, sum, count, etc.` |
| PROC CORR | `pyspark.ml.stat.Correlation` | `from pyspark.ml.stat import Correlation` |
| PROC REG | `pyspark.ml.regression.LinearRegression` | `from pyspark.ml.regression import LinearRegression` |
| Date functions | `pyspark.sql.functions` | `from pyspark.sql.functions import to_date, date_format` |
| LAG/LEAD | `pyspark.sql.window.Window` | `from pyspark.sql.window import Window` |
| Data types | `pyspark.sql.types` | `from pyspark.sql.types import StringType, DoubleType` |
| Feature engineering | `pyspark.ml.feature` | `from pyspark.ml.feature import VectorAssembler` |

---

## ⚠️ Key Differences and Gotchas

### 1. **Immutability**
- **SAS:** Datasets can be modified in place
- **PySpark:** DataFrames are immutable; transformations create new DataFrames

### 2. **Lazy Evaluation**
- **SAS:** Executes immediately
- **PySpark:** Builds execution plan, runs only when action is called (`.show()`, `.count()`, `.write()`)

### 3. **Null Handling**
- **SAS:** Uses `.` for numeric nulls
- **PySpark:** Uses `None` or `null`; requires explicit `.na.drop()` or `.na.fill()`

### 4. **Window Functions**
- **SAS:** `LAG()` and `RETAIN` work automatically
- **PySpark:** Requires explicit Window specification with `partitionBy()` and `orderBy()`

### 5. **Date Arithmetic**
- **SAS:** `INTNX()`, `INTCK()` functions
- **PySpark:** `add_months()`, `datediff()`, `months_between()`

### 6. **Array Indexing**
- **SAS:** 1-based indexing
- **PySpark:** 0-based indexing (Python convention)

### 7. **Performance**
- **SAS:** Optimized for single-machine processing
- **PySpark:** Distributed processing; requires partitioning strategy

### 8. **UDF Performance**
- **SAS:** Custom functions are optimized
- **PySpark:** UDFs have overhead; prefer native functions when possible

---

## 🚀 Performance Optimization Notes

### 1. Avoid UDFs When Possible
```python
# BAD (using UDF)
@udf(returnType=DoubleType())
def calculate_return(close, prev_close):
    return (close - prev_close) / prev_close * 100

df = df.withColumn("return", calculate_return(col("close"), col("prev_close")))

# GOOD (using native functions)
df = df.withColumn("return", ((col("close") - col("prev_close")) / col("prev_close") * 100))
```

### 2. Use Broadcast Joins for Small Tables
```python
from pyspark.sql.functions import broadcast

# If one table is small (<10MB)
result = large_df.join(broadcast(small_df), on="key", how="left")
```

### 3. Partition Data Appropriately
```python
# Repartition before heavy operations
df = df.repartition(100, "year_month")
```

### 4. Cache Intermediate Results
```python
# Cache frequently accessed DataFrames
merged_analysis = fed_rates.join(sp500_data, on="date")
merged_analysis.cache()

# Use multiple times
result1 = merged_analysis.filter(...)
result2 = merged_analysis.groupBy(...)
```

### 5. Use Columnar Operations
```python
# GOOD - vectorized operations
df = df.withColumn("new_col", col("a") * col("b") + col("c"))

# BAD - row-by-row UDF
```

---

## 📈 Analysis-Specific Conversions

### Forward Returns Calculation

**Challenge:** SAS `INTNX` for forward-looking joins is straightforward. PySpark requires date arithmetic and self-joins.

**SAS Approach:**
```sas
proc sql;
    create table forward_returns as
    select
        a.date,
        a.sp500_close as close_at_cut,
        b.sp500_close as close_1m_later
    from rate_cuts a
    left join sp500_data b
    on intnx('month', a.date, 1) = b.date;
quit;
```

**PySpark Approach:**
```python
from pyspark.sql.functions import add_months, datediff, expr

# Method 1: Using add_months and date match
forward_returns = rate_cuts.alias("a").join(
    sp500_data.alias("b"),
    add_months(col("a.date"), 1) == col("b.date"),
    how="left"
).select(
    col("a.date"),
    col("a.sp500_close").alias("close_at_cut"),
    col("b.sp500_close").alias("close_1m_later")
)

# Method 2: Using date range (more flexible)
forward_returns = rate_cuts.alias("a").join(
    sp500_data.alias("b"),
    expr("datediff(b.date, a.date) >= 28 AND datediff(b.date, a.date) <= 35"),
    how="left"
).groupBy("a.date", "a.sp500_close").agg(
    avg("b.sp500_close").alias("close_1m_later")
)
```

---

### Correlation Matrix Display

**Challenge:** SAS `PROC CORR` outputs formatted tables. PySpark returns arrays.

**SAS Output:**
```
Pearson Correlation Coefficients
                      Fed_Rate    Rate_Change    SP500_Change
Fed_Rate              1.0000      0.8234        -0.4521
Rate_Change           0.8234      1.0000        -0.5678
SP500_Change         -0.4521     -0.5678         1.0000
```

**PySpark Conversion:**
```python
# Get correlation matrix
corr_matrix = Correlation.corr(vector_df, "features").head()[0].toArray()

# Create readable DataFrame
import pandas as pd
columns = ["Fed_Rate", "Rate_Change", "SP500_Change"]
corr_df = pd.DataFrame(corr_matrix, columns=columns, index=columns)
print(corr_df)
```

---

## 📝 Summary of Key Conversions

| Task | SAS Lines | PySpark Lines | Complexity |
|------|-----------|---------------|------------|
| Data Import | 5 | 3 | Low |
| Date Conversion | 3 | 2 | Low |
| LAG Function | 2 | 5 | Medium |
| Aggregation | 6 | 4 | Low |
| SQL Join | 8 | 3 | Low |
| Correlation | 4 | 8 | Medium |
| Regression | 5 | 12 | High |
| Forward Returns | 10 | 15 | High |
| Visualization | 8 | N/A (external) | High |

**Total Conversion Ratio:** ~1.5x more lines in PySpark, but with better scalability

---

## ✅ Testing and Validation

### Validate Results Match SAS

```python
# 1. Check record counts
print(f"SAS record count: {sas_count}")
print(f"PySpark record count: {pyspark_df.count()}")

# 2. Compare aggregation results
sas_avg = 4532.15  # From SAS output
pyspark_avg = pyspark_df.agg(avg("close")).first()[0]
print(f"Difference: {abs(sas_avg - pyspark_avg)}")

# 3. Compare correlation values (within tolerance)
assert abs(sas_corr - pyspark_corr) < 0.01, "Correlation mismatch!"

# 4. Validate regression coefficients
print("Coefficient comparison:")
print(f"SAS Intercept: {sas_intercept}")
print(f"PySpark Intercept: {lr_model.intercept}")
```

---

## 🎯 Conclusion

The conversion from SAS to PySpark requires:

1. **Mindset Shift:** From procedural to functional/distributed programming
2. **Library Knowledge:** Understanding PySpark equivalents for SAS procedures
3. **Custom Logic:** Implementing UDFs only when native functions don't suffice
4. **Performance Tuning:** Leveraging Spark's distributed architecture
5. **Testing:** Validating results match SAS output

**Key Takeaway:** While PySpark may require more verbose code in some cases, it offers superior scalability, integration with modern data pipelines, and better support for large-scale analytics.

---

*Document Version: 1.0*
*Last Updated: November 2025*
*Author: AI-Powered Conversion Assistant*
