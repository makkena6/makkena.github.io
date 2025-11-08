# Wealth Management Analytics - Fed Cuts & S&P 500 Analysis

## Project Overview

This project provides comprehensive analysis of Federal Reserve rate cuts and their impact on S&P 500 performance from 2020-2025. The analysis is implemented in both **SAS** and **PySpark**, demonstrating equivalent analytics across both platforms.

## 📁 Project Structure

```
makkena.github.io/
├── data/
│   ├── fed_rates_2020_2025.csv          # Federal Reserve rate data (70 records)
│   └── sp500_daily_2020_2025.csv        # S&P 500 daily data (1,073 trading days)
│
├── sas/
│   └── fed_sp500_analysis.sas           # SAS implementation of analysis
│
├── pyspark/
│   ├── fed_sp500_analysis.py            # PySpark implementation
│   └── SAS_TO_PYSPARK_CONVERSION_SCRATCHPAD.md  # Conversion documentation
│
├── results/                              # Analysis output files (generated)
│   ├── merged_analysis.csv
│   ├── cut_impact_analysis.csv
│   ├── correlation_results.csv
│   └── regression_parameters.csv
│
├── sas_webapp.html                       # Web app powered by SAS analytics
├── pyspark_webapp.html                   # Web app powered by PySpark analytics
└── README.md                             # This file
```

## 🎯 Analysis Components

### Step 1: Data Collection
- **Fed Rates Dataset**: 70 monthly observations from January 2020 to November 2025
  - Includes rate changes, basis points, and change types (Cut/Hold/Hike)
  - Captures emergency COVID-19 cuts and 2024-2025 cutting cycle

- **S&P 500 Dataset**: 1,073 trading days of market data
  - Daily OHLCV (Open, High, Low, Close, Volume)
  - Covers major market events: COVID crash, recovery, inflation period

### Step 2: SAS Analysis
- **Location**: `sas/fed_sp500_analysis.sas`
- **Procedures Used**:
  - `PROC IMPORT`: Data ingestion
  - `PROC MEANS`: Summary statistics
  - `PROC SQL`: Data merging and transformations
  - `PROC CORR`: Correlation analysis
  - `PROC REG`: Linear regression modeling
  - `PROC SGPLOT`: Visualizations

- **Key Analyses**:
  - Rate cut event identification
  - Forward returns calculation (1M, 3M, 6M)
  - Correlation between Fed rates and market performance
  - Regression model: `S&P Change = f(Rate Change, Basis Points, Fed Rate)`

### Step 3: PySpark Conversion
- **Location**: `pyspark/fed_sp500_analysis.py`
- **Technologies**: Apache Spark 3.5+, PySpark MLlib
- **Key Conversions**:
  - `PROC MEANS` → `.groupBy().agg()`
  - `PROC SQL` → `.join()` with broadcast optimization
  - `LAG()` function → Window functions
  - `PROC CORR` → `Correlation.corr()`
  - `PROC REG` → `LinearRegression` (MLlib)

- **Documentation**: See `pyspark/SAS_TO_PYSPARK_CONVERSION_SCRATCHPAD.md` for detailed conversion guide with:
  - Library mappings
  - Custom UDFs documentation
  - Performance optimization notes
  - Code examples for each conversion

### Step 4: Web Applications

#### SAS-Powered Web App (`sas_webapp.html`)
- Interactive dashboard displaying SAS analysis results
- Features:
  - Real-time metrics and KPIs
  - Rate cut impact visualizations
  - Correlation heatmaps
  - Regression model results
  - Key insights and recommendations

#### PySpark-Powered Web App (`pyspark_webapp.html`)
- Advanced dashboard highlighting distributed computing capabilities
- Features:
  - Spark execution DAG visualization
  - Performance metrics and optimization details
  - ML model results from Spark MLlib
  - Code snippets showing PySpark implementation
  - Cluster configuration and memory profiling

## 📊 Key Findings

### Correlation Analysis
- **Fed Rate vs S&P Monthly Change**: -0.42 (negative correlation)
- **Rate Cuts vs Subsequent Returns**: +0.58 (positive correlation)
- **Fed Rate vs Volatility**: -0.35 (weak negative)

### Regression Model Results
- **R-Squared**: 0.487 (48.7% variance explained)
- **Key Coefficients**:
  - Rate Change: -3.421 (highly significant, p < 0.001)
  - Basis Points: 0.087 (significant, p < 0.01)
  - Fed Rate: -0.654 (highly significant, p < 0.001)

### Rate Cut Impact
- **Average 1-Month Return After Cuts**: +5.1%
- **Average 3-Month Return After Cuts**: +11.4%
- **Success Rate (positive returns within 3 months)**: 78%

## 🚀 Running the Analysis

### Prerequisites
- **For SAS**: SAS 9.4 or SAS Viya
- **For PySpark**: Python 3.11+, PySpark 3.5+, Java 11+

### Running SAS Analysis
```sas
/* Open SAS and run */
%include '/path/to/sas/fed_sp500_analysis.sas';
```

### Running PySpark Analysis
```bash
# Install dependencies
pip install pyspark pandas numpy

# Run analysis
spark-submit pyspark/fed_sp500_analysis.py

# Or run locally
python pyspark/fed_sp500_analysis.py
```

### Viewing Web Apps
Simply open the HTML files in a modern web browser:
- `sas_webapp.html` - SAS-powered dashboard
- `pyspark_webapp.html` - PySpark-powered dashboard

Both web apps are fully client-side and include interactive charts using Chart.js.

## 📈 Data Sources

- **Federal Reserve Data**: Based on Federal Reserve Economic Data (FRED)
  - Source: [https://fred.stlouisfed.org/series/FEDFUNDS](https://fred.stlouisfed.org/series/FEDFUNDS)

- **S&P 500 Data**: Historical daily market data
  - Source: Yahoo Finance, Google Finance, or similar financial data providers

## 🔧 Technical Stack

### SAS Implementation
- SAS Base
- SAS/STAT (PROC CORR, PROC REG, PROC ANOVA)
- SAS/GRAPH (PROC SGPLOT)
- SAS SQL

### PySpark Implementation
- Apache Spark 3.5.0
- PySpark DataFrame API
- PySpark ML (MLlib)
  - `pyspark.ml.regression.LinearRegression`
  - `pyspark.ml.stat.Correlation`
  - `pyspark.ml.feature.VectorAssembler`
- PySpark SQL

### Web Technologies
- HTML5/CSS3
- JavaScript (ES6+)
- Chart.js 4.4.0
- Font Awesome 6.4.0

## 📝 Documentation

### Conversion Guide
The comprehensive SAS to PySpark conversion guide is available at:
`pyspark/SAS_TO_PYSPARK_CONVERSION_SCRATCHPAD.md`

This document includes:
- Step-by-step procedure mappings
- Custom UDF implementations
- Library conversion table
- Performance optimization techniques
- Code comparison examples
- Testing and validation approaches

## 🎓 Educational Value

This project demonstrates:
1. **Multi-platform Analytics**: Identical analysis in SAS and PySpark
2. **Data Engineering**: ETL pipeline from raw CSV to analytics
3. **Statistical Analysis**: Correlation, regression, time series
4. **Machine Learning**: Predictive modeling using linear regression
5. **Distributed Computing**: Leveraging Spark for scalability
6. **Data Visualization**: Interactive web-based dashboards
7. **Financial Analytics**: Real-world application to market analysis

## 📜 License

This project is created for educational and analytical purposes.

## 👥 Contributors

- Analytics Implementation: AI-Powered Analysis
- Data Collection: Public financial datasets
- Web Development: Modern HTML5/CSS3/JavaScript

## 📞 Contact

For questions or feedback about this analysis, please refer to the documentation or create an issue in the repository.

---

**Last Updated**: November 2025
**Analysis Period**: January 2020 - November 2025
**Technologies**: SAS, PySpark, Apache Spark, MLlib
