/*******************************************************************
 * Wealth Management Analysis - Fed Cuts Impact on S&P 500
 * Analysis of Federal Reserve Rate Cuts and S&P 500 Performance
 * Period: 2020-2025
 *******************************************************************/

/* Set up library and file paths */
libname feddata "/home/user/makkena.github.io/data";
options validvarname=any;

/* Import Federal Funds Rate Data */
proc import datafile="/home/user/makkena.github.io/data/fed_rates_2020_2025.csv"
    out=feddata.fed_rates
    dbms=csv
    replace;
    getnames=yes;
run;

/* Import S&P 500 Daily Data */
proc import datafile="/home/user/makkena.github.io/data/sp500_daily_2020_2025.csv"
    out=feddata.sp500_daily
    dbms=csv
    replace;
    getnames=yes;
run;

/* Data Preparation - Convert dates to SAS date format */
data fed_rates_clean;
    set feddata.fed_rates;
    sas_date = input(Date, yymmdd10.);
    format sas_date date9.;
    year_month = put(sas_date, yymmn6.);
run;

data sp500_clean;
    set feddata.sp500_daily;
    sas_date = input(Date, yymmdd10.);
    format sas_date date9.;
    year_month = put(sas_date, yymmn6.);

    /* Calculate daily return */
    daily_return = (Close - lag(Close)) / lag(Close) * 100;

    /* Calculate 30-day moving average */
    retain sum30 count30;
    if _n_ = 1 then do;
        sum30 = 0;
        count30 = 0;
    end;
run;

/* Calculate monthly S&P 500 statistics */
proc means data=sp500_clean noprint nway;
    class year_month;
    var Close daily_return Volume;
    output out=sp500_monthly
        mean(Close)=avg_close
        mean(daily_return)=avg_daily_return
        std(daily_return)=volatility
        min(Close)=min_close
        max(Close)=max_close
        sum(Volume)=total_volume;
run;

/* Merge Fed Rates with S&P 500 monthly data */
proc sql;
    create table merged_analysis as
    select
        a.year_month,
        a.sas_date as fed_date,
        a.Federal_Funds_Rate,
        a.Rate_Change,
        a.Change_Type,
        a.Basis_Points,
        b.avg_close,
        b.avg_daily_return,
        b.volatility,
        b.min_close,
        b.max_close,
        b.total_volume,
        /* Calculate percentage change in S&P 500 */
        (b.avg_close - lag(b.avg_close)) / lag(b.avg_close) * 100 as sp500_monthly_change
    from fed_rates_clean a
    left join sp500_monthly b
    on a.year_month = b.year_month
    order by a.sas_date;
quit;

/* Identify Rate Cut Events and their impact */
data rate_cut_events;
    set merged_analysis;
    where Change_Type in ('Cut', 'Emergency Cut');

    /* Flag significant cuts (>= 25 basis points) */
    if abs(Basis_Points) >= 25 then significant_cut = 1;
    else significant_cut = 0;
run;

/* Calculate S&P 500 performance after rate cuts */
/* Look at 1-month, 3-month, and 6-month forward returns */
proc sql;
    create table cut_impact_analysis as
    select
        a.fed_date,
        a.Federal_Funds_Rate,
        a.Basis_Points,
        a.avg_close as sp500_at_cut,
        b.avg_close as sp500_1m_later,
        c.avg_close as sp500_3m_later,
        d.avg_close as sp500_6m_later,
        /* Calculate returns */
        (b.avg_close - a.avg_close) / a.avg_close * 100 as return_1m,
        (c.avg_close - a.avg_close) / a.avg_close * 100 as return_3m,
        (d.avg_close - a.avg_close) / a.avg_close * 100 as return_6m
    from rate_cut_events a
    left join merged_analysis b on intnx('month', a.fed_date, 1) = b.fed_date
    left join merged_analysis c on intnx('month', a.fed_date, 3) = c.fed_date
    left join merged_analysis d on intnx('month', a.fed_date, 6) = d.fed_date;
quit;

/* Correlation Analysis */
proc corr data=merged_analysis pearson spearman;
    var Federal_Funds_Rate Rate_Change Basis_Points;
    with avg_close avg_daily_return volatility sp500_monthly_change;
    ods output PearsonCorr=pearson_corr SpearmanCorr=spearman_corr;
run;

/* Regression Analysis - Impact of Rate Changes on S&P 500 */
proc reg data=merged_analysis outest=reg_estimates;
    model sp500_monthly_change = Rate_Change Basis_Points Federal_Funds_Rate;
    output out=reg_output predicted=predicted_change residual=residual;
    ods output ParameterEstimates=param_est FitStatistics=fit_stats;
quit;

/* Summary Statistics for Rate Cuts */
proc means data=rate_cut_events n mean median std min max;
    var Basis_Points avg_daily_return volatility sp500_monthly_change;
    class significant_cut;
    ods output Summary=cut_summary;
run;

/* Time Series Analysis - Visualize trends */
proc timeseries data=sp500_clean out=ts_output;
    id sas_date interval=day;
    var Close;
    decomp tcc;
run;

/* ANOVA - Compare market performance across different rate change types */
proc anova data=merged_analysis;
    class Change_Type;
    model avg_daily_return = Change_Type;
    means Change_Type / tukey cldiff;
    ods output ModelANOVA=anova_results;
quit;

/* Create visualizations */

/* 1. Fed Funds Rate Over Time */
proc sgplot data=fed_rates_clean;
    title "Federal Funds Rate: 2020-2025";
    series x=sas_date y=Federal_Funds_Rate / lineattrs=(thickness=2 color=blue);
    refline '01MAR2020'd / axis=x label="COVID-19 Cuts" lineattrs=(pattern=dash color=red);
    refline '01SEP2024'd / axis=x label="2024 Rate Cuts Begin" lineattrs=(pattern=dash color=green);
    xaxis label="Date";
    yaxis label="Federal Funds Rate (%)";
run;

/* 2. S&P 500 Performance Over Time */
proc sgplot data=sp500_clean;
    title "S&P 500 Index: 2020-2025";
    series x=sas_date y=Close / lineattrs=(thickness=2 color=darkgreen);
    refline '01MAR2020'd / axis=x label="COVID-19 Impact" lineattrs=(pattern=dash color=red);
    xaxis label="Date";
    yaxis label="S&P 500 Close Price";
run;

/* 3. Correlation Heatmap Data Preparation */
proc sgplot data=merged_analysis;
    title "Fed Rate vs S&P 500 Monthly Change";
    scatter x=Rate_Change y=sp500_monthly_change / markerattrs=(symbol=circlefilled size=10);
    reg x=Rate_Change y=sp500_monthly_change / lineattrs=(color=red thickness=2);
    xaxis label="Fed Rate Change (%)";
    yaxis label="S&P 500 Monthly Change (%)";
run;

/* 4. Rate Cut Impact Analysis */
proc sgplot data=cut_impact_analysis;
    title "S&P 500 Returns Following Rate Cuts";
    vbar fed_date / response=return_1m legendlabel="1-Month Return" barwidth=0.8 fillattrs=(color=lightblue);
    vbar fed_date / response=return_3m legendlabel="3-Month Return" barwidth=0.6 fillattrs=(color=blue);
    vbar fed_date / response=return_6m legendlabel="6-Month Return" barwidth=0.4 fillattrs=(color=darkblue);
    xaxis label="Rate Cut Date" type=time;
    yaxis label="Return (%)";
run;

/* 5. Volatility Analysis */
proc sgplot data=merged_analysis;
    title "Market Volatility vs Federal Funds Rate";
    scatter x=Federal_Funds_Rate y=volatility / markerattrs=(symbol=circlefilled size=8 color=orange);
    reg x=Federal_Funds_Rate y=volatility / lineattrs=(color=red thickness=2);
    xaxis label="Federal Funds Rate (%)";
    yaxis label="S&P 500 Daily Return Volatility (%)";
run;

/* Export key results to CSV for web app */
proc export data=merged_analysis
    outfile="/home/user/makkena.github.io/results/merged_analysis.csv"
    dbms=csv
    replace;
run;

proc export data=cut_impact_analysis
    outfile="/home/user/makkena.github.io/results/cut_impact_analysis.csv"
    dbms=csv
    replace;
run;

proc export data=pearson_corr
    outfile="/home/user/makkena.github.io/results/correlation_results.csv"
    dbms=csv
    replace;
run;

proc export data=param_est
    outfile="/home/user/makkena.github.io/results/regression_parameters.csv"
    dbms=csv
    replace;
run;

/* Generate HTML Report Summary */
ods html file="/home/user/makkena.github.io/results/sas_analysis_report.html";

proc print data=cut_impact_analysis noobs label;
    title "Rate Cut Events and Subsequent S&P 500 Performance";
    var fed_date Basis_Points sp500_at_cut return_1m return_3m return_6m;
    label
        fed_date = "Date of Cut"
        Basis_Points = "Basis Points Cut"
        sp500_at_cut = "S&P 500 at Cut"
        return_1m = "1-Month Return (%)"
        return_3m = "3-Month Return (%)"
        return_6m = "6-Month Return (%)";
run;

proc print data=pearson_corr noobs;
    title "Correlation Analysis: Fed Rates and S&P 500 Metrics";
run;

proc print data=param_est noobs;
    title "Regression Analysis: Impact of Rate Changes on S&P 500";
run;

ods html close;

/* Key Insights Summary */
data insights;
    length insight $500;
    insight = "ANALYSIS COMPLETE"; output;
    insight = "1. Correlation between Fed rate cuts and S&P 500 performance calculated"; output;
    insight = "2. Rate cut events identified and impact measured over 1, 3, and 6 months"; output;
    insight = "3. Regression model built to predict S&P 500 changes based on rate changes"; output;
    insight = "4. Results exported for web application visualization"; output;
run;

proc print data=insights noobs;
    title "Analysis Summary";
run;
