# Stock Analytics Engineering - Multi-Factor Model 

**⚠️ Work In Progress...**  
An end-to-end analytics engineering project that models financial data and builds a comprehensive stock scoring system by combining fundamental analysis, technical indicators, valuation metrics, momentum signals, and sentiment analysis from financial news.

## 📊 Project Overview

This project implements a multi-factor investment model that ranks stocks based on multiple dimensions:

- **Fundamental Analysis**: Financial health indicators as ROA, ROE, ROIC...
- **Valuation Metrics**: Price-to-earnings, price-free-cash-flow ratio, and other valuation ratios
- **Momentum Signals**: Price trends and technical indicators such as SMA, RSI...
- **Sentiment Analysis**: News sentiment scoring using VADER
- **Overall Score**: Composite ranking combining all factors

The final output is an interactive Power BI dashboard that provides actionable investment insights.

![Dashboard Preview](path/to/dashboard.gif)

## 🏗️ Architecture

### Tech Stack

- **Data Source**: Finviz API
- **Orchestration**: Databricks Jobs
- **Transformation**: dbt (data build tool)
- **Sentiment Analysis**: VADER (Valence Aware Dictionary and sEntiment Reasoner)
- **Processing**: Apache Spark SQL
- **Visualization**: Power BI

### Data Pipeline

```
Finviz API → Databricks (Extract) → dbt (Transform Stage 1) → Databricks (Sentiment Analysis) → dbt (Transform Stage 2) → Power BI
```

![Pipeline](path/to/dashboard.gif)

## 🔄 Pipeline Workflow

### 1. Data Extraction (Databricks)
- Notebooks fetch stock data and news from Finviz API
- Raw data stored in staging tables: `news_data` and `screener_data`

### 2. Initial Transformation (dbt)
```bash
dbt build -s +int_news_union
```
- Processes raw news data
- Creates an intermediate union table for sentiment analysis by joining stock-related news from `screener_data` with market and blog news from the `news_data` source.

### 3. Sentiment Analysis (Databricks)
- Spark SQL notebook processes `int_news_union`
- Applies VADER library to calculate sentiment scores
- Generates `news_scores_data` table with sentiment metrics

### 4. Final Transformation (dbt)
```bash
dbt build --exclude +int_news_union
```
- Processes fundamental, valuation, technical and momentum data
- Applies a factor model scoring algorithm
- Generates final mart and dimension tables with composite scores
- Outputs an asset recommendation ranking based on multiple categories

![Data Modeling](path/to/dashboard.gif)

### 5. Visualization (Power BI)
- Interactive dashboard displaying:
  - Top scoring recommendations based on each category
  - Income/Sales 
  - Fundamental metrics
  - Analyst recommendation
  - Price and valuation charts

## 📁 Repository Structure

```
stock-analytics-engineering/
├── databricks/                                       # databricks ingestion/transformation models
|   ├── int/                                          # Intermediate/integration layer
│   |   └── int_news_sentiment_scores.ipynb           # News sentiment analysis processing
|   ├── sql/                                          # SQL scripts and table definitions
│   |   └── tables.sql                                # Table creation and schema definitions
|   └── src/                                          # Source data ingestion layer
|       ├── news/                                     # News data sources
|       │   └── src_finviz_raw_news_data.ipynb        # Raw news data extraction from Finviz
|       └── screener/                                 # Stock screener data sources
|           └── src_finviz_raw_screener_data.ipynb    # Raw screener data extraction from Finviz
|
├── dbt/                                              # dbt transformation models
|   ├── dbt_project.yml                               # Main dbt project configuration
|   ├── package-lock.yml                              # dbt package lock file
|   ├── packages.yml                                  # dbt package dependencies
|   ├── README.md                                     # dbt project documentation
|   ├── analyses/                                     # Ad-hoc analytical queries
|   ├── docs/                                         # Project documentation
|   │   ├── columns/                                  # Column-level documentation
│   |   ├── metrics/                                  # Business metrics documentation
|   │   ├── models/                                   # Model-level documentation
│   |   └── tests/                                    # Test documentation
|   ├── macros/                                       # Reusable SQL macros
|   ├── models/                                       # dbt models (transformations)
|   │   ├── marts/                                    # Business-level models (Gold layer)
|   │   │   ├── intermediate/                         # Intermediate transformations
|   │   │   │   ├── fundamentals/                     # Fundamental metrics
|   │   │   │   ├── news/                             # News data transformations
|   │   │   │   ├── ownership/                        # Ownership data transformations
|   │   │   │   ├── performance/                      # Performance metrics
|   │   │   │   ├── technicals/                       # Technical indicators
|   │   │   │   └── valuation/                        # Valuation metrics
|   │   │   ├── dim_*.sql                             # Dimension tables
│   |   │   ├── dim_*.yml
│   |   │   ├── fct_*.sql                             # Fact tables
│   │   |   └── fct_*.yml
|   │   └── staging/                                  # Staging models (Silver layer)
|   │       ├── finviz/                               # Finviz source staging
|   │       └── sources.yml                           # Source table definitions
|   ├── seeds/                                        # Static reference data (CSV files)
|   │   ├── schema.yml
|   │   └── source_domains.csv                        # News source domain mappings
|   ├── snapshots/                                    # Slowly changing dimensions (SCD Type 2)
|   │   ├── asset_identity_snapshot.sql               # Asset identity historical tracking
|   │   └── schema.yml
|   └── tests/                                        # Data quality tests
|       ├── generic/                                  # Reusable generic tests
|       │   ├── grain_unique.sql                      # Test for unique grain
|       │   ├── normalized_range.sql                  # Test for normalized value ranges
|       │   └── row_count_minimum.sql                 # Test for minimum row counts
|       │ 
|       ├── test_overall_score_consistency.sql        # Overall scoring consistency test
|       └── test_scores_bounds.sql                    # Score boundary validation test
└── README.md
```

## 📈 Factor Model Methodology

The scoring system evaluates stocks across four key dimensions:

### 1. **Fundamental Score**
- Revenue growth
- Earnings quality
- Profit margins
- Return on equity (ROE)

### 2. **Valuation Score**
- P/E ratio
- P/B ratio
- PEG ratio
- Price/Sales ratio

### 3. **Momentum Score**
- Price trends (YTD, 6M, 3M performance)
- Relative strength
- Moving average positions

### 4. **Sentiment Score**
- News sentiment analysis using VADER
- Aggregated sentiment from recent news articles
- Weighted by recency and source quality

### Overall Score
A weighted composite of all four factors, providing a holistic view of investment opportunities.

## 📊 Key Features

- **Automated Data Pipeline**: End-to-end orchestration from data extraction to visualization
- **Modular dbt Models**: Clean, tested, and documented transformation logic
- **Sentiment Analysis**: Real-time news sentiment tracking
- **Interactive Dashboard**: Dynamic filtering and drill-down capabilities
- **Reproducible**: Version-controlled transformations and lineage tracking

## 🎯 Use Cases

- **Investment Research**: Identify high-potential stocks based on multiple factors
- **Portfolio Management**: Track and compare stocks across different scoring dimensions
- **Risk Assessment**: Evaluate stocks from fundamental and technical perspectives
- **Sentiment Monitoring**: Stay informed about market perception and news impact

## 📝 dbt Project Details

The dbt project follows best practices:

- **Staging models**: One-to-one with source tables
- **Intermediate models**: Business logic and calculations
- **Mart models**: Final, business-ready datasets
- **Documentation**: Comprehensive model and column descriptions
- **Tests**: Data quality tests on key metrics and relationships

### Key Models

- `stg_raw_news`: Staged news data from Finviz
- `stg_raw_screener`: Staged stock screener data
- `int_news_union`: Unified news dataset for sentiment analysis
- `int_news_scores`: News with sentiment scores
- `fct_stock_scores`: Final fact table with all scores
- `dim_analyst_recommendations`: Analyst recommendation dimension

## 🔍 DAG Visualization

The project includes comprehensive data lineage tracking through dbt's DAG:

![dbt DAG](path/to/dag_image.png)

This shows the complete flow from raw data sources through intermediate transformations to final mart tables.

## 🤝 Contributing

This is a portfolio project, but suggestions and feedback are welcome! Feel free to open an issue or submit a pull request.

## 📫 Contact

**Tomeu** - [GitHub](https://github.com/tomeu90)

Project Link: [https://github.com/tomeu90/stock-analytics-engineering](https://github.com/tomeu90/stock-analytics-engineering)

## 📜 License

This project is open source and available under the MIT License.

---

*Built with ❤️ for analytics engineering excellence*



















