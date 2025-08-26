# Zipco Pipeline

A robust ETL pipeline built with **Python, Pandas, SQLAlchemy, PostgreSQL, and Power BI** for real estate listing data.  
The pipeline extracts, transforms, and loads property listing data into a PostgreSQL data warehouse following a **Medallion Architecture (Bronze → Silver → Gold).**

---

## Features
- **Data Extraction** from raw listing sources (JSON/CSV).
- **Data Transformation** with Pandas (cleaning, standardizing, handling nulls).
- **Data Loading** into PostgreSQL with star schema design:
  - **Dimension Tables:** `dim_location`, `dim_office`, `dim_property`, `dim_date`
  - **Fact Table:** `fact_listings`
- **Incremental & Full Load Support**
- **Date Dimension Generation** (min/max across `listed_date`).
- **Data Visualization** in Power BI.
- **Automated Scheduling** via cron in WSL.


