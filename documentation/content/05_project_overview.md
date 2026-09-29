# 5. Project Overview & Problem Statement

### 5.1 Problem Statement
1. **Opaque Compensation:** Job descriptions frequently omit structured salary figures or use disparate currencies and time horizons (hourly, monthly, annual).
2. **Dynamic Skill Valuation:** Technological skill demand shifts rapidly, making static yearly surveys obsolete.
3. **Data Velocity & Scale:** Labor market data arrives continuously from hundreds of platforms, requiring streaming ingestion and distributed aggregation.

### 5.2 Project Motivation & Objectives
- **Automated Normalization:** Convert disparate global currencies, time periods, and messy job titles into standardized USD annual salaries and 7 distinct role categories.
- **Enterprise Warehousing:** Model job market facts and dimensions in Apache Hive for sub-second analytical aggregations across 20 distinct business queries.
- **Explainable Machine Learning:** Deliver an end-to-end Spark MLlib pipeline predicting salaries based on skill sets, seniority, company tier, and location.
