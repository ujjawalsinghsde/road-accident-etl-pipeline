# 🚦 Road Accident Analysis ETL Pipeline

## 📌 Project Overview
This project demonstrates an **end-to-end ETL pipeline for Road Accident Analysis** built on **Databricks**.  
It ingests raw accident data, applies quality checks, transforms it into clean datasets, and enables visual dashboards for decision-making.  
The pipeline follows the **Medallion Architecture (Bronze → Silver → Gold)** and ensures **data governance, security, and scalability**.

---

## 🛠️ Technology Stack
- **Python & SQL** – Data processing & queries  
- **Databricks Notebook** – ETL orchestration  
- **Auto Loader** – Scalable ingestion with schema evolution  
- **Delta Lake** – Bronze, Silver, and Gold tables  
- **Unity Catalog** – Security & access control  
- **Serverless SQL** – Interactive queries & reporting  

---

## 🔄 ETL Pipeline Stages
1. **Data Generation** – Faker + Spark to simulate accident records  
2. **Data Ingestion (Bronze)** – Databricks Auto Loader with schema evolution  
3. **Data Quality (Silver)** – Validations + split into good/bad records  
4. **Aggregation (Gold)** – Accident insights (area, severity, weather, causes, etc.)  
5. **Security (Unity Catalog)** – RBAC & dynamic masking on sensitive fields  
6. **Visualization** – Dashboards for accident trends and insights  

---

## 📊 Pipeline Workflow
Here’s the **Databricks Job & Pipeline image** (uploaded in repository):  

<div align="center">
    <img src="assets/job&pipeline.png" width="100%" />
</div>


## 📊 Dashboard Insights

The final dashboards provide **business-ready visualizations** from the Gold Layer tables.  

### Area-Wise Total Accidents
Identifies accident hotspots for infrastructure planning.
```sql
SELECT 
    Area_accident_occured AS area,
    Total_Accidents
FROM main_cluster.autoloader_pipeline.area_year_accidents
ORDER BY total_accidents DESC;
```
<div align="center">
    <img src="assets/AreaWiseTotalAccidents.png" width="400" />
</div>

### Day of Week Accident Trend
Highlights weekdays/weekends with higher risks.
```sql
SELECT 
    Day_of_week,
    COUNT(*) AS total_accidents
FROM main_cluster.autoloader_pipeline.death_report_details
GROUP BY Day_of_week
ORDER BY 
    CASE Day_of_week
        WHEN 'Sunday' THEN 1
        WHEN 'Monday' THEN 2
        WHEN 'Tuesday' THEN 3
        WHEN 'Wednesday' THEN 4
        WHEN 'Thursday' THEN 5
        WHEN 'Friday' THEN 6
        WHEN 'Saturday' THEN 7
    END;
```
<div align="center">
    <img src="assets/DayOfWeekAccidentTrend.png" width="400" />
</div>

### Hour of Day Accident Trend
Shows peak accident hours for better resource allocation.
```sql
SELECT 
    hour(try_to_timestamp(Time, 'H:mm:ss')) AS hour_of_day,
    COUNT(*) AS total_accidents
FROM main_cluster.autoloader_pipeline.death_report_details
GROUP BY hour(try_to_timestamp(Time, 'H:mm:ss'))
ORDER BY hour_of_day;
```
<div align="center">
    <img src="assets/HourOfDayAccidentTrend.png" width="400" />
</div>

### Accident Severity Distribution
Breakdown into fatal, serious, and minor categories.
```sql
SELECT 
    Accident_severity,
    Count
FROM main_cluster.autoloader_pipeline.accident_severity_breakdown;
```
<div align="center">
    <img src="assets/AccidentSeverityDistribution.png" width="400" />
</div>

### Gender-wise Fatalities
Compares accident impact by gender.
```sql
SELECT 
    Sex_of_driver, 
    Casualty_severity,
    COUNT(*) AS death_count
FROM main_cluster.autoloader_pipeline.death_report_details
WHERE Casualty_severity = :severity
GROUP BY Sex_of_driver, Casualty_severity;
```
<div align="center">
    <img src="assets/GenderWiseFatalities.png" width="400" />
</div>

### Age Band Fatalities
Identifies vulnerable age groups.
```sql
SELECT 
    Age_band_of_driver,
    COUNT(*) AS fatal_accidents
FROM main_cluster.autoloader_pipeline.death_report_details
WHERE Casualty_severity = 'Serious Injury'
GROUP BY Age_band_of_driver
ORDER BY fatal_accidents DESC;
```
<div align="center">
    <img src="assets/AgeBandFatalities.png" width="400" />
</div>

### Driving Experience vs. Accident Count
Links experience level with accident likelihood.
```sql
SELECT 
    Driving_experience,
    COUNT(*) AS total_accidents
FROM main_cluster.autoloader_pipeline.death_report_details
GROUP BY Driving_experience
ORDER BY total_accidents DESC;
```
<div align="center">
    <img src="assets/DrivingExperiencevsAccidentCount.png" width="400" />
</div>

### Weather-wise Accidents
Shows accident distribution under Normal, Rain, Fog, etc.
```sql
SELECT 
    Cause_of_accident,
    COUNT(*) AS total_accidents
FROM main_cluster.autoloader_pipeline.death_report_details
WHERE Weather_conditions IN (:weather)
GROUP BY Cause_of_accident
ORDER BY total_accidents DESC
LIMIT 10;
```
<div align="center">
    <img src="assets/WeatherWiseAccidents.png" width="400" />
</div>

### Road Surface Conditions
Correlates road quality with accident frequency.
```sql
SELECT 
    Road_surface_conditions,
    COUNT(*) AS total_accidents
FROM main_cluster.autoloader_pipeline.death_report_details
GROUP BY Road_surface_conditions
ORDER BY total_accidents DESC;
```
<div align="center">
    <img src="assets/RoadSurfaceConditions.png" width="400" />
</div>

### Light Condition Accidents
Compares daylight vs. night-time accidents.
```sql
SELECT 
    Light_conditions,
    COUNT(*) AS total_accidents
FROM main_cluster.autoloader_pipeline.death_report_details
GROUP BY Light_conditions
ORDER BY total_accidents DESC;
```
<div align="center">
    <img src="assets/LightConditionAccidents.png" width="400" />
</div>

### Top 10 Causes of Accidents
Lists overspeeding, distractions, and other key causes.
```sql
SELECT 
    Cause_of_accident,
    COUNT(*) AS total_accidents
FROM main_cluster.autoloader_pipeline.death_report_details
GROUP BY Cause_of_accident
ORDER BY total_accidents DESC
LIMIT 10;
```
<div align="center">
    <img src="assets/Top10CausesOfAccidents.png" width="400" />
</div>

---

## 📜 Complete Project Code

This section contains **all project files with explanations**.

<details>
<summary>🔹 aggregation.py</summary>

```python
from pyspark.sql.functions import count, year, col, to_timestamp

# Load Silver Layer data
df = spark.table("main_cluster.autoloader_pipeline.silver_filtered_data")

# 1. State-wise Total Accidents by Year
df_with_year = df.withColumn("Year", year(to_timestamp(col("Time"))))
state_year_df = (
    df_with_year.groupBy("Area_accident_occured", "Year")
    .agg(count("*").alias("Total_Accidents"))
)
state_year_df.write.mode("overwrite").saveAsTable(
    "main_cluster.autoloader_pipeline.area_year_accidents"
)

# 2. Accident Severity Breakdown
severity_df = df.groupBy("Accident_severity").agg(count("*").alias("Count"))
severity_df.write.mode("overwrite").saveAsTable(
    "main_cluster.autoloader_pipeline.accident_severity_breakdown"
)

# 3. Fatal & Serious Injury Reports
death_report_df = df.filter(
    col("Casualty_severity").isin("Fatal injury", "Serious Injury")
).select(
    "Sex_of_casualty",
    "Age_band_of_casualty",
    "Day_of_week",
    "Time",
    "Area_accident_occured",
    "Cause_of_accident",
    "Accident_severity",
    "Light_conditions",
    "Road_surface_conditions",
    "Driving_experience",
    "Weather_conditions",
    "Age_band_of_driver",
    "Sex_of_driver",
    "Casualty_severity"
)
death_report_df.write.mode("overwrite").saveAsTable(
    "main_cluster.autoloader_pipeline.death_report_details"
)
```

</details>

<details>
<summary>🔹 autoloader.py</summary>

```python
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, IntegerType

# Define paths
input_path = "/Volumes/databricks_main_cluster/default/accident_data"
checkpoint_path = "/Volumes/databricks_main_cluster/default/autoloader_checkpoint"
output_path = "/Volumes/main_cluster/autoloader_pipeline/raw_data"

# Stream ingestion with Autoloader
df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferSchema", "true")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .load(input_path)
)

# Write to Delta Lake
query = (
    df.writeStream.format("delta")
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .start(output_path)
)
```

</details>

<details>
<summary>🔹 DataCleaningSilverLayer.py</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, to_timestamp, expr, trim, lower
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AccidentDataCleaning")

def main():
    try:
        spark = SparkSession.builder.appName("AccidentDataCleaning").getOrCreate()
        logger.info("Spark session started.")

        # Define valid values
        valid_sex = ["male", "female"]
        valid_accident_severity = ["slight injury", "serious injury", "fatal injury"]
        valid_age_band = ["18-30", "31-50", "under 18", "over 51"]

        # Load Bronze data
        bronze_df = spark.read.table("main_cluster.autoloader_pipeline.bronze_table")

        # Handle missing values and type casting
        df = bronze_df.withColumn(
            "Number_of_vehicles_involved",
            when(col("Number_of_vehicles_involved").isNull(), lit(1)).otherwise(
                col("Number_of_vehicles_involved").cast("int")
            )
        ).withColumn(
            "Number_of_casualties",
            when(col("Number_of_casualties").isNull(), lit(1)).otherwise(
                col("Number_of_casualties").cast("int")
            )
        )

        # Clean timestamp
        df = df.withColumn("Time_cleaned", expr("try_cast(Time as timestamp)"))

        # Normalize categorical fields
        categorical_columns = [
            "Age_band_of_driver",
            "Sex_of_driver",
            "Sex_of_casualty",
            "Age_band_of_casualty",
            "Accident_severity"
        ]
        for col_name in categorical_columns:
            df = df.withColumn(col_name, lower(trim(col(col_name))))

        # Replace invalid values with null
        for col_name in ["Day_of_week", "Cause_of_accident"]:
            df = df.withColumn(
                col_name,
                when((col(col_name).isin("na", "-", "")), None).otherwise(col(col_name))
            )

        # Mark valid vs invalid data
        cleaned_df = df.withColumn(
            "is_valid",
            when(
                col("Time_cleaned").isNotNull()
                & col("Day_of_week").isNotNull()
                & col("Age_band_of_driver").isin(valid_age_band)
                & col("Sex_of_driver").isin(valid_sex)
                & col("Sex_of_casualty").isin(valid_sex)
                & col("Age_band_of_casualty").isin(valid_age_band)
                & col("Cause_of_accident").isNotNull()
                & col("Area_accident_occured").isNotNull()
                & col("Accident_severity").isin(valid_accident_severity),
                True,
            ).otherwise(False)
        )

        # Split valid/invalid datasets
        good_data_df = cleaned_df.filter(col("is_valid") == True).drop("is_valid")
        bad_data_df = cleaned_df.filter(col("is_valid") == False).drop("is_valid")

        # Save to Silver Layer
        good_data_df.write.format("delta").mode("overwrite").saveAsTable(
            "main_cluster.autoloader_pipeline.silver_filtered_data"
        )
        bad_data_df.write.format("delta").mode("overwrite").saveAsTable(
            "main_cluster.autoloader_pipeline.silver_filtered_bad_data"
        )

        logger.info("Silver layer data processing complete.")

    except Exception as e:
        logger.exception(f"Unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
```

</details>

<details>
<summary>🔹 DataGeneration.txt (logs)</summary>

```
2025-08-20 11:49:21,971 - INFO - Starting dataset generation...
2025-08-20 11:49:22,001 - WARNING - Corruption: Field 'Number_of_casualties' replaced with NONSENSE value 'Likely scene.'
2025-08-20 11:49:22,003 - WARNING - Corruption: Field 'Type_of_collision' replaced with NONSENSE value 'Vote couple.'
2025-08-20 11:49:22,006 - WARNING - Corruption: Field 'Service_year_of_vehicle' set to MISSING
...existing log lines...
```

</details>

<details>
<summary>🔹 GenerateData.py</summary>

```python
import random
import pandas as pd
from faker import Faker
import logging
import variables
import traceback

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("data_generation.log"), logging.StreamHandler()]
)

class DataGenerator:
    def __init__(self, no_of_records=1000):
        self.fake = Faker()
        self.NUM_RECORDS = no_of_records
        self.CORRUPTION_RATE = 0.2

    def random_choice(self, values):
        return random.choice(values if isinstance(values, list) else values.split(","))

    def generate_valid_record(self):
        return {
            "Time": self.fake.time(pattern="%H:%M:%S"),
            "Day_of_week": self.random_choice(variables.DAYS_OF_WEEKS),
            "Age_band_of_driver": self.random_choice("18-30,31-50,Over 51,Unknown"),
            "Sex_of_driver": self.random_choice("Male,Female,Unknown"),
            "Educational_level": self.random_choice(variables.EDUCATION_LEVEL),
            "Vehicle_driver_relation": self.random_choice("Owner,Employee,Unknown"),
            "Driving_experience": self.random_choice("1-2yr,2-5yr,5-10yr,Over 10yr,Unknown"),
            "Type_of_vehicle": self.random_choice(variables.VEHICLE_TYPE),
            "Owner_of_vehicle": self.random_choice("Owner,Government,Other,Unknown"),
            "Service_year_of_vehicle": random.randint(1, 30),
            "Defect_of_vehicle": self.random_choice(
                "No defect,Brake failure,Steering failure,Tyre burst,Other"
            ),
            "Area_accident_occured": self.random_choice(variables.ACCIDENT_AREA),
            "Lanes_or_Medians": self.random_choice(variables.LANES_OR_MEDIANS),
            "Road_allignment": self.random_choice(variables.ROAD_ALIGNMENT),
            "Types_of_Junction": self.random_choice(variables.TYPES_OF_JUNCTION),
            "Road_surface_type": self.random_choice(variables.ROAD_SURFACE_TYPE),
            "Road_surface_conditions": self.random_choice(variables.ROAD_SURFACE_CONDITIONS),
            "Light_conditions": self.random_choice(variables.LIGHT_CONDITIONS),
            "Weather_conditions": self.random_choice(variables.WEATHER_CONDITIONS),
            "Type_of_collision": self.random_choice(variables.TYPE_OF_COLLISION),
            "Number_of_vehicles_involved": random.randint(1, 5),
            "Number_of_casualties": random.randint(0, 5),
            "Vehicle_movement": self.random_choice(variables.VEHICLE_MOVEMENT),
            "Casualty_class": self.random_choice(variables.CASUALTY_CLASS),
            "Sex_of_casualty": self.random_choice("Male,Female,Unknown"),
            "Age_band_of_casualty": self.random_choice("Under 18,18-30,31-50,Over 51,Unknown"),
            "Casualty_severity": self.random_choice(variables.ACCIDENT_SEVERITY),
            "Work_of_casuality": self.random_choice("Employed,Unemployed,Student,Unknown"),
            "Fitness_of_casuality": self.random_choice(variables.FITNESS_OF_CASUALITY),
            "Pedestrian_movement": self.random_choice(variables.PEDESTRIAN_MOVEMENT),
            "Cause_of_accident": self.random_choice(variables.CAUSE_OF_ACCIDENT),
            "Accident_severity": self.random_choice(variables.ACCIDENT_SEVERITY),
        }

    def generate_corrupted_record(self, valid_record):
        record = valid_record.copy()
        field_to_corrupt = random.choice(list(record.keys()))
        corruption_type = random.choice(["missing", "nonsense", "negative"])
        if corruption_type == "missing":
            record[field_to_corrupt] = None
            logging.warning(f"Corruption: Field '{field_to_corrupt}' set to MISSING")
        elif corruption_type == "nonsense":
            nonsense_value = self.fake.text(max_nb_chars=15)
            record[field_to_corrupt] = nonsense_value
            logging.warning(f"Corruption: Field '{field_to_corrupt}' replaced with NONSENSE value '{nonsense_value}'")
        elif corruption_type == "negative" and isinstance(record[field_to_corrupt], int):
            negative_value = -abs(record[field_to_corrupt])
            record[field_to_corrupt] = negative_value
            logging.warning(f"Corruption: Field '{field_to_corrupt}' set to NEGATIVE value '{negative_value}'")
        return record

    def generate_dataset(self):
        data = []
        for _ in range(self.NUM_RECORDS):
            valid_record = self.generate_valid_record()
            if random.random() >= self.CORRUPTION_RATE:
                data.append(valid_record)
        return spark.createDataFrame(data)

if __name__ == "__main__":
    logging.info("Starting dataset generation...")
    try:
        data_generator = DataGenerator(2000)
        sdf = data_generator.generate_dataset()
        output_path = "/Volumes/databricks_main_cluster/default/accident_data"
        logging.info(f"Row count: {sdf.count()}")
        sdf.write.mode("append").option("header", True).csv(output_path)
        logging.info("Dataset successfully generated and saved.")
    except Exception as e:
        logging.error(f"Error saving dataset: {e}")
        traceback.print_exc()
```

</details>

<details>
<summary>🔹 RBAC.py</summary>

```sql
-- Create User-Group Mapping Table
CREATE OR REPLACE TABLE databricks_main_cluster.default.user_groups (
  username STRING,
  groupname STRING
);

-- Insert sample user-group data
INSERT INTO databricks_main_cluster.default.user_groups VALUES
  ('ayeujjawalsingh@gmail.com', 'data_analysts'),
  ('shashank@gmail.com', 'data_analysts'),
  ('nayan@gmail.com', 'data_readers'),
  ('neha@gmail.com', 'data_readers');

-- View 1: Basic Accident Info with masking
CREATE OR REPLACE VIEW databricks_main_cluster.default.vw_accident_basic_info AS
WITH user_group_flag AS (
  SELECT DISTINCT username
  FROM databricks_main_cluster.default.user_groups
  WHERE username = current_user() AND groupname = 'data_analysts'
)
SELECT
  d.Time,
  d.Day_of_week,
  d.Area_accident_occured,
  CASE WHEN ug.username IS NOT NULL THEN d.Sex_of_driver ELSE 'MASKED' END AS Sex_of_driver,
  CASE WHEN ug.username IS NOT NULL THEN d.Age_band_of_driver ELSE 'MASKED' END AS Age_band_of_driver,
  CASE WHEN ug.username IS NOT NULL THEN d.Owner_of_vehicle ELSE 'MASKED' END AS Owner_of_vehicle,
  d.Type_of_collision,
  d.Weather_conditions,
  d.Light_conditions
FROM databricks_main_cluster.default.silver_filtered_data d
LEFT JOIN user_group_flag ug ON TRUE;

-- View 2: Vehicle and Driver Details
CREATE OR REPLACE VIEW databricks_main_cluster.default.vw_vehicle_driver_details AS
WITH user_group_flag AS (
  SELECT DISTINCT username
  FROM databricks_main_cluster.default.user_groups
  WHERE username = current_user() AND groupname = 'data_analysts'
)
SELECT
  d.Time,
  d.Area_accident_occured,
  CASE WHEN ug.username IS NOT NULL THEN d.Sex_of_driver ELSE 'MASKED' END AS Sex_of_driver,
  CASE WHEN ug.username IS NOT NULL THEN d.Age_band_of_driver ELSE 'MASKED' END AS Age_band_of_driver,
  CASE WHEN ug.username IS NOT NULL THEN d.Owner_of_vehicle ELSE 'MASKED' END AS Owner_of_vehicle,
  d.Educational_level,
  d.Vehicle_driver_relation,
  d.Driving_experience,
  d.Type_of_vehicle,
  d.Service_year_of_vehicle,
  d.Defect_of_vehicle
FROM databricks_main_cluster.default.silver_filtered_data d
LEFT JOIN user_group_flag ug ON TRUE;

-- View 3: Road and Environmental Conditions
CREATE OR REPLACE VIEW databricks_main_cluster.default.vw_road_environmental_conditions AS
WITH user_group_flag AS (
  SELECT DISTINCT username
  FROM databricks_main_cluster.default.user_groups
  WHERE username = current_user() AND groupname = 'data_analysts'
)
SELECT
  d.Time,
  d.Area_accident_occured,
  CASE WHEN ug.username IS NOT NULL THEN d.Sex_of_driver ELSE 'MASKED' END AS Sex_of_driver,
  CASE WHEN ug.username IS NOT NULL THEN d.Age_band_of_driver ELSE 'MASKED' END AS Age_band_of_driver,
  CASE WHEN ug.username IS NOT NULL THEN d.Owner_of_vehicle ELSE 'MASKED' END AS Owner_of_vehicle,
  d.Road_allignment,
  d.Road_surface_type,
  d.Road_surface_conditions,
  d.Weather_conditions,
  d.Light_conditions,
  d.Lanes_or_Medians,
  d.Types_of_Junction
FROM databricks_main_cluster.default.silver_filtered_data d
LEFT JOIN user_group_flag ug ON TRUE;
```

</details>

<details>
<summary>🔹 RoadAccidentAnalysisNotebook.py</summary>

```sql
-- Area-wise Total Accidents
SELECT 
    Area_accident_occured AS area,
    Total_Accidents
FROM databricks_main_cluster.default.area_year_accidents
ORDER BY total_accidents DESC;

-- Day of Week Accident Trend
SELECT 
    Day_of_week,
    COUNT(*) AS total_accidents
FROM main_cluster.autoloader_pipeline.death_report_details
GROUP BY Day_of_week
ORDER BY total_accidents DESC;

-- Hour of Day Accident Trend
SELECT 
    hour(try_to_timestamp(Time, 'H:mm:ss')) AS hour_of_day,
    COUNT(*) AS total_accidents
FROM main_cluster.autoloader_pipeline.death_report_details
GROUP BY hour(try_to_timestamp(Time, 'H:mm:ss'))
ORDER BY hour_of_day;

-- Accident Severity Distribution
SELECT 
    Accident_severity,
    Count
FROM main_cluster.autoloader_pipeline.accident_severity_breakdown;

-- Gender-wise Fatalities
SELECT 
    Sex_of_driver, 
    Casualty_severity,
    COUNT(*) AS death_count
FROM main_cluster.autoloader_pipeline.death_report_details
WHERE Casualty_severity = :severity
GROUP BY Sex_of_driver, Casualty_severity;

-- Age Band Fatalities
SELECT 
    Age_band_of_driver,
    COUNT(*) AS fatal_accidents
FROM main_cluster.autoloader_pipeline.death_report_details
WHERE Casualty_severity = 'Serious Injury'
GROUP BY Age_band_of_driver
ORDER BY fatal_accidents DESC;

-- Driving Experience vs Accident Count
SELECT 
    Driving_experience,
    COUNT(*) AS total_accidents
FROM main_cluster.autoloader_pipeline.death_report_details
GROUP BY Driving_experience
ORDER BY total_accidents DESC;

-- Weather-wise Accidents (Top 10 causes under selected weather)
SELECT 
    Cause_of_accident,
    COUNT(*) AS total_accidents
FROM main_cluster.autoloader_pipeline.death_report_details
WHERE Weather_conditions IN (:weather)
GROUP BY Cause_of_accident
ORDER BY total_accidents DESC
LIMIT 10;

-- Road Surface Conditions
SELECT 
    Road_surface_conditions,
    COUNT(*) AS total_accidents
FROM main_cluster.autoloader_pipeline.death_report_details
GROUP BY Road_surface_conditions
ORDER BY total_accidents DESC;

-- Light Condition Accidents
SELECT 
    Light_conditions,
    COUNT(*) AS total_accidents
FROM main_cluster.autoloader_pipeline.death_report_details
GROUP BY Light_conditions
ORDER BY total_accidents DESC;

-- Top 10 Causes of Accidents
SELECT 
    Cause_of_accident,
    COUNT(*) AS total_accidents
FROM main_cluster.autoloader_pipeline.death_report_details
GROUP BY Cause_of_accident
ORDER BY total_accidents DESC
LIMIT 10;
```

</details>

<details>
<summary>🔹 variables.py</summary>

```python
import calendar

DAYS_OF_WEEKS = list(calendar.day_name)
VEHICLE_TYPE = "Automobile,Public (> 45 seats),Lorry (41?100Q),Public (13?45 seats),Lorry (11?40Q),Long lorry,Public (12 seats),Taxi,Pick up upto 10Q,Stationwagen,Ridden horse,Other,Bajaj,Turbo,Motorcycle,Special vehicle,Bicycle"
ACCIDENT_AREA = "Residential areas,Office areas,Recreational areas,Industrial areas,Other,Church areas,Market areas,Unknown,Rural village areas,Outside rural areas,Hospital areas,School areas"
LANES_OR_MEDIANS = "Undivided Two way,other,Double carriageway (median),One way,Two-way (divided with solid lines road marking),Two-way (divided with broken lines road marking
```

</details>

---

## 🏁 Conclusion

This pipeline provides a **scalable, secure, and automated ETL solution** for road accident data.
It helps **governments, policymakers, and analysts** gain insights into accident causes and trends, enabling data-driven safety initiatives.

