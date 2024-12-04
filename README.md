# Data Processing on Synthetic Synthea Dataset

## Purpose

This repository is designed to explore and process the **Synthea dataset**, a synthetic dataset that provides healthcare-related data. The dataset (100 Sample Synthetic Patient Records, CSV: 7 MB) can be downloaded from [here](https://synthea.mitre.org/downloads) and comes as a ZIP file containing 18 CSV files. 

For this project, six key files have been selected:
- **conditions.csv**
- **encounters.csv**
- **medications.csv**
- **organisations.csv**
- **patients.csv**
- **procedures.csv**

These files enable tracking the patient journey across interactions with healthcare services. All data is synthetic, ensuring no privacy or compliance risks.

---

## Selected Files Overview

- **patients.csv**: Patient details (e.g., age, gender, state).
- **encounters.csv**: Details of patient encounters (e.g., visit dates and health facility).
- **medications.csv**: Medications prescribed during specific encounters.
- **organisations.csv**: Health facility details.
- **conditions.csv**: Conditions identified during encounters (e.g., housing crisis).
- **procedures.csv**: Procedures performed during encounters.

---

## Repository Structure

```
├── README.md
├── config/
│   └── YAML configuration files for standardising column mappings
├── data/
│   ├── raw_data/ - Contains raw CSV files
│   ├── processed_data/ - Contains processed CSV files
├── eda/
│   └── Notebooks and reports for Exploratory Data Analysis (EDA)
├── etl/
│   ├── notebooks/ - ETL Jupyter notebooks for data processing
│   ├── src/ - Production-ready ETL Python scripts
│       ├── airflow/ - Airflow DAGs for ETL automation
│       ├── push_to_sql/ - Scripts to load data into SQL
│       ├── logs/ - Execution logs
├── prototype/
│   └── Rough notebooks for initial exploration
├── utils/
│   └── Common utility functions for reuse in ETL scripts
├── visualisations/
    └── PowerBI models and dashboard sketches
```

---

## Folder Details

### Config
This folder contains YAML files that document the column-level details of the CSV files, including **conditions.csv**, **encounters.csv**, **medications.csv**, **organisations.csv**, **patients.csv**, and **procedures.csv**. For example, **conditions.yaml** standardizes column names and types across the project lifecycle and provides descriptions of the information each column represents. This serves as good starting point for individuals new to working with healthcare-related data. Additionally, YAML files prefixed with "processed_" define column mappings for the transformed data produced by the ETL scripts.

### Data
1. **raw_data/**: Contains unprocessed CSV files.
2. **processed_data/**: Holds transformed CSV files outputted by ETL processes.

### EDA
This folder is dedicated to exploratory data analysis (EDA) and contains two primary notebooks:  

1. **eda_report-generation.ipynb**: Generates detailed data quality reports on the processed data, outputting them as HTML files. These reports include various data quality metrics such as mean, median, mode, histograms, and box plots. Example reports include **eda_encounter_report.html**, **eda_medications_report.html**, **eda_patients_report.html**, and **eda_procedure_report.html**, with instructions provided on how to view them in a browser.  
2. **eda_insights.ipynb**: Offers a comprehensive, well-documented analysis of the data, highlighting key insights and trends.

### ETL
Core ETL scripts reside here:
1. **notebooks/**: This folder contains interactive ETL notebooks designed for processing raw CSV data. Each notebook takes a raw CSV file and its corresponding YAML configuration file as input and produces a processed file stored in the `data/processed_data` folder. For example, **etl_patients.ipynb** processes the raw file **data/raw_data/patients.csv** using its configuration file **config/patients.yaml** and outputs the processed file as **data/processed_data/processed_patients.csv**.
2. **src/**: This folder contains Python source code for performing ETL operations similar to those in the notebooks. However, it includes well-documented, modular code designed for deployment in production environments. Execution logs are saved in the `src/logs` folder, which provides detailed information about script execution. Additionally, the folder includes subdirectories: **"airflow"** for preliminary code to convert ETL processes into Airflow DAGs and **"push_to_sql"** for example Python scripts to load data frames into a specified SQL database. Screenshots are provided to illustrate the execution workflow for both Airflow DAGs and SQL load scripts.

### Prototype
Houses initial exploratory notebooks for rough data analysis and experimentation.

### Utils
Provides reusable utility functions to maintain modularity across ETL scripts.

### Visualisations
This folder is dedicated to sharing and documenting ideas for dashboards and report generation through brainstorming sessions. Currently, we have chosen PowerBI for its interactive features, including the ability to filter data and use dropdowns. For example, our initial brainstorming session resulted in the creation of **datamodel_powerBI.png**, a screenshot illustrating the data model and relationships across tables (e.g., how a single patient in **patients.csv** can have multiple encounters in **encounters.csv**). Another output from this session is **powerBI_dashboard_rough-sketch_v1.png**, which outlines a preliminary layout for filters and visualizations on a single-page report. This folder will serve as a repository for all collaboration and documentation efforts as we refine and develop high-quality reports.

---

## Prerequisites

The notebooks and scripts were developed with:
- **Python 3.11.6**
- **Ubuntu 20.04.6 LTS**

Ensure the following Python libraries are installed:
- matplotlib==3.9.3
- numpy==1.26.4
- pandas==2.2.1
- seaborn==0.13.2
- ydata-profiling==4.12.0

---

## Setup and Usage

### Clone the Repository
```bash
git clone <repository-url>
cd <repository-folder>
```

### Prepare the Dataset
1. Download the ZIP file (100 Sample Synthetic Patient Records, CSV: 7 MB) from the [Synthea dataset link](https://synthea.mitre.org/downloads).
2. Extract the contents and place the selected CSV files in `data/raw_data/`.
3. Ensure corresponding YAML configuration files are in the `config/` directory.

### Run ETL Notebooks
1. Start with **etl_patients.ipynb**, as other notebooks rely on patient data for join operations.
2. Provide paths to the respective raw CSV and YAML files in the notebook.

### Run ETL Scripts
Switch to the `etl/src/` directory and execute Python scripts:
```bash
python etl_patients.py > logs/logs-etl_patients.txt
```
and the all remaining python scripts in any order. For instance:

```bash
python etl_encounters.py > logs/logs-etl_encounters.txt
```

Logs are saved in the `logs/` folder for reference.

---

## Data Quality Reports
To view a report, such as `eda/eda_encounter_report.html`, in your browser, you can use a service like HTML Preview. Follow these steps:

1. Get the file URL of the report. For example, the URL for patient's encounter report is:  
   `https://github.com/shaukat-abidi/cancer-data-analysis/blob/main/eda/eda_encounter_report.html`

2. Open a browser and modify the URL for HTML Preview. Add the report's URL after `https://htmlpreview.github.io/?` to create the following link:  
   `https://htmlpreview.github.io/?https://github.com/shaukat-abidi/cancer-data-analysis/blob/main/eda/eda_encounter_report.html`

3. Copy and paste the modified URL into your browser to render the report as HTML.

You can use the same method to view other reports, such as:  
- `https://htmlpreview.github.io/?https://github.com/shaukat-abidi/cancer-data-analysis/blob/main/eda/eda_medications_report.html`  
- `https://htmlpreview.github.io/?https://github.com/shaukat-abidi/cancer-data-analysis/blob/main/eda/eda_patients_report.html`  
- `https://htmlpreview.github.io/?https://github.com/shaukat-abidi/cancer-data-analysis/blob/main/eda/eda_procedure_report.html`

## Improvements and Future Work

1. **Dynamic Paths**: Use the `argparse` module to specify file paths dynamically in scripts.
2. **Data Governance**: Implement data governance checks alongside data quality measures for the modified CSV files. For example, include a column to track any modifications to the data, such as changes in case (e.g., "Ibuprofen 100 MG Oral Tablet" to "ibuprofen 100 mg oral tablet"). This will support future data audits and ensure compliance with regulatory requirements.
3. **Database Integration**: Load processed data into SQL databasesinstead of CSV.
4. **PowerBI Enhancements**: Connect dashboards directly to SQL databases (reporting and presentation layer - data marts) for real-time updates.
5. **Data Lineage and Compliance**: Leverage tools like Azure Purview for lineage tracking and governance.
6. **Feedbacks**: Feedback and contributions are welcome!

---
