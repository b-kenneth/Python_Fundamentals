# TMDB Movie Data Analysis with PySpark

## Project Overview

This project implements a complete movie data analysis pipeline using **PySpark**. The goal is to fetch movie data from the TMDB API, process and clean the data, compute key performance indicators (KPIs), and generate insights and visualizations.

By leveraging PySpark, this project can efficiently handle more scalable data analysis, taking advantage of distributed computing for large datasets.

---

## Project Objectives

- **API Data Extraction**: Fetch detailed movie data from the TMDB API for a pre-specified list of movie IDs.
- **Data Cleaning & Transformation**: Process, structure, and clean the dataset for analysis using PySpark.
- **Exploratory Data Analysis (EDA)**: Perform initial exploration to uncover key movie trends.
- **Advanced Filtering & Ranking**: Identify best and worst performing movies using financial and popularity KPIs.
- **Franchise & Director Analysis**: Compare the performance of movie franchises and directors.
- **Visualization & Insights**: Present key findings using Python visualization libraries.

---

## File Structure

```
.
├── data/
│   └── movies_raw.json                 # Raw movie data extracted from the API
├── notebooks/
│   └── movies_analysis.ipynb           # Final Jupyter Notebook for the pipeline
├── src/
│   ├── __init__.py
│   └── utils.py                        # All modular functions for extraction, preprocessing, analysis, visualization
├── .gitignore
├── requirements.txt
└── README.md                           
```

---

## Setup Instructions

### 1. Clone the Repository

```sh
git clone https://github.com/b-kenneth/DataEng_Phase1_labs.git
cd 
```

### 2. Install Dependencies

Create and activate a Python environment (recommended):

```sh
python -m venv venv
source venv/bin/activate  
```

Install all requirements:

```sh
pip install -r requirements.txt
```

### 3. TMDB API Key Configuration

- [Sign up for a TMDB API account](https://www.themoviedb.org/documentation/api).
- Create a `.env` file in the project root with your API access token:

  ```
  API_ACCESS_TOKEN=your_tmdb_bearer_token
  ```

---

## How to Run the Project

### 1. Data Extraction, Cleaning, and Analysis

Open the Jupyter Notebook:

```sh
jupyter notebook notebooks/movies_analysis.ipynb
```

Follow the notebook cells sequentially:

- **Step 1:** Data Extraction
  - Fetches movie data using TMDB API via functions in `src/utils.py`
  - Saves data as `movies_raw.json`
- **Step 2:** Data Cleaning & Preprocessing
  - Drops irrelevant columns
  - Extracts nested JSON fields (collection, genres, production companies, countries, languages, cast, director, etc.)
  - Handles missing/invalid data and converts types
  - Finalizes the schema for analysis
- **Step 3:** KPI Implementation & Analysis
  - Compute and rank by Revenue, Budget, Profit, ROI, Popularity, Ratings, Votes
  - Advanced movie filtering/search (e.g., "Science Fiction Action movies starring Bruce Willis")
  - Franchise vs. Standalone performance
  - Most successful franchises and directors
- **Step 4:** Data Visualization
  - Converts Spark DataFrames to Pandas for plotting (matplotlib/seaborn)
  - Plots Revenue vs. Budget, ROI by Genre, Popularity vs. Rating, Yearly Trends, Franchise/Standalone Box Office

### 2. Use of `utils.py`

- All key functions for each pipeline stage are in `src/utils.py`

## Project Highlights

- **Efficient PySpark Pipeline:** The workflow is entirely modular, using PySpark DataFrames and UDFs for scalable performance.
- **Robust Data Processing:** Handles deeply nested JSON structures and edge cases in the raw API data.
- **Flexible Analysis:** Easily filter, rank, and analyze movies by any KPI or custom query.
- **Professional Visualization:** High-quality plots using Matplotlib after converting to Pandas DataFrames.
