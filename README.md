# TMDB Movie Data Analysis with PySpark

This project implements a movie data analysis pipeline using PySpark. It fetches movie data from the TMDB API, cleans and transforms the dataset, and implements various analyses and KPIs.

## Project Structure

- `data/`: Directory to store any data files
- `notebooks/`: Jupyter notebooks for analysis
- `src/`: Source code modules
  - `config.py`: Configuration settings (API keys, etc.)
  - `data_extraction.py`: Functions for API data extraction
  - `data_preprocessing.py`: Data cleaning and transformation functions
  - `analysis.py`: KPI implementation and analysis functions
  - `visualization.py`: Data visualization functions
- `results/`: Directory to save output figures and tables
- `requirements.txt`: Project dependencies

## Setup

1. Install required packages:
`pip install -r requirements.txt`