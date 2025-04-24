"""
Functions for extracting movie data from the TMDB API.
"""

import os
import requests
import json
import time
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
 

def initialize_spark(app_name="Movie Data Analysis", memory="4g"):
    """
    Initialize a Spark session with specified configurations.
    
    Args:
        app_name (str): Name of the Spark application
        memory (str): Memory allocation for driver
        
    Returns:
        SparkSession: Configured Spark session
    """
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.driver.memory", memory) \
            .getOrCreate()
        
        # Set log level to reduce console output
        spark.sparkContext.setLogLevel("ERROR")
        
        logger.info(f"PySpark session initialized: version {spark.version}")
        return spark
    
    except Exception as e:
        logger.error(f"Failed to initialize Spark session: {e}")
        raise

def fetch_movie_data(movie_id, base_url, api_access_token):
    """
    Fetch details for a single movie using TMDb API.
    
    Args:
        movie_id (int): Movie ID to fetch
        base_url (str): Base URL for the API
        api_access_token (str): API access token
        
    Returns:
        dict: JSON response containing movie details, or None if request fails
    """
    url = f"{base_url}/{movie_id}?append_to_response=credits"
    
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_access_token}"
    }
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            logger.info(f"Successfully fetched data for movie_id={movie_id}")
            return response.json()
        else:
            logger.error(f"Error {response.status_code} for movie_id={movie_id}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data for movie_id={movie_id}: {e}")
        return None

def fetch_all_movies(movie_ids, base_url, api_access_token):
    """
    Fetch data for a list of movie IDs.
    
    Args:
        movie_ids (list): List of movie IDs to fetch
        base_url (str): Base URL for the API
        api_access_token (str): API access token
        
    Returns:
        list: List of dictionaries containing movie details
    """
    logger.info(f"Fetching data for {len(movie_ids)} movies")
    
    movies = []
    for movie_id in movie_ids:
        data = fetch_movie_data(movie_id, base_url, api_access_token)
        if data:  # Only add valid responses
            movies.append(data)
        time.sleep(0.1)  # Rate limiting for API calls
    
    logger.info(f"Successfully fetched data for {len(movies)} out of {len(movie_ids)} movies")
    return movies

def create_spark_dataframe(spark, movie_data):
    """
    Convert a list of movie data dictionaries into a PySpark DataFrame.
    
    Args:
        spark (SparkSession): Active Spark session
        movie_data (list): List of movie data dictionaries
        
    Returns:
        DataFrame: PySpark DataFrame containing structured movie data
    """
    if not movie_data:
        logger.error("No movie data provided to create DataFrame")
        return None
    
    try:
        # Create DataFrame from list of dictionaries
        df = spark.createDataFrame(movie_data)
        logger.info(f"Created DataFrame with {df.count()} movies and {len(df.columns)} columns")
        return df
    
    except Exception as e:
        logger.error(f"Error creating DataFrame: {e}")
        raise
