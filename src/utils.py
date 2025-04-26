"""
Functions for extracting movie data from the TMDB API.
"""

import os
import requests
import json
import time
from pyspark.sql import SparkSession, functions as F, Window
from dotenv import load_dotenv
import logging
import sys
from pyspark.sql.types import StringType, IntegerType, DoubleType, BooleanType, ArrayType
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


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



"""
Functions for cleaning and preprocessing movie data.
"""

def drop_irrelevant_columns(df, columns_to_drop):
    """
    Drop specified columns from the DataFrame.
    
    Args:
        df (DataFrame): PySpark DataFrame containing movie data
        columns_to_drop (list): List of column names to drop
        
    Returns:
        DataFrame: DataFrame with columns removed
    """
    # Filter to only include columns that exist in the DataFrame
    columns_to_drop = [col for col in columns_to_drop if col in df.columns]
    
    if columns_to_drop:
        df = df.drop(*columns_to_drop)
        logger.info(f"Dropped columns: {columns_to_drop}")
    else:
        logger.info("No columns to drop")
    
    return df

from pyspark.sql import functions as F

def extract_data(df, source_column, target_column, field_name=None, join_char='|'):
    """
    Extract data from complex JSON-like structures in a PySpark DataFrame.
    
    Args:
        df (DataFrame): PySpark DataFrame containing movie data
        source_column (str): Name of the column containing the data to extract
        target_column (str): Name of the new column to create
        field_name (str, optional): Name of the field to extract from each item
        join_char (str, optional): Character to use when joining multiple values
        
    Returns:
        DataFrame: DataFrame with extracted data
    """
    # Log extraction (optional - assuming logger is defined)
    print(f"Extracting {field_name or source_column} from {source_column} to {target_column}")

    # For a single dictionary object (like 'belongs_to_collection')
    if field_name and not source_column in ["genres", "production_companies", "production_countries", "spoken_languages"]:
        df = df.withColumn(target_column, F.col(f"{source_column}.{field_name}"))

    # For arrays of dictionaries (like genres, etc.)
    elif field_name:
        df = df.withColumn(
            target_column,
            F.expr(f"concat_ws('{join_char}', transform({source_column}, x -> x['{field_name}']))")
        )

    return df

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType

def process_credits(df, source_column, target_column, operation_type, job_filter=None):
    """
    Process movie credits data for various operations.

    Args:
        df (DataFrame): PySpark DataFrame containing movie data
        source_column (str): Name of the column containing the credits data
        target_column (str): Name of the new column to create
        operation_type (str): Type of operation to perform ('extract_names', 'count_members', 'extract_by_job')
        job_filter (str, optional): Job title to filter by (used only with 'extract_by_job')

    Returns:
        DataFrame: DataFrame with processed credits data
    """
    print(f"Processing credits: {operation_type} from {source_column} to {target_column}")

    if operation_type == 'extract_names':
        # Extract cast or crew names
        @F.udf(StringType())
        def extract_names_udf(credits_obj):
            try:
                section = 'cast' if 'cast' in target_column.lower() else 'crew'
                if credits_obj and section in credits_obj:
                    return "|".join([member['name'] for member in credits_obj[section]])
            except Exception as e:
                print(f"Error extracting names: {e}")
            return None

        df = df.withColumn(target_column, extract_names_udf(F.col(source_column)))

    elif operation_type == 'count_members':
        # Count number of cast or crew
        @F.udf(IntegerType())
        def count_members_udf(credits_obj):
            try:
                section = 'cast' if 'cast' in target_column.lower() else 'crew'
                if credits_obj and section in credits_obj:
                    return len(credits_obj[section])
            except Exception as e:
                print(f"Error counting members: {e}")
            return 0

        df = df.withColumn(target_column, count_members_udf(F.col(source_column)))

    elif operation_type == 'extract_by_job' and job_filter:
        # Extract names with a specific job in crew
        @F.udf(StringType())
        def extract_by_job_udf(credits_obj):
            try:
                if credits_obj and 'crew' in credits_obj:
                    names = [member['name'] for member in credits_obj['crew'] if member['job'] == job_filter]
                    return "|".join(names)
            except Exception as e:
                print(f"Error extracting by job: {e}")
            return None

        df = df.withColumn(target_column, extract_by_job_udf(F.col(source_column)))

    return df

def convert_datatypes(df):
    """
    Convert columns to appropriate data types.
    
    Args:
        df (DataFrame): PySpark DataFrame containing movie data
        
    Returns:
        DataFrame: DataFrame with converted data types
    """
    logger.info("Converting column data types")
    
    try:
        df = df \
            .withColumn("budget", F.col("budget").cast(DoubleType())) \
            .withColumn("revenue", F.col("revenue").cast(DoubleType())) \
            .withColumn("id", F.col("id").cast(IntegerType())) \
            .withColumn("popularity", F.col("popularity").cast(DoubleType())) \
            .withColumn("runtime", F.col("runtime").cast(IntegerType())) \
            .withColumn("vote_count", F.col("vote_count").cast(IntegerType())) \
            .withColumn("vote_average", F.col("vote_average").cast(DoubleType())) \
            .withColumn("release_date", F.to_date(F.col("release_date"), "yyyy-MM-dd"))
        
        logger.info("Successfully converted column data types")
        return df
    
    except Exception as e:
        logger.error(f"Error converting data types: {e}")
        raise

def handle_missing_data(df):
    """
    Handle missing, incorrect, and unrealistic values in the DataFrame.
    
    Args:
        df (DataFrame): PySpark DataFrame containing movie data
        
    Returns:
        DataFrame: DataFrame with handled missing values
    """
    logger.info("Handling missing and incorrect data")
    
    try:
        # Convert budget and revenue to million USD and handle unrealistic values
        df = df \
            .withColumn("budget_musd", 
                        F.when(F.col("budget") > 0, F.col("budget") / 1000000).otherwise(None)) \
            .withColumn("revenue_musd", 
                        F.when(F.col("revenue") > 0, F.col("revenue") / 1000000).otherwise(None))
        
        # Handle missing values in vote_average
        df = df \
            .withColumn("vote_average", 
                        F.when(F.col("vote_count") == 0, None).otherwise(F.col("vote_average")))
        
        # Replace placeholders in text fields
        df = df \
            .withColumn("overview", 
                        F.when(F.col("overview") == "No Data", None).otherwise(F.col("overview"))) \
            .withColumn("tagline", 
                        F.when(F.col("tagline") == "No Data", None).otherwise(F.col("tagline")))
        
        # Filter to include only released movies
        if "status" in df.columns:
            df = df.filter(F.col("status") == "Released")
            df = df.drop("status")
        
        # Filter rows with at least 10 non-null columns
        non_null_count = sum([F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in df.columns])
        df = df.filter(non_null_count >= 10)
        
        logger.info("Successfully handled missing and incorrect data")
        return df
    
    except Exception as e:
        logger.error(f"Error handling missing data: {e}")
        raise

def organize_final_dataframe(df, column_order):
    """
    Organize and finalize the DataFrame with specified column order.
    
    Args:
        df (DataFrame): PySpark DataFrame containing movie data
        column_order (list): List of column names in desired order
        
    Returns:
        DataFrame: Final organized DataFrame
    """
    logger.info("Organizing final DataFrame")
    
    try:
        # Filter column order to only include columns that exist
        final_columns = [col for col in column_order if col in df.columns]
        
        # Select columns in specified order
        df = df.select(final_columns)
        
        # Cache the DataFrame for better performance
        df.cache()
        
        logger.info(f"Final DataFrame has {df.count()} rows and {len(df.columns)} columns")
        return df
    
    except Exception as e:
        logger.error(f"Error organizing final DataFrame: {e}")
        raise

def preprocess_movie_data(df, columns_to_drop, final_column_order):
    """
    Complete preprocessing pipeline for movie data.
    
    Args:
        df (DataFrame): Raw PySpark DataFrame containing movie data
        columns_to_drop (list): List of column names to drop
        final_column_order (list): List of column names in desired order
        
    Returns:
        DataFrame: Fully preprocessed DataFrame
    """
    logger.info("Starting preprocessing pipeline")
    
    try:
        # Step 1: Drop irrelevant columns
        df = drop_irrelevant_columns(df, columns_to_drop)
        
        # Step 2: Extract collection name from belongs_to_collection
        df = extract_data(df, "belongs_to_collection", "collection_name", "name")
        
        # Step 3: Extract genre names from genres
        df = extract_data(df, "genres", "genre_names", "name")
        
        # Step 4: Extract production countries
        df = extract_data(df, "production_countries", "production_countries_names", "name")
        
        # Step 5: Extract production companies
        df = extract_data(df, "production_companies", "production_companies_names", "name")
        
        # Step 6: Extract spoken languages
        df = extract_data(df, "spoken_languages", "spoken_languages_names", "english_name")
        
        # Step 7: Extract cast names
        df = process_credits(df, "credits", "cast", "extract_names")
        
        # Step 8: Get cast size
        df = process_credits(df, "credits", "cast_size", "count_members")
        
        # Step 9: Extract director names
        df = process_credits(df, "credits", "director", "extract_by_job", "Director")
        
        # Step 10: Get crew size
        df = process_credits(df, "credits", "crew_size", "count_members")
        
        # Step 11: Convert datatypes
        df = convert_datatypes(df)
        
        # Step 12: Handle missing and incorrect data
        df = handle_missing_data(df)
        
        # Step 13: Reorder and finalize DataFrame
        df = organize_final_dataframe(df, final_column_order)
        
        logger.info("Preprocessing pipeline completed successfully")
        return df
    
    except Exception as e:
        logger.error(f"Error in preprocessing pipeline: {e}")
        raise



"""
Functions for analyzing movie data and implementing KPIs.
"""

def calculate_financial_metrics(df):
    """
    Calculate profit and ROI metrics.
    
    Args:
        df (DataFrame): PySpark DataFrame containing movie data
        
    Returns:
        DataFrame: DataFrame with added profit and ROI columns
    """
    logger.info("Calculating financial metrics")
    
    try:
        df = df.withColumn("profit_musd", 
                          F.col("revenue_musd") - F.col("budget_musd"))
        
        df = df.withColumn("roi", 
                          F.when(F.col("budget_musd") > 0, 
                                F.col("revenue_musd") / F.col("budget_musd"))
                          .otherwise(None))
        
        logger.info("Successfully calculated financial metrics")
        return df
    
    except Exception as e:
        logger.error(f"Error calculating financial metrics: {e}")
        raise

def rank_movies(df, column_name, ascending=False, min_filter=None, filter_column=None, limit=10):
    """
    Rank movies based on a specific column with optional filtering.
    
    Args:
        df (DataFrame): PySpark DataFrame with movie data
        column_name (str): Column to rank by
        ascending (bool): Sort in ascending order if True
        min_filter (float/int): Minimum value for filter_column
        filter_column (str): Column to apply the minimum filter
        limit (int): Number of results to return
        
    Returns:
        DataFrame: Ranked DataFrame
    """
    try:
        # Create a filtered DataFrame if filter is specified
        if min_filter is not None and filter_column is not None:
            filtered_df = df.filter(F.col(filter_column) >= min_filter)
        else:
            filtered_df = df
        
        # Sort based on direction
        if ascending:
            sorted_df = filtered_df.orderBy(F.col(column_name).asc())
        else:
            sorted_df = filtered_df.orderBy(F.col(column_name).desc())
        
        # Return with limit
        return sorted_df.limit(limit)
    
    except Exception as e:
        logger.error(f"Error ranking movies by {column_name}: {e}")
        raise

def get_highest_revenue_movies(df, limit=10):
    """
    Get the highest revenue movies.
    
    Args:
        df (DataFrame): PySpark DataFrame with movie data
        limit (int): Number of results to return
        
    Returns:
        DataFrame: DataFrame with highest revenue movies
    """
    logger.info(f"Finding top {limit} highest revenue movies")
    return rank_movies(df, "revenue_musd", ascending=False, limit=limit)

def get_highest_budget_movies(df, limit=10):
    """
    Get the highest budget movies.
    
    Args:
        df (DataFrame): PySpark DataFrame with movie data
        limit (int): Number of results to return
        
    Returns:
        DataFrame: DataFrame with highest budget movies
    """
    logger.info(f"Finding top {limit} highest budget movies")
    return rank_movies(df, "budget_musd", ascending=False, limit=limit)

def get_highest_profit_movies(df, limit=10):
    """
    Get the highest profit movies.
    
    Args:
        df (DataFrame): PySpark DataFrame with movie data
        limit (int): Number of results to return
        
    Returns:
        DataFrame: DataFrame with highest profit movies
    """
    logger.info(f"Finding top {limit} highest profit movies")
    return rank_movies(df, "profit_musd", ascending=False, limit=limit)

def get_lowest_profit_movies(df, limit=10):
    """
    Get the lowest profit movies.
    
    Args:
        df (DataFrame): PySpark DataFrame with movie data
        limit (int): Number of results to return
        
    Returns:
        DataFrame: DataFrame with lowest profit movies
    """
    logger.info(f"Finding top {limit} lowest profit movies")
    return rank_movies(df, "profit_musd", ascending=True, limit=limit)

def get_highest_roi_movies(df, limit=10, min_budget=10):
    """
    Get the highest ROI movies with a minimum budget.
    
    Args:
        df (DataFrame): PySpark DataFrame with movie data
        limit (int): Number of results to return
        min_budget (float): Minimum budget in million USD
        
    Returns:
        DataFrame: DataFrame with highest ROI movies
    """
    logger.info(f"Finding top {limit} highest ROI movies (budget >= {min_budget}M)")
    return rank_movies(df, "roi", ascending=False, min_filter=min_budget, filter_column="budget_musd", limit=limit)

def get_lowest_roi_movies(df, limit=10, min_budget=10):
    """
    Get the lowest ROI movies with a minimum budget.
    
    Args:
        df (DataFrame): PySpark DataFrame with movie data
        limit (int): Number of results to return
        min_budget (float): Minimum budget in million USD
        
    Returns:
        DataFrame: DataFrame with lowest ROI movies
    """
    logger.info(f"Finding top {limit} lowest ROI movies (budget >= {min_budget}M)")
    return rank_movies(df, "roi", ascending=True, min_filter=min_budget, filter_column="budget_musd", limit=limit)

def get_most_voted_movies(df, limit=10):
    """
    Get the most voted movies.
    
    Args:
        df (DataFrame): PySpark DataFrame with movie data
        limit (int): Number of results to return
        
    Returns:
        DataFrame: DataFrame with most voted movies
    """
    logger.info(f"Finding top {limit} most voted movies")
    return rank_movies(df, "vote_count", ascending=False, limit=limit)

def get_highest_rated_movies(df, limit=10, min_votes=10):
    """
    Get the highest rated movies with a minimum number of votes.
    
    Args:
        df (DataFrame): PySpark DataFrame with movie data
        limit (int): Number of results to return
        min_votes (int): Minimum number of votes
        
    Returns:
        DataFrame: DataFrame with highest rated movies
    """
    logger.info(f"Finding top {limit} highest rated movies (votes >= {min_votes})")
    return rank_movies(df, "vote_average", ascending=False, min_filter=min_votes, filter_column="vote_count", limit=limit)

def get_lowest_rated_movies(df, limit=10, min_votes=10):
    """
    Get the lowest rated movies with a minimum number of votes.
    
    Args:
        df (DataFrame): PySpark DataFrame with movie data
        limit (int): Number of results to return
        min_votes (int): Minimum number of votes
        
    Returns:
        DataFrame: DataFrame with lowest rated movies
    """
    logger.info(f"Finding top {limit} lowest rated movies (votes >= {min_votes})")
    return rank_movies(df, "vote_average", ascending=True, min_filter=min_votes, filter_column="vote_count", limit=limit)

def get_most_popular_movies(df, limit=10):
    """
    Get the most popular movies.
    
    Args:
        df (DataFrame): PySpark DataFrame with movie data
        limit (int): Number of results to return
        
    Returns:
        DataFrame: DataFrame with most popular movies
    """
    logger.info(f"Finding top {limit} most popular movies")
    return rank_movies(df, "popularity", ascending=False, limit=limit)

def search_scifi_action_willis(df):
    """
    Search for Science Fiction Action movies starring Bruce Willis.
    
    Args:
        df (DataFrame): PySpark DataFrame with movie data
        
    Returns:
        DataFrame: Filtered and sorted DataFrame
    """
    logger.info("Searching for Science Fiction Action movies starring Bruce Willis")
    
    try:
        result = df \
            .filter(
                F.col("genre_names").contains("Science Fiction") & 
                F.col("genre_names").contains("Action") & 
                F.col("cast").contains("Bruce Willis")
            ) \
            .orderBy(F.col("vote_average").desc())
        
        return result
    
    except Exception as e:
        logger.error(f"Error searching for Science Fiction Action movies starring Bruce Willis: {e}")
        raise

def search_thurman_tarantino(df):
    """
    Search for movies starring Uma Thurman and directed by Quentin Tarantino.
    
    Args:
        df (DataFrame): PySpark DataFrame with movie data
        
    Returns:
        DataFrame: Filtered and sorted DataFrame
    """
    logger.info("Searching for movies starring Uma Thurman and directed by Quentin Tarantino")
    
    try:
        result = df \
            .filter(
                F.col("cast").contains("Uma Thurman") & 
                F.col("director").contains("Quentin Tarantino")
            ) \
            .orderBy(F.col("runtime").asc())
        
        return result
    
    except Exception as e:
        logger.error(f"Error searching for movies starring Uma Thurman and directed by Quentin Tarantino: {e}")
        raise

def compare_franchise_vs_standalone(df):
    """
    Compare franchise vs. standalone movie performance.
    
    Args:
        df (DataFrame): PySpark DataFrame with movie data
        
    Returns:
        DataFrame: Comparison DataFrame
    """
    logger.info("Comparing franchise vs. standalone movie performance")
    
    try:
        # Define franchise and standalone movies
        franchise_movies = df.filter(F.col("collection_name").isNotNull())
        standalone_movies = df.filter(F.col("collection_name").isNull())
        
        # Calculate aggregated metrics for comparison
        franchise_metrics = franchise_movies.agg(
            F.round(F.mean("revenue_musd"), 3).alias("mean_revenue"),
            F.round(F.expr("percentile_approx(roi, 0.5)"), 3).alias("median_roi"),
            F.round(F.mean("budget_musd"), 3).alias("mean_budget"),
            F.round(F.mean("popularity"), 3).alias("mean_popularity"),
            F.round(F.mean("vote_average"), 3).alias("mean_rating")
        ).withColumn("movie_type", F.lit("Franchise"))

        standalone_metrics = standalone_movies.agg(
            F.round(F.mean("revenue_musd"), 3).alias("mean_revenue"),
            F.round(F.expr("percentile_approx(roi, 0.5)"), 3).alias("median_roi"),
            F.round(F.mean("budget_musd"), 3).alias("mean_budget"),
            F.round(F.mean("popularity"), 3).alias("mean_popularity"),
            F.round(F.mean("vote_average"), 3).alias("mean_rating")
        ).withColumn("movie_type", F.lit("Standalone"))

        
        # Union the results for comparison
        comparison = franchise_metrics.union(standalone_metrics)
        
        return comparison
    
    except Exception as e:
        logger.error(f"Error comparing franchise vs. standalone movie performance: {e}")
        raise

def get_successful_franchises(spark, limit=10):
    """
    Get the most successful movie franchises.
    
    Args:
        spark (SparkSession): Active Spark session
        limit (int): Number of results to return
        
    Returns:
        DataFrame: DataFrame with successful franchises
    """
    logger.info(f"Finding top {limit} most successful movie franchises")
    
    try:
        successful_franchises = spark.sql(f"""
            SELECT 
                collection_name,
                COUNT(*) as movie_count,
                SUM(budget_musd) as total_budget,
                AVG(budget_musd) as mean_budget,
                SUM(revenue_musd) as total_revenue,
                AVG(revenue_musd) as mean_revenue,
                AVG(vote_average) as mean_rating
            FROM 
                movies
            WHERE 
                collection_name IS NOT NULL
            GROUP BY 
                collection_name
            ORDER BY 
                total_revenue DESC
            LIMIT {limit}
        """)
        
        return successful_franchises
    
    except Exception as e:
        logger.error(f"Error finding successful franchises: {e}")
        raise

def get_successful_directors(spark, limit=10):
    """
    Get the most successful directors.
    
    Args:
        spark (SparkSession): Active Spark session
        limit (int): Number of results to return
        
    Returns:
        DataFrame: DataFrame with successful directors
    """
    logger.info(f"Finding top {limit} most successful directors")
    
    try:
        successful_directors = spark.sql(f"""
            SELECT 
                director,
                COUNT(*) as movies_directed,
                SUM(revenue_musd) as total_revenue,
                AVG(vote_average) as mean_rating
            FROM 
                movies
            WHERE 
                director IS NOT NULL
            GROUP BY 
                director
            ORDER BY 
                total_revenue DESC
            LIMIT {limit}
        """)
        
        return successful_directors
    
    except Exception as e:
        logger.error(f"Error finding successful directors: {e}")
        raise




"""
Functions for plotting graphs.
"""

def plot_budget_vs_revenue_trends(cleaned_movies_df):
    cleaned_movies_df['release_year'] = pd.to_datetime(cleaned_movies_df['release_date']).dt.year
    yearly_trends = cleaned_movies_df.groupby('release_year')[['budget_musd', 'revenue_musd']].sum().reset_index()

    plt.figure(figsize=(12, 6))
    plt.plot(yearly_trends['release_year'], yearly_trends['budget_musd'], label='Total Budget (M USD)', color='darkorange', marker='o')
    plt.plot(yearly_trends['release_year'], yearly_trends['revenue_musd'], label='Total Revenue (M USD)', color='blue', marker='o')
    plt.title('Yearly Trends in Revenue and Budget', fontsize=16)
    plt.xlabel('Year', fontsize=14)
    plt.ylabel('Amount (Million USD)', fontsize=14)
    plt.legend(fontsize=12)
    plt.grid(True)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    plt.show()

def plot_mean_roi_by_genre(cleaned_movies_df):
    df = cleaned_movies_df.copy()
    df['ROI'] = df['revenue_musd'] / df['budget_musd']
    df['genre_names'] = df['genre_names'].str.split('|')
    genre_df = df.explode('genre_names').query('budget_musd > 0')
    mean_roi = genre_df.groupby('genre_names')['ROI'].mean().sort_values(ascending=False)
    plt.figure(figsize=(12, 6))
    mean_roi.plot(kind='bar', color='skyblue')
    plt.title('Mean ROI by Genre')
    plt.xlabel('Genre')
    plt.ylabel('Mean ROI')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def plot_popularity_vs_rating(cleaned_movies_df):
    valid_data = cleaned_movies_df[
        (cleaned_movies_df['popularity'].notna()) & 
        (cleaned_movies_df['vote_average'].notna())
    ]
    plt.figure(figsize=(10, 6))
    plt.scatter(valid_data['popularity'], valid_data['vote_average'], alpha=0.7, color='blue')
    z = np.polyfit(valid_data['popularity'], valid_data['vote_average'], 1)
    p = np.poly1d(z)
    plt.plot(valid_data['popularity'], p(valid_data['popularity']), color='red', linestyle='--', label='Trendline')
    plt.title('Popularity vs. Rating', fontsize=16)
    plt.xlabel('Popularity', fontsize=14)
    plt.ylabel('Vote Average (Rating)', fontsize=14)
    plt.grid(True)
    plt.legend()
    plt.show()

def plot_yearly_revenue_stats(cleaned_movies_df):
    cleaned_movies_df['release_year'] = pd.to_datetime(cleaned_movies_df['release_date']).dt.year
    yearly_revenue_stats = cleaned_movies_df.groupby('release_year')['revenue_musd'].agg(['sum', 'mean']).reset_index()
    yearly_revenue_stats.columns = ['Year', 'Total Revenue (M USD)', 'Mean Revenue (M USD)']
    plt.figure(figsize=(12, 6))
    plt.plot(yearly_revenue_stats['Year'], yearly_revenue_stats['Total Revenue (M USD)'], label='Total Revenue', color='blue', marker='o')
    plt.plot(yearly_revenue_stats['Year'], yearly_revenue_stats['Mean Revenue (M USD)'], label='Mean Revenue', color='green', marker='o')
    plt.title('Yearly Trends in Box Office Performance', fontsize=16)
    plt.xlabel('Year', fontsize=14)
    plt.ylabel('Revenue (Million USD)', fontsize=14)
    plt.legend(fontsize=12)
    plt.grid(True)
    plt.show()

def plot_franchise_vs_standalone(cleaned_movies_df):
    cleaned_movies_df['movie_type'] = cleaned_movies_df['collection_name'].apply(lambda x: 'Franchise' if pd.notna(x) else 'Standalone')
    movie_type_stats = cleaned_movies_df.groupby('movie_type').agg(
        {'revenue_musd': ['sum', 'mean'], 'vote_average': 'mean'}
    ).reset_index()
    movie_type_stats.columns = ['Movie Type', 'Total Revenue (M USD)', 'Mean Revenue (M USD)', 'Mean Rating']
    plt.figure(figsize=(12, 6))
    x = movie_type_stats['Movie Type']
    total_revenue = movie_type_stats['Total Revenue (M USD)']
    mean_revenue = movie_type_stats['Mean Revenue (M USD)']
    mean_rating = movie_type_stats['Mean Rating']
    width = 0.2
    plt.bar(x, total_revenue, width=0.2, label='Total Revenue', color='blue', align='edge')
    plt.bar(x, mean_revenue, width=-0.2, label='Mean Revenue', color='green', align='edge')
    plt.bar(x, mean_rating * 100, width=-0.2, label='Mean Rating (Scaled)', color='orange', align='center')
    plt.title('Comparison of Franchise vs. Standalone Success', fontsize=16)
    plt.xlabel('Movie Type', fontsize=14)
    plt.ylabel('Metrics', fontsize=14)
    plt.legend(fontsize=12)
    plt.grid(True)
    plt.xticks(fontsize=12)
    plt.show()


def plot_all_graphs(cleaned_movies_df):
    """
    Calls all graph plotting functions in sequence.
    """
    plot_budget_vs_revenue_trends(cleaned_movies_df)
    plot_mean_roi_by_genre(cleaned_movies_df)
    plot_popularity_vs_rating(cleaned_movies_df)
    plot_yearly_revenue_stats(cleaned_movies_df)
    plot_franchise_vs_standalone(cleaned_movies_df)

