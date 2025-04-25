"""
Functions for analyzing movie data and implementing KPIs.
"""

from pyspark.sql import functions as F
from pyspark.sql import Window
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
