
"""
Functions for cleaning and preprocessing movie data.
"""

import json
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType, BooleanType, ArrayType
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

@F.udf(StringType())
def extract_collection_name(collection_json):
    """
    Extract collection name from belongs_to_collection JSON string.
    
    Args:
        collection_json (str): JSON string containing collection information
        
    Returns:
        str: Collection name or None
    """
    if collection_json is None:
        return None
    try:
        if isinstance(collection_json, str):
            collection = json.loads(collection_json.replace("'", "\""))
        else:
            collection = collection_json
        return collection.get("name")
    except:
        return None

@F.udf(StringType())
def extract_names_from_array(array_json, key="name"):
    """
    Extract names from array of objects and join them with a pipe.
    
    Args:
        array_json (str): JSON string containing array of objects
        key (str): Key to extract from each object
        
    Returns:
        str: Pipe-separated string of extracted values
    """
    if array_json is None:
        return None
    try:
        if isinstance(array_json, str):
            array = json.loads(array_json.replace("'", "\""))
        else:
            array = array_json
        names = [item.get(key) for item in array if key in item]
        return "|".join(names) if names else None
    except:
        return None

@F.udf(StringType())
def extract_director(credits_json):
    """
    Extract director names from credits JSON string.
    
    Args:
        credits_json (str): JSON string containing credits information
        
    Returns:
        str: Pipe-separated string of director names
    """
    if credits_json is None:
        return None
    try:
        if isinstance(credits_json, str):
            credits = json.loads(credits_json.replace("'", "\""))
        else:
            credits = credits_json
        crew = credits.get("crew", [])
        directors = [person.get("name") for person in crew if person.get("job") == "Director"]
        return "|".join(directors) if directors else None
    except:
        return None

@F.udf(StringType())
def extract_cast(credits_json):
    """
    Extract cast names from credits JSON string.
    
    Args:
        credits_json (str): JSON string containing credits information
        
    Returns:
        str: Pipe-separated string of cast names
    """
    if credits_json is None:
        return None
    try:
        if isinstance(credits_json, str):
            credits = json.loads(credits_json.replace("'", "\""))
        else:
            credits = credits_json
        cast = credits.get("cast", [])
        cast_names = [person.get("name") for person in cast if "name" in person]
        return "|".join(cast_names) if cast_names else None
    except:
        return None

@F.udf(IntegerType())
def get_cast_size(credits_json):
    """
    Get the number of cast members from credits JSON string.
    
    Args:
        credits_json (str): JSON string containing credits information
        
    Returns:
        int: Number of cast members
    """
    if credits_json is None:
        return 0
    try:
        if isinstance(credits_json, str):
            credits = json.loads(credits_json.replace("'", "\""))
        else:
            credits = credits_json
        return len(credits.get("cast", []))
    except:
        return 0

@F.udf(IntegerType())
def get_crew_size(credits_json):
    """
    Get the number of crew members from credits JSON string.
    
    Args:
        credits_json (str): JSON string containing credits information
        
    Returns:
        int: Number of crew members
    """
    if credits_json is None:
        return 0
    try:
        if isinstance(credits_json, str):
            credits = json.loads(credits_json.replace("'", "\""))
        else:
            credits = credits_json
        return len(credits.get("crew", []))
    except:
        return 0

def process_complex_columns(df):
    """
    Process complex JSON-like columns and extract relevant information.
    
    Args:
        df (DataFrame): PySpark DataFrame containing movie data
        
    Returns:
        DataFrame: DataFrame with extracted data
    """
    logger.info("Processing complex columns")
    
    try:
        # Extract data from collection, genres, countries, companies, languages
        df = df \
            .withColumn("collection_name", extract_collection_name(F.col("belongs_to_collection"))) \
            .withColumn("genre_names", extract_names_from_array(F.col("genres"))) \
            .withColumn("production_countries_names", 
                        extract_names_from_array(F.col("production_countries"), F.lit("iso_3166_1"))) \
            .withColumn("production_companies_names", 
                        extract_names_from_array(F.col("production_companies"))) \
            .withColumn("spoken_languages_names", 
                        extract_names_from_array(F.col("spoken_languages"), F.lit("iso_639_1")))
        
        # Extract director and cast information
        df = df \
            .withColumn("director", extract_director(F.col("credits"))) \
            .withColumn("cast", extract_cast(F.col("credits"))) \
            .withColumn("cast_size", get_cast_size(F.col("credits"))) \
            .withColumn("crew_size", get_crew_size(F.col("credits")))
        
        logger.info("Successfully processed complex columns")
        return df
    
    except Exception as e:
        logger.error(f"Error processing complex columns: {e}")
        raise

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
        
        # Step 2: Process complex JSON-like columns
        df = process_complex_columns(df)
        
        # Step 3: Convert datatypes
        df = convert_datatypes(df)
        
        # Step 4: Handle missing and incorrect data
        df = handle_missing_data(df)
        
        # Step 5: Reorder and finalize DataFrame
        df = organize_final_dataframe(df, final_column_order)
        
        logger.info("Preprocessing pipeline completed successfully")
        return df
    
    except Exception as e:
        logger.error(f"Error in preprocessing pipeline: {e}")
        raise
