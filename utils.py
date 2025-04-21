import os
from dotenv import load_dotenv
import requests
import pandas as pd
import logging
import time
import json


# Load variables from .env file
load_dotenv()

# Access the API access token
api_access_token = os.getenv('API_ACCESS_TOKEN')

def fetch_movie_data(movie_id, BASE_URL):
    """
    Fetch details for a single movie using TMDb API and returns
    a JSON response containing movie details, or None if the request fails.
    """
    url = f"{BASE_URL}/{movie_id}?append_to_response=credits"
    # https://api.themoviedb.org/3/movie/11?append_to_response=videos
    # ?append_to_response=videos

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_access_token}"
    }
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()  # Return JSON data if successful
        else:
            print(f"Error {response.status_code} for movie_id={movie_id}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for movie_id={movie_id}: {e}")
        return None

def fetch_all_movies(movie_ids, BASE_URL):
    """
    Fetch data for a list of movie IDs and returns
    a list of dictionaries containing movie details.
    """
    movies = []
    for movie_id in movie_ids:
        data = fetch_movie_data(movie_id, BASE_URL)
        if data:  # Only add valid responses
            movies.append(data)
        time.sleep(0.1) # Rate limiting in place for future scalability; current usage stays within limits.

    return movies

def save_to_dataframe(movie_data):
    """
    Converts a list of movie data dictionaries into a Pandas DataFrame 
    returns Pandas DataFrame containing structured movie data.
    """
    df = pd.DataFrame(movie_data)
    return df


def extract_data(df, source_column, target_column, field_name=None, join_char='|', drop_source=True):
    """
    Extract data from complex structures in a pandas DataFrame.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        The DataFrame to process
    source_column : str
        Name of the column containing the data to extract
    target_column : str
        Name of the new column to create
    field_name : str or None
        Name of the field to extract from each item. If None, the whole item is used.
        For singular items, it's the key to extract from the dictionary.
        For lists of items, it's the key to extract from each dictionary in the list.
    join_char : str
        Character to use when joining multiple values (for list data)
    drop_source : bool
        Whether to drop the source column after extraction
    
    Returns:
    --------
    pandas.DataFrame
        The processed DataFrame
    """
    if field_name:
        # Handle single dictionary case
        def extract_single(x):
            if pd.isna(x) or x is None:
                return None
            if isinstance(x, dict):
                return x.get(field_name)
            return None
            
        # Handle list of dictionaries case
        def extract_list(x):
            if not isinstance(x, list) or not x:
                return None
            try:
                return join_char.join([item[field_name] for item in x if field_name in item])
            except (TypeError, KeyError):
                return None
                
        # Apply the appropriate function based on the data type
        df[target_column] = df[source_column].apply(
            lambda x: extract_list(x) if isinstance(x, list) else extract_single(x)
        )
    else:
        # If no field_name is provided, just copy the source column
        df[target_column] = df[source_column]
    
    # Drop the source column if requested
    if drop_source:
        df.drop(columns=[source_column], inplace=True)
    
    return df



def process_credits(df, source_column, target_column, operation_type, job_filter=None, drop_source=False):
    """
    Process movie credits data for various operations.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        The DataFrame containing the credits data
    source_column : str
        Name of the column containing the credits data (usually 'credits')
    target_column : str
        Name of the new column to create
    operation_type : str
        Type of operation to perform:
        - 'extract_names': Extract names from a credits section (cast or crew)
        - 'count_members': Count the number of members in a section
        - 'extract_by_job': Extract names of crew members with a specific job
    job_filter : str, optional
        Job title to filter by when operation_type is 'extract_by_job', e.g., 'Director'
    drop_source : bool, default False
        Whether to drop the source column after extraction
    
    Returns:
    --------
    pandas.DataFrame
        The processed DataFrame
    """
    def extract_names(credits_data, section):
        """Extract names from a section (cast or crew) as a pipe-separated string."""
        if section in credits_data and isinstance(credits_data[section], list):
            return '|'.join([member['name'] for member in credits_data[section]])
        return None
        
    def count_members(credits_data, section):
        """Count the number of members in a section."""
        if section in credits_data and isinstance(credits_data[section], list):
            return len(credits_data[section])
        return 0
        
    def extract_by_job(credits_data, job):
        """Extract names of crew members with a specific job."""
        if 'crew' in credits_data and isinstance(credits_data['crew'], list):
            matching_members = [member['name'] for member in credits_data['crew'] if member.get('job') == job]
            if matching_members:
                return '|'.join(matching_members)
        return None
    
    # Apply the appropriate operation
    if operation_type == 'extract_names':
        # Determine if we're working with cast or crew based on target column
        section = 'cast' if 'cast' in target_column.lower() else 'crew'
        df[target_column] = df[source_column].apply(lambda x: extract_names(x, section) if pd.notna(x) else None)
    
    elif operation_type == 'count_members':
        # Determine if we're counting cast or crew based on target column
        section = 'cast' if 'cast' in target_column.lower() else 'crew'
        df[target_column] = df[source_column].apply(lambda x: count_members(x, section) if pd.notna(x) else 0)
    
    elif operation_type == 'extract_by_job' and job_filter:
        df[target_column] = df[source_column].apply(lambda x: extract_by_job(x, job_filter) if pd.notna(x) else None)
    
    # Drop the source column if requested
    if drop_source:
        df.drop(columns=[source_column], inplace=True)
        
    return df


def inspect_column_frequencies(df, columns):
    for column in columns:
        print(f"{column} Frequencies:")
        print(df[column].value_counts())
        print("\n" + "-" * 50 + "\n")