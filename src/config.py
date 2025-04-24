"""
Configuration settings for the TMDB Movie Analysis project.
Contains API settings, file paths, and other constants.
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# API Configuration
API_ACCESS_TOKEN = os.getenv('API_ACCESS_TOKEN')
BASE_URL = "https://api.themoviedb.org/3/movie"

# Movie IDs to analyze (as specified in the project requirements)
MOVIE_IDS = [0, 299534, 19995, 140607, 299536, 597, 135397,
             420818, 24428, 168259, 99861, 284054, 12445,
             181808, 330457, 351286, 109445, 321612, 260513]

# Columns to drop during preprocessing
COLUMNS_TO_DROP = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']

# Final column order for the cleaned DataFrame
FINAL_COLUMN_ORDER = [
    'id', 'title', 'tagline', 'release_date', 'genre_names', 'collection_name',
    'original_language', 'budget_musd', 'revenue_musd', 'production_companies_names',
    'production_countries_names', 'vote_count', 'vote_average', 'popularity', 'runtime',
    'overview', 'spoken_languages_names', 'poster_path', 'cast', 'cast_size', 'director', 'crew_size'
]

# Paths for saving results
DATA_DIR = "../data/"
RESULTS_DIR = "../results/"
