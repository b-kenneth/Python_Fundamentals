# individual_graphs.py

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

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

