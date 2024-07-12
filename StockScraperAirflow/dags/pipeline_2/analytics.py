import pandas as pd

relative_path_data = './dags/test_data/ml-100k/u.data'
relative_path_movie = './dags/test_data/ml-100k/u.item'
relative_path_user = './dags/test_data/ml-100k/u.user'

# Load datasets
u_data = pd.read_csv(relative_path_data, sep='\t', names=['user_id', 'movie_id', 'rating', 'timestamp'])
u_item = pd.read_csv(relative_path_movie, sep='|', names=['movie_id', 'movie_title', 'release_date', 'video_release_date',
                                                'IMDb_URL', 'unknown', 'Action', 'Adventure', 'Animation',
                                                'Children', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy',
                                                'Film-Noir', 'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi',
                                                'Thriller', 'War', 'Western'], encoding='latin-1')
u_user = pd.read_csv(relative_path_user, sep='|', names=['user_id', 'age', 'gender', 'occupation', 'zip_code'])

def find_mean_age_by_occupation(**kwargs):
    mean_age_by_occupation = u_user.groupby('occupation')['age'].mean().reset_index()
    print(mean_age_by_occupation)

def find_top_20_highest_rated_movies(min_ratings=35,**kwargs):
    movie_ratings = u_data.groupby('movie_id').agg({'rating': ['mean', 'count']})
    movie_ratings.columns = ['mean_rating', 'rating_count']
    popular_movies = movie_ratings[movie_ratings['rating_count'] >= min_ratings]
    top_20_movies = popular_movies.sort_values('mean_rating', ascending=False).head(20)
    top_20_movies = top_20_movies.merge(u_item[['movie_id', 'movie_title']], on='movie_id')
    top_20_movies = top_20_movies[['movie_title', 'mean_rating', 'rating_count']]
    print(top_20_movies)

def find_top_genres_by_occupation_and_age_group(**kwargs):
    age_bins = [0, 20, 25, 35, 45, 100]
    age_labels = ['<20', '20-25', '25-35', '35-45', '45+']
    u_user['age_group'] = pd.cut(u_user['age'], bins=age_bins, labels=age_labels, right=False)
    user_ratings = u_data.merge(u_user, on='user_id')
    movie_genres = u_item[['movie_id', 'Action', 'Adventure', 'Animation', 'Children', 'Comedy', 'Crime',
                            'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery',
                            'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western']]
    user_ratings = user_ratings.merge(movie_genres, on='movie_id')
    genre_cols = movie_genres.columns[1:]
    genre_ratings = user_ratings.groupby(['age_group', 'occupation'])[genre_cols].mean()
    print(genre_ratings)



def find_top_similar_movies(movie_id, threshold=0.60,
                            min_common_ratings=20, top_n=10,**kwargs):
    rating_matrix = u_data.pivot(index='user_id', columns='movie_id',
                                    values='rating')

    movie_ratings = rating_matrix[movie_id]

    similarity = rating_matrix.corrwith(movie_ratings)
    similarity = similarity.dropna()
    similarity = similarity[similarity >= threshold]
    common_ratings = rating_matrix.notna().astype(int).T.dot(
    rating_matrix.notna().astype(int))
    common_ratings = common_ratings.loc[movie_id]

    # Exclude the input movie from similar movies
    common_ratings[movie_id] = 0

    similar_movies = similarity[common_ratings >= min_common_ratings]

    # Sort by similarity and take top_n movies
    top_similar_movies = similar_movies.sort_values(ascending=False).head(
        top_n).reset_index()
    top_similar_movie_ids = top_similar_movies['movie_id'].tolist()
    top_similar_movie_titles = \
    u_item[u_item['movie_id'].isin(top_similar_movie_ids)][
        'movie_title'].tolist()

    # Calculate strength (number of common ratings) for each similar movie
    strengths = []
    for movie in top_similar_movie_ids:
        common_ratings = rating_matrix[movie_id] * rating_matrix[movie]
        common_ratings = common_ratings[common_ratings > 0]
        strength = len(common_ratings)
        strengths.append(strength)

    # Create DataFrame with movie titles, similarities, and strengths
    top_similar_movies = pd.DataFrame({
        'movie_title': top_similar_movie_titles,
        'similarity': top_similar_movies[0],
        'strength': strengths
    })

    print(top_similar_movies)