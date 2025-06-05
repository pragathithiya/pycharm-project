import pandas as pd
from sqlalchemy import create_engine, text
import random

# Database connection
# DB_URL = 'postgresql://postgres:your_password@localhost:5432/your_database'
DB_URL = 'postgresql://postgres:praga%401825%23@localhost:5432/mydb'

def create_movie_data():
    """Create sample movie dataset"""
    genres = ['Action', 'Comedy', 'Drama', 'Horror', 'Romance', 'Sci-Fi', 'Thriller']
    ratings = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    movies = []
    for i in range(1, 8807):
        movies.append({
            'id': i,
            'title': f'Movie {i}',
            'genre': random.choice(genres),
            'year': random.randint(2000, 2024),
            'rating': random.choice(ratings),
            'director': f'Director {random.randint(1, 20)}'
        })

    df = pd.DataFrame(movies)
    print(f"✓ Created {len(df)} movies")
    return df


def ingest_to_postgres(df):
    """Upload data to PostgreSQL"""
    try:
        engine = create_engine(DB_URL)
        df.to_sql('movies', engine, if_exists='replace', index=False)
        print("✓ Data uploaded to PostgreSQL successfully!")
        return engine
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


def search_movies_by_genre(engine, genre):
    """Search movies by genre"""
    query = f"SELECT * FROM movies WHERE genre = '{genre}'"

    with engine.connect() as conn:
        result = conn.execute(text(query))
        movies = result.fetchall()

    print(f"\n🎬 Movies in '{genre}' genre:")
    print("-" * 50)

    if movies:
        for movie in movies:
            print(f"• {movie[1]} ({movie[3]}) - Rating: {movie[4]}/10")
        print(f"\n📊 Total {genre} movies: {len(movies)}")
    else:
        print(f"No {genre} movies found")

    return movies


def get_rating_counts(engine, genre):
    """Get count of each rating for a genre"""
    query = f"""
    SELECT rating, COUNT(*) as count 
    FROM movies 
    WHERE genre = '{genre}' 
    GROUP BY rating 
    ORDER BY rating
    """

    with engine.connect() as conn:
        result = conn.execute(text(query))
        rating_counts = result.fetchall()

    print(f"\n📈 Rating distribution for '{genre}' movies:")
    print("-" * 30)

    total_movies = 0
    for rating, count in rating_counts:
        print(f"Rating {rating}: {count} movies")
        total_movies += count

    print(f"\nTotal: {total_movies} movies")
    return rating_counts


def main():
    print("🎬 MOVIE DATABASE APPLICATION")
    print("=" * 40)

    # Step 1: Create movie data
    print("\n1. Creating movie data...")
    df = create_movie_data()

    # Step 2: Upload to PostgreSQL
    print("\n2. Uploading to PostgreSQL...")
    engine = ingest_to_postgres(df)

    if not engine:
        print("❌ Failed to connect to database. Check your credentials!")
        return

    # Step 3: Search by genre
    print("\n3. Searching movies...")

    # Show available genres
    with engine.connect() as conn:
        result = conn.execute(text("SELECT DISTINCT genre FROM movies"))
        genres = [row[0] for row in result.fetchall()]

    print(f"\nAvailable genres: {', '.join(genres)}")

    # Demo searches
    search_genre = 'Action'  # Change this to search different genres
    movies = search_movies_by_genre(engine, search_genre)

    # Step 4: Get rating counts
    if movies:
        get_rating_counts(engine, search_genre)

    # Interactive search
    print("\n" + "=" * 40)
    while True:
        user_genre = input(f"\nEnter genre to search (or 'quit' to exit): ").strip()

        if user_genre.lower() == 'quit':
            break

        if user_genre in genres:
            search_movies_by_genre(engine, user_genre)
            get_rating_counts(engine, user_genre)
        else:
            print(f"Genre '{user_genre}' not found. Available: {', '.join(genres)}")

    print("\n✅ Application completed!")

if __name__ == "__main__":
    main()