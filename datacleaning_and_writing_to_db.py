import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv
from scraping import scraping_data

load_dotenv()


class MovieData:
    def __init__(self, file_name="data.csv"):
        self.df = pd.read_csv(file_name).drop_duplicates()
        self.df["genre_ids"] = self.df["genre_ids"].apply(lambda x: x.strip("[]"))

    def movies(self):
        return self.df[
            [
                "adult",
                "popularity",
                "id",
                "release_date",
                "title",
                "video",
                "vote_average",
                "vote_count",
            ]
        ]

    def backdrop(self):
        return self.df[["id", "backdrop_path"]]


class Database:
    def __init__(self, data):
        self.user = os.getenv("db_user")
        self.password = os.getenv("db_password")
        self.host = os.getenv("db_host")
        self.database = os.getenv("db_database")
        self.port = os.getenv("db_port")
        self.data = data

        try:
            self.conn = psycopg2.connect(
                f"host={self.host} dbname={self.database} user={self.user} password={self.password}"
            )
            print("connected to database")
        except psycopg2.OperationalError as e:
            print(f"Error connecting to the database: {e}")

            # create engine for sqlalchemy
        self.engine = create_engine(
            f"postgresql://{self.user}:{self.password}@{self.host}:{int(self.port)}/{self.database}"
        )

    def create_movie_fact_table(self):
        with self.conn.cursor() as cursor:
            drop_table_query = "DROP TABLE IF EXISTS movie_fact CASCADE"
            cursor.execute(drop_table_query)

            create_table_query = """
            CREATE TABLE movie_fact(
                movie_id SERIAL PRIMARY KEY ,
                adult BOOLEAN,
                popularity float,
                id INT UNIQUE,
                release_date DATE,
                title VARCHAR,
                video BOOLEAN,
                vote_average FLOAT,
                vote_count INT
            );
            """
            cursor.execute(create_table_query)
            self.conn.commit()

    def create_backdrop_table(self):
        with self.conn.cursor() as cursor:
            drop_table_query = "DROP TABLE IF EXISTS backdrop"
            cursor.execute(drop_table_query)

            create_table_query = """
            CREATE TABLE backdrop(
                backdrop_id SERIAL PRIMARY KEY,
                id INT,
                FOREIGN KEY (id) REFERENCES movie_fact(id),
                backdrop_path VARCHAR
            );
            """

            cursor.execute(create_table_query)
            self.conn.commit()

    def insert_movies_data(self):
        movies_df = self.data.movies()
        movies_df.to_sql("movie_fact", self.engine, if_exists="append", index=False)

    def insert_backdrop_table(self):
        backdrop_df = self.data.backdrop()
        backdrop_df.to_sql("backdrop", self.engine, if_exists="append", index=False)


def main():
    # read data from csv file
    max_pages = 5
    current_page = 1
    scraping_data(max_pages, current_page)
    movie_data = MovieData()
    db = Database(movie_data)

    db.create_movie_fact_table()
    print("movie fact table created")
    db.create_backdrop_table()
    print("backdrop table created")

    try:
        db.insert_movies_data()
    except Exception as e:
        print(f"Error inserting data into database: {e}")

    try:
        db.insert_backdrop_table()
    except Exception as e:
        print(f" Error inserting backdrop table into database: {e}")


if __name__ == "__main__":
    main()

