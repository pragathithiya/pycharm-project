import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(_name_)


class PostgreSQLDataIngestion:
    def _init_(self, host='localhost', port=5432, database='your_database',
                 username='your_username', password='your_password'):
        """
        Initialize PostgreSQL connection
        """
        self.connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database}'
        self.engine = None

    def connect(self):
        """Create database connection"""
        try:
            self.engine = create_engine(self.connection_string)
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Successfully connected to PostgreSQL database")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Failed to connect to database: {e}")
            return False

    def ingest_csv_to_postgres(self, csv_file_path, table_name, if_exists='replace'):
        """
        Ingest CSV data into PostgreSQL table

        Args:
            csv_file_path (str): Path to CSV file
            table_name (str): Name of the target table
            if_exists (str): 'fail', 'replace', or 'append'
        """
        try:
            # Read CSV file
            logger.info(f"Reading CSV file: {csv_file_path}")
            df = pd.read_csv(csv_file_path)

            # Display basic info about the dataset
            logger.info(f"Dataset shape: {df.shape}")
            logger.info(f"Columns: {list(df.columns)}")

            # Clean data (optional)
            df = self.clean_data(df)

            # Insert data into PostgreSQL
            logger.info(f"Inserting data into table: {table_name}")
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists=if_exists,
                index=False,
                method='multi',
                chunksize=1000
            )

            logger.info(f"Successfully inserted {len(df)} rows into {table_name}")
            return True

        except Exception as e:
            logger.error(f"Error during data ingestion: {e}")
            return False

    def ingest_dataframe_to_postgres(self, df, table_name, if_exists='replace'):
        """
        Ingest pandas DataFrame into PostgreSQL table

        Args:
            df (pandas.DataFrame): DataFrame to insert
            table_name (str): Name of the target table
            if_exists (str): 'fail', 'replace', or 'append'
        """
        try:
            logger.info(f"Inserting DataFrame into table: {table_name}")
            logger.info(f"DataFrame shape: {df.shape}")

            # Clean data
            df = self.clean_data(df)

            # Insert data
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists=if_exists,
                index=False,
                method='multi',
                chunksize=1000
            )

            logger.info(f"Successfully inserted {len(df)} rows into {table_name}")
            return True

        except Exception as e:
            logger.error(f"Error during DataFrame ingestion: {e}")
            return False

    def clean_data(self, df):
        """
        Basic data cleaning operations
        """
        # Remove completely empty rows
        df = df.dropna(how='all')

        # Clean column names (remove spaces, special characters)
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('[^\w]', '', regex=True)

        # Handle missing values (you can customize this)
        # df = df.fillna('')  # Fill with empty string
        # df = df.dropna()    # Drop rows with missing values

        return df

    def execute_query(self, query):
        """Execute a SQL query and return results"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                return result.fetchall()
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return None

    def get_table_info(self, table_name):
        """Get information about a table"""
        query = f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position;
        """
        return self.execute_query(query)

    def close_connection(self):
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")


# Usage Example
def main():
    # Database configuration
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'your_database_name',
        'username': 'your_username',
        'password': 'your_password'
    }

    # Initialize ingestion class
    ingestion = PostgreSQLDataIngestion(**db_config)

    # Connect to database
    if not ingestion.connect():
        return

    try:
        # Method 1: Ingest from CSV file
        csv_file_path = 'path/to/your/dataset.csv'
        table_name = 'your_table_name'

        success = ingestion.ingest_csv_to_postgres(
            csv_file_path=csv_file_path,
            table_name=table_name,
            if_exists='replace'  # 'replace', 'append', or 'fail'
        )

        if success:
            # Verify the data was inserted
            query_result = ingestion.execute_query(f"SELECT COUNT(*) FROM {table_name}")
            print(f"Total rows in {table_name}: {query_result[0][0]}")

            # Get table structure
            table_info = ingestion.get_table_info(table_name)
            print(f"Table structure for {table_name}:")
            for column in table_info:
                print(f"  {column[0]} ({column[1]}) - Nullable: {column[2]}")

        # Method 2: Create sample DataFrame and ingest
        sample_data = {
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
            'age': [25, 30, 35, 28, 32],
            'city': ['New York', 'London', 'Paris', 'Tokyo', 'Sydney']
        }

        df = pd.DataFrame(sample_data)

        success = ingestion.ingest_dataframe_to_postgres(
            df=df,
            table_name='sample_users',
            if_exists='replace'
        )

        if success:
            # Query the data
            results = ingestion.execute_query("SELECT * FROM sample_users LIMIT 5")
            print("\nSample data from sample_users table:")
            for row in results:
                print(row)

    except Exception as e:
        logger.error(f"Error in main execution: {e}")

    finally:
        # Close connection
        ingestion.close_connection()


if _name_ == "_main_":
    main()