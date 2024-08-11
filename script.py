import logging
import os
import psycopg2
import time


def update_task_status(job_id, status, name=None, table_name=None, result=None, processing_time=None, rows_inserted=None, rows_failed=None):
    conn = psycopg2.connect("dbname=movielens user=postgres password=postgres host=localhost port=5432")
    cursor = conn.cursor()

    if status == 'completed':
        completed_at = 'NOW()'
    else:
        completed_at = 'NULL'

    try:
        cursor.execute("""
            INSERT INTO tasks (job_id, name, table_name, status, result, updated_at, completed_at, processing_time, rows_inserted, rows_failed) 
            VALUES (%s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s)
            ON CONFLICT (job_id) 
            DO UPDATE SET status = EXCLUDED.status, result = EXCLUDED.result, updated_at = EXCLUDED.updated_at, completed_at = EXCLUDED.completed_at, processing_time = EXCLUDED.processing_time, rows_inserted = EXCLUDED.rows_inserted, rows_failed = EXCLUDED.rows_failed;
        """, (
            job_id, name, table_name, status, result, 
            completed_at if completed_at == 'NOW()' else None, 
            f'{processing_time} seconds' if processing_time else None,
            rows_inserted, rows_failed
        ))
        conn.commit()
        logging.info(f"Task status updated: {job_id} - {status}")
    except psycopg2.DatabaseError as e:
        logging.error(f"Database error while updating task status: {e}")
        conn.rollback()
    except Exception as e:
        logging.error(f"Unexpected error while updating task status: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def recreate_materialized_view():
    conn = psycopg2.connect("dbname=movielens user=postgres password=postgres host=localhost port=5432")
    cursor = conn.cursor()

    try:
        cursor.execute("""
            DROP MATERIALIZED VIEW IF EXISTS mv_movie_ratings;
            CREATE MATERIALIZED VIEW mv_movie_ratings AS
            SELECT mv.movieId, mv.title, mv.genres, AVG(rt.rating) AS avg_rating, COUNT(*) AS rating_count, lk.imdbid, lk.tmdbid
            FROM movies mv
            INNER JOIN ratings rt ON mv.movieId = rt.movieId
            INNER JOIN links lk ON mv.movieId = lk.movieId
            GROUP BY mv.movieId, lk.imdbid, lk.tmdbid;
        """)
        
        conn.commit()
        logging.info("Materialized view mv_movie_ratings recreated successfully.")
    except Exception as e:
        logging.error(f"Error recreating materialized view: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def import_csv_with_copy(file_path, file_name, table_name):
    logging.info(f"Importing {file_name} to {table_name} table...")
    job_id = os.getenv('RQ_JOB_ID')
    update_task_status(job_id, status='started', name=file_name, table_name=table_name)

    start_time = time.time()

    rows_inserted = 0
    rows_failed = 0

    try:
        conn = psycopg2.connect("dbname=movielens user=postgres password=postgres host=localhost port=5432")
        cursor = conn.cursor()

        temp_table_name = f"{table_name}_temp"
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name};")

        if table_name == 'ratings':
            cursor.execute(f"""
                CREATE TABLE {temp_table_name} (
                    userId INT,
                    movieId INT,
                    rating FLOAT CHECK (rating >= 0.5 AND rating <= 5.0),
                    timestamp BIGINT
                );
            """)
        elif table_name == 'tags':
            cursor.execute(f"""
                CREATE TABLE {temp_table_name} (
                    userId INT,
                    movieId INT,
                    tag TEXT,
                    timestamp BIGINT
                );
            """)
        elif table_name == 'movies':
            cursor.execute(f"""
                CREATE TABLE {temp_table_name} (
                    movieId INT PRIMARY KEY,
                    title TEXT,
                    genres TEXT
                );
            """)
        elif table_name == 'links':
            cursor.execute(f"""
                CREATE TABLE {temp_table_name} (
                    movieId INT PRIMARY KEY,
                    imdbId INT,
                    tmdbId INT
                );
            """)
        elif table_name == 'genomescores':
            cursor.execute(f"""
                CREATE TABLE {temp_table_name} (
                    movieId INT,
                    tagId INT,
                    relevance FLOAT
                );
            """)
        elif table_name == 'genometags':
            cursor.execute(f"""
                CREATE TABLE {temp_table_name} (
                    tagId INT PRIMARY KEY,
                    tag TEXT
                );
            """)

        logging.info(f"Copying data from {file_path} to {temp_table_name}...")
        with open(file_path, 'r') as f:
            cursor.copy_expert(f"COPY {temp_table_name} FROM stdin WITH CSV HEADER", f)
        
        cursor.execute(f'SELECT COUNT(*) FROM {temp_table_name};')
        rows_inserted = cursor.fetchone()[0]

        if table_name in ['ratings', 'tags']:
            cursor.execute(f"""
                ALTER TABLE {temp_table_name} 
                ALTER COLUMN timestamp TYPE TIMESTAMP USING to_timestamp(timestamp);
            """)
            
        cursor.execute(f'DROP TABLE IF EXISTS {table_name} CASCADE;')
        cursor.execute(f'ALTER TABLE {temp_table_name} RENAME TO {table_name};')

        conn.commit()
        logging.info(f"Import completed for {file_name}. Rows inserted: {rows_inserted}")

    except Exception as e:
        end_time = time.time()
        processing_time = end_time - start_time

        logging.error(f"Error importing {file_name} to {table_name} table: {str(e)}")
        update_task_status(job_id, status='failed', name=file_name, table_name=table_name, result=str(e), processing_time=processing_time, rows_inserted=rows_inserted, rows_failed=rows_failed)

        return False
    finally:
        cursor.close()
        conn.close()

        os.remove(file_path)
        end_time = time.time()
        processing_time = end_time - start_time
        update_task_status(job_id, status='completed', name=file_name, table_name=table_name, processing_time=processing_time, rows_inserted=rows_inserted, rows_failed=rows_failed)

    # Recreate the materialized view after importing
    recreate_materialized_view()
    
    return True
