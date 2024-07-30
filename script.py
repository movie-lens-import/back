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
    cursor.close()
    conn.close()


def import_csv_with_copy(file_path, file_name, table_name):
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

        with open(file_path, 'r') as f:
            cursor.copy_expert(f"COPY {temp_table_name} FROM stdin WITH CSV HEADER", f)
        
        cursor.execute(f'SELECT COUNT(*) FROM {temp_table_name};')
        rows_inserted = cursor.fetchone()[0]

        if table_name == 'ratings':
            cursor.execute(f"""
                ALTER TABLE {temp_table_name} 
                ALTER COLUMN timestamp TYPE TIMESTAMP USING to_timestamp(timestamp);
            """)
        elif table_name == 'tags':
            cursor.execute(f"""
                ALTER TABLE {temp_table_name} 
                ALTER COLUMN timestamp TYPE TIMESTAMP USING to_timestamp(timestamp);
            """)

        cursor.execute(f'DROP TABLE IF EXISTS {table_name};')
        cursor.execute(f'ALTER TABLE {temp_table_name} RENAME TO {table_name};')

        conn.commit()
        cursor.close()
        conn.close()

        os.remove(file_path)
        end_time = time.time()
        processing_time = end_time - start_time

        update_task_status(job_id, status='completed', name=file_name, table_name=table_name, processing_time=processing_time, rows_inserted=rows_inserted, rows_failed=rows_failed)
        return True

    except Exception as e:
        end_time = time.time()
        processing_time = end_time - start_time

        update_task_status(job_id, status='failed', name=file_name, table_name=table_name, result=str(e), processing_time=processing_time, rows_inserted=rows_inserted, rows_failed=rows_failed)

        return False