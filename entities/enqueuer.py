from abc import ABC, abstractmethod
import os
from rq import Queue
from db import IDatabase
from script import import_csv_with_copy, update_task_status

class IEnqueuer(ABC):
    @abstractmethod
    def validate(self, data: dict):
        pass
    
    @abstractmethod
    def enqueue(self, path: str, name: str, table: str):
        pass
    
class Enqueuer(IEnqueuer):
    def __init__(self, queue: Queue, db: IDatabase):
        self.queue = queue

    def validate(self, data: dict) -> str:
        if not data:
            return 'Invalid request body'
        
        name = data.get('name')
        table = data.get('table')

        if not name or not table:
            return 'Both "name" and "table" fields are required'

        path = os.path.join('../file/chunks', name)
        if not os.path.exists(path):
            return f'File {name} does not exist.'

        return None

    def enqueue(self, path: str, name: str, table: str) -> str:
        job = self.queue.enqueue(import_csv_with_copy, path, name, table)
        update_task_status(job_id=job.get_id(), status='queued', name=name, table_name=table)
        return job.get_id()
    
    def update_task_status(self, job_id, status, name=None, table_name=None, result=None, processing_time=None, rows_inserted=None, rows_failed=None):
        conn = self.db.connect()
        cur = conn.cursor()

        if status == 'completed':
            completed_at = 'NOW()'
        else:
            completed_at = 'NULL'
        
        cur.execute("""
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
        cur.close()
        conn.close()
    