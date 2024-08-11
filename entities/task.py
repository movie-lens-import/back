from abc import ABC, abstractmethod
from flask import jsonify
import psycopg2
from db import IDatabase

class ITasks(ABC):
    @abstractmethod
    def get_all_tasks(self):
        pass
    
    @abstractmethod
    def get_task_by_id(self, task_id: str):
        pass

class Tasks(ITasks):
    def __init__(self, db: IDatabase):
        self.db = db
        
    def get_all_tasks(self):
        query = """
            SELECT job_id, name, table_name, status, result, created_at, updated_at, completed_at, processing_time, rows_inserted, rows_failed 
            FROM tasks 
            ORDER BY created_at DESC;
        """
        try:
            with self.db.connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    
                    tasks = []
                    for row in rows:
                        task = {
                            'job_id': row[0],
                            'name': row[1],
                            'table_name': row[2],
                            'status': row[3],
                            'result': row[4],
                            'created_at': row[5],
                            'updated_at': row[6],
                            'completed_at': row[7] if row[7] else None,
                            'processing_time': row[8].total_seconds() if row[7] else None,
                            'rows_inserted': row[9],
                            'rows_failed': row[10]
                        }
                        tasks.append(task)

                    return jsonify({
                        'count': len(tasks),
                        'results': tasks
                    })
                    
        except psycopg2.DatabaseError as e:
            return jsonify({
                'status': 'error',
                'message': f'Database error: {str(e)}'
            }), 500
        except Exception as e:
            return jsonify({
                'status': 'error',
                'message': f'Unexpected error: {str(e)}'
            }), 500
        
    def get_task_by_id(self, task_id: str):
        query = """
            SELECT job_id, name, table_name, status, result, created_at, updated_at, completed_at, processing_time, rows_inserted, rows_failed 
            FROM tasks 
            WHERE job_id = %s;
        """
        try:
            with self.db.connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (task_id,))
                    task = cursor.fetchone()
                    
                    if not task:
                        return jsonify({
                            'status': 'error',
                            'message': 'Task not found'
                        }), 404

                    task_dict = {
                        'job_id': task[0],
                        'name': task[1],
                        'table_name': task[2],
                        'status': task[3],
                        'result': task[4],
                        'created_at': task[5],
                        'updated_at': task[6],
                        'completed_at': task[7] if task[7] else None,
                        'processing_time': task[8].total_seconds() if task[7] else None,
                        'rows_inserted': task[9],
                        'rows_failed': task[10]
                    }
                    
                    return jsonify({
                        'task': task_dict
                    })
                    
        except psycopg2.DatabaseError as e:
            return jsonify({
                'status': 'error',
                'message': f'Database error: {str(e)}'
            }), 500
        except Exception as e:
            return jsonify({
                'status': 'error',
                'message': f'Unexpected error: {str(e)}'
            }), 500