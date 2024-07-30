from abc import ABC, abstractmethod
from flask_socketio import SocketIO
from flask import Flask, jsonify, request
from flask_cors import CORS
from redis import Redis
from rq import Queue, job as Job
import psycopg2
import os
from script import import_csv_with_copy, update_task_status

app = Flask(__name__)
CORS(app)  # Adiciona suporte a CORS
socketio = SocketIO(app, cors_allowed_origins="*")        

# Connect to Redis server
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
conn = Redis.from_url(redis_url)
q = Queue(connection=conn)

@app.route("/convert", methods=['POST'])
def convert():
    data = request.get_json()
    if not data:
        return jsonify({
            'status': 'error',
            'message': 'Invalid request body'
        }), 400

    name = data.get('name')
    table = data.get('table')
    

    if not name or not table:
        return jsonify({
            'status': 'error',
            'message': 'Both "name" and "table" fields are required'
        }), 400

    path = os.path.join('../file/chunks', name)
    
    if not os.path.exists(path):
        return jsonify({
            'status': 'error',
            'message': f'File {name} does not exist.'
        }), 400

    # Enqueue the task
    job = q.enqueue(import_csv_with_copy, path, name, table)
    
    # Save initial task status
    update_task_status(job_id=job.get_id(), status='queued', name=name, table_name=table)

    return jsonify({
        'status': 'success',
        'message': 'File is being processed',
        'job_id': job.get_id()
    }), 200

@app.route("/tasks/<job_id>", methods=['GET'])
def get_task(job_id):
    conn = psycopg2.connect("dbname=movielens user=postgres password=postgres host=localhost port=5432")
    cursor = conn.cursor()
    cursor.execute("SELECT job_id, name, table_name, status, result, created_at, updated_at, completed_at, processing_time, rows_inserted, rows_failed FROM tasks WHERE job_id = %s;", (job_id,))

    task = cursor.fetchone()
    conn.close()

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
        'processing_time': task[8].total_seconds() if task[7] else None,  # Convert timedelta to seconds
        'rows_inserted': task[9],
        'rows_failed': task[10]
    }

    return jsonify({
        'task': task_dict
    })

@app.route("/tasks", methods=['GET'])
def list_tasks():
    conn = psycopg2.connect("dbname=movielens user=postgres password=postgres host=localhost port=5432")
    cursor = conn.cursor()
    cursor.execute("SELECT job_id, name, table_name, status, result, created_at, updated_at, completed_at, processing_time, rows_inserted, rows_failed FROM tasks ORDER BY created_at DESC;")   
    rows = cursor.fetchall()
    conn.close()

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
            'processing_time': row[8].total_seconds() if row[7] else None,  # Convert timedelta to seconds
            'rows_inserted': row[9],
            'rows_failed': row[10]
        }
        tasks.append(task)

    return jsonify({
        'count': len(tasks),
        'results': tasks
    })
    
@socketio.on('connect')
def handle_connect():
    print('Client connected')

@socketio.on('disconnect')
def handle_disconnect():
    print('Client disconnected')


if __name__ == "__main__":
    app.run(debug=True)
