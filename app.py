from flask_socketio import SocketIO
from flask import Flask, jsonify, request
from flask_cors import CORS
from redis import Redis
from rq import Queue
import os
import logging
from db import Database
from entities.task import Tasks
from entities.movie import Movies
from entities.enqueuer import Enqueuer

# Configure logging
logging.basicConfig(level=logging.DEBUG)

app = Flask(__name__)
app.debug = True
CORS(app)  # Add CORS support
socketio = SocketIO(app, cors_allowed_origins="*")

db = Database()
tasks = Tasks(db)
movies = Movies(db)

# Connect to Redis server
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
conn = Redis.from_url(redis_url)
queue = Queue(connection=conn)

# Initialize  instance
enqueuer = Enqueuer(queue, db)


@app.route("/convert", methods=['POST'])
def convert():
    data = request.get_json()
    validation_error = enqueuer.validate(data)
    if validation_error:
        logging.error(f"Validation error: {validation_error}")
        return jsonify({
            'status': 'error',
            'message': validation_error
        }), 400

    name = data['name']
    table = data['table']
    path = os.path.join('../file/chunks', name)

    try:
        job_id = enqueuer.enqueue(path, name, table)
        logging.info(f"Task {job_id} enqueued for file {name}")

        return jsonify({
            'status': 'success',
            'message': 'File is being processed',
            'job_id': job_id
        }), 200

    except Exception as e:
        logging.error(f"Error enqueuing task: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Error processing file: {str(e)}'
        }), 500

@app.route("/tasks", methods=['GET'])
def list_tasks():
    return tasks.get_all_tasks()

@app.route("/tasks/<job_id>", methods=['GET'])
def get_task_by_id(job_id):
    return tasks.get_task_by_id(job_id)


@app.route("/movies", methods=['GET'])
def list_movies():
    limit = request.args.get('limit', 30, type=int)
    offset = request.args.get('offset', 0, type=int)
    year = request.args.get('year', None, type=int)
    genre = request.args.get('genre', None, type=str)
    rating = request.args.get('rating', None, type=float)
    ratings_count = request.args.get('ratings_count', None, type=int)
    return movies.get_all_movies(limit, offset, year, genre, rating, ratings_count)

@socketio.on('connect')
def handle_connect():
    logging.info('Client connected')

@socketio.on('disconnect')
def handle_disconnect():
    logging.info('Client disconnected')

if __name__ == "__main__":
    app.run(debug=True)
