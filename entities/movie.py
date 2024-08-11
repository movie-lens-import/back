from abc import ABC, abstractmethod
import logging

from flask import jsonify
import psycopg2

from db import IDatabase


class IMovies(ABC):
    @abstractmethod
    def get_all_movies(self, limit: int = 30, offset: int = 0):
        pass
    
class Movies(IMovies):
    def __init__(self, db: IDatabase):
        self.db = db

    def get_all_movies(self, limit: int = 30, offset: int = 0, year=None, genre=None, rating=None, ratings_count=None):
        query_filters = []
        query_params = []

        if year:
            query_filters.append("title ILIKE %s")
            query_params.append(f'%({year})%')
        
        if genre:
            query_filters.append("genres ILIKE %s")
            query_params.append(f'%{genre}%')
        
        if rating:
            query_filters.append("avg_rating >= %s")
            query_params.append(rating)
        
        if ratings_count:
            query_filters.append("rating_count >= %s")
            query_params.append(ratings_count)

        where_clause = "WHERE " + " AND ".join(query_filters) if query_filters else ""
        
        
        query_total_count = f"SELECT COUNT(*) FROM mv_movie_ratings {where_clause};"
        query_movies = f"""
            SELECT *
            FROM mv_movie_ratings
            {where_clause}
            ORDER BY movieid
            LIMIT %s
            OFFSET %s;
        """
        

        query_params.extend([limit, offset])
        
        print(query_params[:-2])
        
        try:
            with self.db.connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query_total_count, query_params[:-2]) 
                    total_count = cursor.fetchone()[0]
                    
                    cursor.execute(query_movies, query_params)
                    rows = cursor.fetchall()

                    movies = []
                    for row in rows:
                        movie = {
                            'movieid': row[0],
                            'title': row[1],
                            'genres': row[2],
                            'average_rating': row[3],
                            'ratings_count': row[4],
                            'imdbid': row[5],
                            'tmdbid': row[6]
                        }
                        movies.append(movie)

                    next_offset = offset + limit
                    previous_offset = max(0, offset - limit)
                    
                    return jsonify({
                        'count': total_count,
                        'next': f'/movies?limit={limit}&offset={next_offset}' if next_offset < total_count else None,
                        'previous': f'/movies?limit={limit}&offset={previous_offset}' if offset > 0 else None,
                        'results': movies
                    })
                    
        except psycopg2.DatabaseError as e:
            logging.error(f'Database error: {str(e)}')
            return jsonify({
                'status': 'database-error',
                'message': f'Database error: {str(e)}'
            }), 500
        except Exception as e:
            logging.error(f'Unexpected error: {str(e)}')
            return jsonify({
                'status': 'error',
                'message': f'Unexpected error: {str(e)}'
            }), 500