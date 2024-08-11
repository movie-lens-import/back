
from abc import ABC, abstractmethod
import os

import psycopg2


class IDatabase(ABC):
    @abstractmethod
    def connect(self):
        pass

class Database(IDatabase):
    def __init__(self):
        self.db_params = {
            "dbname": os.getenv("DB_NAME", "movielens"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", "postgres"),
            "host": os.getenv("DB_HOST", "localhost"),
            "port": os.getenv("DB_PORT", "5432")
        }
        
    def connect(self):
        return psycopg2.connect(**self.db_params)