import psycopg2
import psycopg2.extras

from config import config


class CustomDataRepository:
    @staticmethod
    def _run_select_query(sql: str, bindings: tuple = None):
        with psycopg2.connect(**config['custom_data']) as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute(sql, bindings)
            return cursor.fetchall()

    @staticmethod
    def _run_query(sql: str, bindings: tuple = None):
        with psycopg2.connect(**config['custom_data']) as conn:
            cursor = conn.cursor()
            cursor.execute(sql, bindings)

