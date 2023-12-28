import psycopg2
import logging


class DatabaseConnection:
    def __init__(self):
        """DatabaseConnection class represents a connection to a PostgreSQL database."""
        self.database = "gnznsgob"
        self.user = "gnznsgob"
        self.password = "6QLrzwymH4CSerp3E1M_6auaFP-Vp5ub"
        self.host = "mahmud.db.elephantsql.com"
        self.port = "5432"

    async def connect(self):
        """Connects to the PostgreSQL database and returns the connection object."""
        try:
            self.conn = psycopg2.connect(
                database=self.database,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
            )
            logging.info("Connection Successful to PostgreSQL DB")
            self.cur = self.conn.cursor()
        except Exception as e:
            raise Exception(
                f"Some error occurred while connecting to PostgreSQL: {str(e)}"
            )

    def fetchall_query(self, query, params):
        """Executes the given query with the provided parameters and returns the result."""
        try:
            self.cur.execute(query, params)
            result = self.cur.fetchall()
            return result
        except Exception as e:
            raise Exception(f"Some error occurred while executing query: {str(e)}")
        
    def fetchone_query(self, query, params):
        """Executes the given query with the provided parameters and returns the result."""
        try:
            self.cur.execute(query, params)
            result = self.cur.fetchone()
            return result
        except Exception as e:
            raise Exception(f"Some error occurred while executing query: {str(e)}")
    
    def execute_query(self, query, params):
        """Executes the given query with the provided parameters and returns the result."""
        try:
            self.cur.execute(query, params)
            self.conn.commit()
        except Exception as e:
            raise Exception(f"Some error occurred while executing query: {str(e)}")
    
    def execute_query_with_return(self, query, params):
        """Executes the given query with the provided parameters and returns the result."""
        try:
            self.cur.execute(query, params)
            self.conn.commit()
            return self.cur.fetchone()
        except Exception as e:
            raise Exception(f"Some error occurred while executing query: {str(e)}")
        
    def close(self):
        """Closes the connection to the PostgreSQL database."""
        try:
            self.conn.close()
            logging.info("Connection Closed to PostgreSQL")
        except Exception as e:
            raise Exception(f"Some error occurred while closing connection: {str(e)}")
