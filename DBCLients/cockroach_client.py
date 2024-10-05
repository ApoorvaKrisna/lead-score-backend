import psycopg2
from psycopg2 import sql, OperationalError

class CockroachDBClient:
    _pool = None

    def __init__(self):
        if CockroachDBClient._pool is None:
            CockroachDBClient._pool = self.create_pool('host', 'database', 'user', 'password', 10)

    @classmethod
    def create_pool(cls, host, database, user, password, pool_size):
        """Establish a pool of connections to the CockroachDB."""
        connections = []
        try:
            for _ in range(pool_size):
                conn = psycopg2.connect(
                    host=host,
                    database=database,
                    user=user,
                    password=password
                )
                connections.append(conn)
            return connections
        except OperationalError as e:
            return f"Error creating connection pool: {e}"

    def get_connection(self):
        """Get a connection from the pool."""
        if CockroachDBClient._pool:
            if CockroachDBClient._pool:
                return CockroachDBClient._pool.pop()
            else:
                return None  # No available connections
        return None

    def release_connection(self, conn):
        """Release a connection back to the pool."""
        CockroachDBClient._pool.append(conn)

    def create_table(self, table_name):
        conn = self.get_connection()
        if conn:
            try:
                cursor = conn.cursor()
                create_table_query = sql.SQL(
                    "CREATE TABLE IF NOT EXISTS {} (id SERIAL PRIMARY KEY, name TEXT, age INT)"
                ).format(sql.Identifier(table_name))
                cursor.execute(create_table_query)
                conn.commit()
                return f"Table '{table_name}' created."
            except OperationalError as e:
                return f"Error creating table: {e}"
            finally:
                cursor.close()
                self.release_connection(conn)
        return "No available connections."

    def insert_record(self, table_name, name, age):
        conn = self.get_connection()
        if conn:
            try:
                cursor = conn.cursor()
                insert_query = sql.SQL("INSERT INTO {} (name, age) VALUES (%s, %s)").format(sql.Identifier(table_name))
                cursor.execute(insert_query, (name, age))
                conn.commit()
                return "Record inserted."
            except OperationalError as e:
                return f"Error inserting record: {e}"
            finally:
                cursor.close()
                self.release_connection(conn)
        return "No available connections."

    def fetch_records(self, table_name):
        conn = self.get_connection()
        records = []
        if conn:
            try:
                cursor = conn.cursor()
                fetch_query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))
                cursor.execute(fetch_query)
                records = cursor.fetchall()
                return records
            except OperationalError as e:
                return f"Error fetching records: {e}"
            finally:
                cursor.close()
                self.release_connection(conn)
        return "No available connections."

    def update_record(self, table_name, record_id, name, age):
        conn = self.get_connection()
        if conn:
            try:
                cursor = conn.cursor()
                update_query = sql.SQL("UPDATE {} SET name = %s, age = %s WHERE id = %s").format(sql.Identifier(table_name))
                cursor.execute(update_query, (name, age, record_id))
                conn.commit()
                return "Record updated."
            except OperationalError as e:
                return f"Error updating record: {e}"
            finally:
                cursor.close()
                self.release_connection(conn)
        return "No available connections."

    def delete_record(self, table_name, record_id):
        conn = self.get_connection()
        if conn:
            try:
                cursor = conn.cursor()
                delete_query = sql.SQL("DELETE FROM {} WHERE id = %s").format(sql.Identifier(table_name))
                cursor.execute(delete_query, (record_id,))
                conn.commit()
                return "Record deleted."
            except OperationalError as e:
                return f"Error deleting record: {e}"
            finally:
                cursor.close()
                self.release_connection(conn)
        return "No available connections."

    @classmethod
    def close_pool(cls):
        """Close all connections in the pool."""
        if cls._pool:
            for conn in cls._pool:
                conn.close()
            cls._pool.clear()
            return "Connection pool to CockroachDB closed."
        return "No connection pool to close."


    
class CockroachClient:
    def __init__(self):
        self.connection = None
        self.cursor = None
        self.host = 'lead-manage-2232.jxf.gcp-asia-southeast1.cockroachlabs.cloud'
        self.port = 26257
        self.database = 'defaultdb'
        self.user = 'aadarsh'
        self.password = 'SGOawdGmMLKuFKjoqdOdWg'

    def connect(self):
        """Connect to the CockroachDB database."""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.connection.cursor()
            print("Connection to CockroachDB successful!")
        except OperationalError as e:
            print(f"The error '{e}' occurred")

    def execute_query(self, query, params=None):
        """Execute a single query."""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            self.connection.commit()
            print(f"query Executed")
        except OperationalError as e:
            print(f"The error '{e}' occurred")

    def fetch_all(self, query, params=None):
        """Fetch all results from a query."""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            return self.cursor.fetchall()
        except OperationalError as e:
            print(f"The error '{e}' occurred")
            return None

    def close(self):
        """Close the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Connection closed.")