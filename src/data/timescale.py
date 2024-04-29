import psycopg2
from src.config.dbconfig import pg_config


class imp_exp_DAO:

    def __init__(self):
        connection_url = "host=%s dbname=%s user=%s password=%s" % (
            pg_config["host"],
            pg_config["dbname"],
            pg_config["user"],
            pg_config["password"],
        )
        self.conn = psycopg2.connect(connection_url)

    def insertBuyer(self, country, time, exports, imports, net_value):
        try:
            cursor = self.conn.cursor()
            query = "insert into fiscal(country, time, exports, imports, net_value) values (%s, %s, %s, %s, %s) returning tid;"
            cursor.execute(query, (country, time, exports, imports, net_value))
            tid = cursor.fetchone()[0]
            self.conn.commit()
            return tid
        except Exception as e:
            self.conn.rollback()
            print("Error occurred during insertion:", str(e))
            return None
        finally:
            cursor.close()
