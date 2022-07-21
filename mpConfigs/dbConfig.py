from sqlalchemy import create_engine
from sqlalchemy import event
from dotenv import load_dotenv
import os
from urllib.parse import quote
import better_exceptions

load_dotenv(dotenv_path='config.env')
# better_exceptions.MAX_LENGTH = None
# better_exceptions.hook()


userName = os.getenv('DBUSERNAME')
password = os.getenv('PASSWORD')
ipAddress = os.getenv('IPADDRESS')
ca_path = os.getenv('SSL_CA')
ssl_args = {'ssl_ca': ca_path}
connectString = f'mysql+pymysql://{userName}:%s@{ipAddress}:3306' % quote(password)
print(connectString)


class dbConnect:
    def __init__(self, database):
        self.database = database
        self.engine = create_engine(f"{connectString}/{database}", connect_args=ssl_args)
        self.conn = self.engine.connect()

    def df_to_sql(self, dataframe, table):
        @event.listens_for(self.conn, "before_cursor_execute")
        def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
            if executemany:
                cursor.fast_executemany = True

        with self.conn.connect() as connection:
            dataframe.to_sql(
                con=connection,
                name=f'{table}',
                if_exists="replace",
                index=False
            )

    def call(self, procedure):
        with self.conn.connect() as connection:
            connection.execute(f'call {procedure}')

    def connection(self):
        return self.conn


if __name__ == "__main__":
    import pandas as pd
    s = dbConnect('gcaassetmgmt_2_0')
    conn = s.connection()
    # print(conn)
    # # shipDataQuery = f"SELECT * FROM gcaassetmgmt_2_0.ship_vwaddressvalidation"
    # # shipData = pd.read_sql(shipDataQuery, conn)
    # # print(shipData)
