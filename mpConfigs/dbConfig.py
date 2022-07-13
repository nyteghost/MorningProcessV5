from sqlalchemy import create_engine
from sqlalchemy import event
from dotenv import load_dotenv
import os
import better_exceptions

load_dotenv()
# better_exceptions.MAX_LENGTH = None
# better_exceptions.hook()


connectString = os.getenv('connectString')


class dbConnect:
    def __init__(self, database):
        self.database = database
        self.engine = create_engine(f"{connectString}/{database}")
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
    shipDataQuery = f"SELECT * FROM gcaassetmgmt_2_0.ship_vwaddressvalidation"
    shipData = pd.read_sql(shipDataQuery, conn)
    print(shipData)
