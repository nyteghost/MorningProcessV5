from sqlalchemy import create_engine
from sqlalchemy import event


class dbConnect:
    def __init__(self, database):
        self.database = database
        self.engine = create_engine(f"mysql+pymysql://Mbrown:99Bullcalf2021@10.200.1.159:3306/{database}")
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
