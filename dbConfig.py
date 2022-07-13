from sqlalchemy import create_engine
import sqlalchemy as sa


def con_to_db(database):
    engine = create_engine(
        f"mysql+pymysql://Mbrown:99Bullcalf2021@10.200.1.159:3306/{database}")
    conn = engine.connect()
    return conn


def df_to_sql(conn, dataframe, table_name):
    with conn.connect() as connection:
        dataframe.to_sql(
            con=connection,
            name=f'{table_name}',
            if_exists="replace",
            index=False
        )
