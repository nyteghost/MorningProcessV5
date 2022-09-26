from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import event
from dotenv import load_dotenv
from mpConfigs.doorKey import config
import os
from urllib.parse import quote
import better_exceptions
from pprint import pprint
import pandas as pd
from sqlalchemy import DateTime
from sqlalchemy import exc
load_dotenv(dotenv_path='config.env')
better_exceptions.MAX_LENGTH = None
better_exceptions.hook()

ipAddress = config['azure']['host']
userName = config['azure']['user']
password = config['azure']['pass']
# ssl_args = {'ssl_ca': config['azure']['cert'].replace('\n','')}
# print(config['azure']['cert'].replace('\n', ''))
ca_path = os.getenv('SSL_CA')
# ssl_args = {'sslcert': ca_path}

# connectAzure = f'mysql+mysqlconnector://{userName}:{password}@{ipAddress}:3306'
# print(connectAzure)

# pd.set_option('display.max_rows', 100)
# pd.set_option('display.max_columns', 100)
class dbConnect:
    def __init__(self, database):
        # connect_string = f'mysql+pymysql://{userName}:{password}@{ipAddress}:3306/{database}'
        # self.engine = create_engine(connect_string, connect_args=ssl_args)
        connect_string = f"mysql+pymysql://{userName}:{password}@{ipAddress}:3306/{database}?ssl_ca=C:/cert/DigiCertGlobalRootCA.crt.pem"
        self.engine = create_engine(connect_string)
        # print(self.engine)
        self.conn = self.engine.connect()

    def df_to_sql(self, dataframe, table, print_only=''):
        print()
        with self.conn.connect() as connection:
            if print_only == 0:
                pprint(dataframe.dtypes.apply(lambda x: x.name).to_dict())
            elif print_only == 1:
                print(dataframe)
                print("Output:")
                print(dataframe.dtypes)
            else:
                print('starting update to SQL')
                print(f'Truncating {table}')
                try:
                    connection.execute(text(f"TRUNCATE TABLE {table}"))
                except exc.SQLAlchemyError as e:
                    print(f'Call {"df_to_sql"} failed')
                # except Exception as e:
                #     if "doesn't exist" in str(e):
                #         table = table.replace('dbo_', '')
                #         connection.execute(text(f"TRUNCATE TABLE {table}"))

                finally:
                    print(f'Truncated {table}')
                print(f'Adding data to {table}.')
                try:
                    dataframe.to_sql(
                        con=connection,
                        name=f'{table}',
                        if_exists="append",
                        index=False,
                        chunksize=10000,
                        method='multi',
                        # dtype={"dob": DateTime}
                    )
                except Exception as e:
                    print(e)
                    print(f'Data Failed to be added to {table}.')
                else:
                    print(f'Data has been added to {table}.')

    def call(self, procedure):
        with self.conn.connect() as connection:
            try:
                connection.execute(f'call {procedure}')
            except exc.SQLAlchemyError as e:
                print(type(e))
                print(e)
                print(f'Call {procedure} failed')
                return False
            else:
                print(f'Call {procedure} successful')
                return True

    def get_dtypes(self, df, query):
        result = pd.read_sql(query, self.engine)
        print(result)
        pprint(result.dtypes.apply(lambda x: x.name).to_dict())
        return result

    def query(self, query):
        with self.conn.connect() as connection:
            query = "CALL " + query
            result = pd.read_sql(query, connection)
            print(result)
        return result

    def connection(self):
        if __name__ == "__main__":
            print('Connection Made')
        return self.conn


if __name__ == "__main__":
    import pandas as pd
    s = dbConnect('gcaassetmgmt_2_0')
    conn = s.connection()
    # print(conn)
    # # shipDataQuery = f"SELECT * FROM gcaassetmgmt_2_0.ship_vwaddressvalidation"
    # # shipData = pd.read_sql(shipDataQuery, conn)
    # # print(shipData)
