import requests
import pyodbc
import pandas as pd
from sqlalchemy import create_engine
from io import StringIO

url = "https://strecursoschallenge.blob.core.windows.net/challenge/nuevas_filas.csv?sp=r&st=2024-08-26T20:28:39Z&se=2024-12-31T04:28:39Z&sv=2022-11-02&sr=b&sig=7vZYDdZc7%2B%2FcwVYEAlSCzixAKiSrKlaeU8%2Fns%2B2YQVU%3D"
connection_string = "mssql+pyodbc:///?odbc_connect=DSN=SQL_ODBC;driver=ODBC+Driver+17+for+SQL+Server"
target_table = 'Raw_Unificado'
server = 'localhost\\SQLEXPRESS01'
database = 'Testing_ETL'
driver = '{ODBC Driver 17 for SQL Server}'
sp_name = '[dbo].[run_raw_Unificado]'

def get_data(url,connection_string,target_table):
   try:
      response = requests.get(url)
      data = pd.read_csv(StringIO(response.text))
      engine = create_engine(connection_string)
      data = data.drop("FECHA_COPIA", axis=1)
      with engine.connect() as connection:
         data.to_sql(target_table, con=connection, if_exists='replace', index=False)
      msg = "Se cargo la tabla raw sin problemas"
   except Exception as e:
      msg = "Ocurrio un error en la carga de informacion" + str(e)
   return msg

def exec_sp(driver,server,database):
   connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
   try:
      conn = pyodbc.connect(connection_string)
      cursor = conn.cursor()
      conn.autocommit = True
      cursor.execute(f"EXEC {sp_name}")
      cursor.close()
      conn.close()
      msg = "Se ejecuto el SP sin problemas"
   except Exception as e:
      msg = "Ocurrio un error al ejecutar el SP" + str(e)
   return msg

if __name__ == "__main__":
   load_data = get_data(url,connection_string,target_table)
   print(load_data)
   exec_store_procedure = exec_sp(driver,server,database)
   print(exec_store_procedure)
