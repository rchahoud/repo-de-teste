import pyodbc 
import pandas as pd
from sqlalchemy import create_engine


server = 'GH_SERVER'
database = 'db_py_motim'
username = 'pyApp'
password = 'python#motim'
driver = 'ODBC Driver 17 for SQL Server'

#monta a url de conexão com o Banco para o pandas        
database_url = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}'

# Crie o engine de conexão com o banco de dados
engine = create_engine(database_url)

class SQLServerConnection:

    def __init__(self):
        self.connection = None
        
    
    def connect(self):
        try:
            self.connection = pyodbc.connect(
                'DRIVER={ODBC Driver 17 for SQL Server};'
                f'SERVER={server};'
                f'DATABASE={database};'
                f'UID={username};'
                f'PWD={password}'
            )
            print("Conexão estabelecida com sucesso.")
        except pyodbc.Error as e:
            print("Erro ao conectar ao banco de dados:", e)
            self.connection = None

    def disconnect(self):
        if self.connection:
            self.connection.close()
            print("Conexão fechada.")

    def execute_query(self, query):
        if not self.connection:
            self.connect()        
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
            print("Query executada com sucesso.")
        except pyodbc.Error as e:
            print("Erro ao executar a Query:", e)
            return None
              
        return cursor
    
    def execute_select_query( self, query):       
        
        if not self.connection:
            self.connect()        
        cursor = self.connection.cursor()
         
        try:            
            cursor.execute(query)
            data = cursor.fetchall()
            return [list(row) for row in data]
            
            """ Para retornar os dados em dicionarios
            for row in cursor.fetchall():
                data.append(dict(zip(columns, row)))
            """
        
        except Exception as e:
            print("Ocorreu um erro ao executar a consulta SELECT:", e)
            return None
        finally:
            self.disconnect()
    

    def insere_data_frame(self, df, nome_tabela):
        if not self.connection:
            self.connect()
        
        # Iteração sobre os dados do DataFrame
        cursor = self.connection.cursor()
        for index, row in df.iterrows():
            # Ajuste este trecho para corresponder às colunas da sua tabela
            columns = ', '.join(row.index)
            values = ', '.join(['?' for _ in row])
            query = f"INSERT INTO {nome_tabela} ({columns}) VALUES ({values})"
                        
            cursor.execute(query, tuple(row))
        
        self.connection.commit()
        print(f"{df.shape[0]} Registros Inseridos na Tabela '{nome_tabela}'.")
        
   
    def execute_update_query(self, query):
        if not self.connection:
            self.connect()
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
            print("Registro Atualizado com sucesso.")
        except pyodbc.Error as e:
            print("Erro ao Atualizar o registro:", e)
            return None
        finally:
            self.disconnect()

