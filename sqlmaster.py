import numpy as np
import pandas as pd
from datetime import datetime, date

class Sqlx:
    def __init__(self, cnn):
        
        self.cnn = cnn
        self.cursor()
        self.formato_sql = {
            int:'int',
            str:'varchar(max)',
            float:'decimal(18,2)',
            date:'date',
            datetime:'datetime'
        }

    def cursor(self):
        self.cursor = self.cnn.cursor()

    def close(self):
        self.cnn.close()

    # Cria a tabela no sql
    def create_table(self, campos, valores, nome):
        
        cols = [campos[n] + ' ' + self.formato_sql[type(i)] for n, i in enumerate(valores[0])]
        cols = ', '.join(cols)
        query = f'CREATE TABLE {nome} ({cols})'
        self.cursor.execute(query)

    # Insere na tabela no sql
    def insert_into(self, nome, valores):
        self.cursor.execute(f"select * from {nome}")
        cols = list(map(lambda x: x[0], self.cursor.description))
        query = f"insert into {nome} ({', '.join(cols)})values ({', '.join(['?' for i in cols])})"
        vals = [tuple(i) for i in valores]
        vals = [vals[i:i+1000] for i in range(0,len(vals),1000)]
        self.cursor.fast_executemany = True
        for val in vals:
            try:
                self.cursor.executemany(query, val)
                self.cnn.commit()
            except:
                print('erro ao inserir')
                self.cnn.commit()
        self.cursor.fast_executemany = False


    # Verifica se a tabela existe
    def table_exist(self, nome):
        self.cursor.execute(f"IF OBJECT_ID ('{nome}') IS NOT NULL SELECT 1 AS res ELSE SELECT 0 AS res")
        return self.cursor.fetchone()[0]

    # Deleta de uma tabela de acordo com condições
    def delete_from(self, nome, **campos):
        cond = [f"{i} = '{campos[i]}'" for i in campos]
        cond = ' and '.join(cond)
        query = f' delete from {nome} where {cond}'
        self.cursor.execute(query)
        self.cnn.commit()

    # Verifica se registros existem em uma tabela
    def reg_existe(self, nome, **campos):
        if not self.table_exist(nome):
            return 0
        cond = [f"{i} = '{campos[i]}'" for i in campos]
        cond = ' and '.join(cond)
        query = f' select count(*) from {nome} where {cond}'
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]

    # Exporta para o sql
    def to_sql(self, nome, tabela, seexiste='fail'):
        
        if isinstance(tabela, pd.DataFrame):
            tabela = [list(tabela),tabela.values.tolist()]

        campos, valores = tabela[0], tabela[1]
        
        if self.table_exist(nome):
            if seexiste == 'replace':
                self.cursor.execute('drop table ' + nome)  
                self.cnn.commit()
                self.create_table(campos, valores, nome)
                self.insert_into(nome, valores)
            elif seexiste == 'append':
                self.insert_into(nome, valores)
            elif seexiste == 'fail':
                print('ERRO: A tabela já existe')
        else:
            self.create_table(campos, valores, nome)
            self.insert_into(nome, valores)

    def read_sql(query):
        cursor = cnn.cursor()
        res = cursor.execute(query)
        res = res.fetchmany(999999999)
        res = [list(i) for i in res]
        return pd.DataFrame(res, columns=[i[0] for i in cursor.description])
