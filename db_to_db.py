import os
from datetime import datetime
import logging
import pyodbc
import psycopg2
import pandas as pd
import json
from sqlalchemy import create_engine, text
from PyQt5.QtWidgets import QMessageBox
# from psycopg2 import sql


class dbdb():

    stat = None

    def __init__(self, prj, dir):
        self.conn_pg = None
        self.dir_path = dir
        self.project_name = prj

        self.prj() #читаем проект

        self.pg_user = 'хххххх'
        self.pg_password = 'хххххх'
        self.pg_port = 'хххххх'
        self.pg_dbname = self.sourse['Pocet_0'][0]['BD_NameS']
        self.pg_host = self.sourse['Pocet_0'][0]['SourseS']

        self.ms_user = 'ххххххх'
        self.ms_password = 'хххххххх'
        self.ms_port = 'хххххххх'
        self.ms_dbname = self.sourse['Pocet_0'][0]['BD_NameR']
        self.ms_host = self.sourse['Pocet_0'][0]['SourseR']

        self.pocet_list = list(self.sourse.keys())  # список пакетов
        self.table_list(self.pocet_list) # список таблиц

        self.connect_pg(self.pg_dbname, self.pg_user, self.pg_password, self.pg_host, self.pg_port)
        self.connect_ms(self.ms_dbname, self.ms_user, self.ms_password, self.ms_host, self.ms_port)


    def read_conv_write(self):
        logging.basicConfig(
            filename='app.log',  # Указываем имя файла
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        for i, table in enumerate(self.pg_table_lst):
            print(table, 'Run')
            logging.info(f'{self.project_name} --{table}-- Start')

            df = self.db_frame(table)

            R_columns = []

            for j, col in enumerate(df.columns):
                print(col, self.sourse[self.pocet_list[i]][j]["TypeS"])
                try:
                    df[col] = df[col].fillna(0)
                    df.columns = ['perc' if col == 'percent' else col for col in df.columns]

                    type = self.sourse[self.pocet_list[i]][j]["TypeS"]
                    if type == 'bigint':
                        df[col] = df[col].astype('Int64')  # Используйте 'Int64' для поддержки NA


                    elif type == 'character varying':
                        # # Удаление недопустимых символов
                        # df[col] = df[col].str.replace(r'[^\x00-\x7F]+%', '', regex=True)
                        df[col] = df[col].astype(str)
                    elif type == 'text':
                        df[col] = df[col].astype(str)
                    elif type == 'double precision':
                        df[col] = df[col].astype(float)
                    elif type == 'boolean':
                        df[col] = df[col].astype('Int64')
                    elif type == 'timestamp without time zone':
                        df[col] = pd.to_datetime(df[col])
                    elif type == 'date':
                        df[col] = pd.to_datetime(df[col]).dt.date
                    else:
                        print(f"Неизвестный тип: {type}")
                except Exception as e:
                    print(f"Ошибка при выполнении запроса: {e}")

                R_columns.append(self.sourse[self.pocet_list[i]][j]["VolumeR"])

            # # Запись DataFrame в MSSQL
            df.to_sql(table, con=self.engine_ms, if_exists='replace', index=False)



            #Получаем данные в DataFrame
            with self.engine_ms.connect() as conn:
                sql_read = f"SELECT TOP 5 * FROM {table}"
                df_1 = pd.read_sql_query(sql_read, conn)

            # Выводим DataFrame
            print(df_1)
            print(df)
            logging.info(f'{self.project_name} ***{table}*** Complete {len(df.columns)} x {len(df)}')

        print('complete')


    def table_list(self, pocet_list):
        self.pg_table_lst = []
        self.ms_table_list = []

        for pocet in pocet_list:
            pg_table = self.sourse[pocet][0]['TableS']
            ms_table = self.sourse[pocet][0]['TableR']
            self.pg_table_lst.append(pg_table)
            self.ms_table_list.append(ms_table)

    def prj(self):
        project = os.path.join(self.dir_path, self.project_name + '.json')
        with open(project, 'r') as file:
            self.sourse = json.load(file)

    def db_frame(self, table):
        try:
            query = f"SELECT * FROM {table};"
            df = pd.read_sql_query(query, self.engine_pg)
            return df

        except Exception as e:
            print(f"Ошибка при выполнении запроса: {e}")
            return None

    def connect_ms(self, ms_dbname, ms_user,  ms_password, ms_host, ms_port):
        dbname = ms_dbname
        user = ms_user
        password = ms_password
        host = ms_host
        port = ms_port

        try:
            connection_string = f"mssql+pyodbc://{user}:{password}@{host}/{dbname}?driver=ODBC+Driver+17+for+SQL+Server"
            self.engine_ms = create_engine(connection_string, fast_executemany=True)

        except Exception as _ex:
            error_box = QMessageBox()
            error_box.setWindowTitle("Ошибка подключения")
            error_box.setText(f"Проблемы с подключением к БД: {_ex}")
            error_box.exec_()

    def connect_pg(self, pg_dbname, pg_user,  pg_password, pg_host, pg_port):
        dbname = pg_dbname
        user = pg_user
        password = pg_password
        host = pg_host
        port = pg_port

        try:
            connection_string = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
            self.engine_pg = create_engine(connection_string)

        except Exception as _ex:
            error_box = QMessageBox()
            error_box.setWindowTitle("Ошибка подключения")
            error_box.setText(f"Проблемы с подключением к БД: {_ex}")
            error_box.exec_()

