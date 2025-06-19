import os
from datetime import datetime
import logging
import pyodbc
import psycopg2
import pandas as pd
import modin.pandas as mpd
import json
from sqlalchemy import create_engine, text, MetaData
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from PyQt5.QtWidgets import QMessageBox
from sqlalchemy.sql.functions import current_date, current_time
from sqlalchemy.testing.suite.test_reflection import metadata
import telega


class dbdb():

    stat = None

    def __init__(self, prj, dir, fltr_str, fltr_strD, D_t):
        self.fltr_str = str(fltr_str)
        self.fltr_strD = str(fltr_strD)
        self.D_t = D_t

        self.conn_pg = None
        self.dir_path = dir
        self.project_name = prj

        self.prj() #читаем проект

        self.pg_user = 'ххххх'
        self.pg_password = 'хххххх'
        self.pg_port = 'ххххх'
        self.pg_dbname = self.sourse['Pocet_0'][0]['BD_NameS']
        self.pg_host = self.sourse['Pocet_0'][0]['SourseS']

        self.ms_user = 'хххххх'
        self.ms_password = 'ххххххх'
        self.ms_port = 'ххххх'
        self.ms_dbname = self.sourse['Pocet_0'][0]['BD_NameR']
        self.ms_host = self.sourse['Pocet_0'][0]['SourseR']

        self.pocet_list = list(self.sourse.keys())  # список пакетов
        self.table_list(self.pocet_list) # список таблиц

        self.connect_pg(self.pg_dbname, self.pg_user, self.pg_password, self.pg_host, self.pg_port)
        self.connect_ms(self.ms_dbname, self.ms_user, self.ms_password, self.ms_host, self.ms_port)

        # _______________==== Создание базы через automap ====____________
        self.Base = None
        # Создаем свой экземпляр MetaData
        self.metadata = MetaData()
        # Отражаем необходимые таблицы
        self.metadata.reflect(self.engine_ms, only=self.ms_table_list)
        # Создаем автоматическую базу
        self.Base = automap_base(metadata=self.metadata)
        # Подготовьте базу, чтобы автоматически создать классы для отраженных таблиц
        self.Base.prepare()
        #**************************************************************************


        #_______________--- Настраиваем логгирование---_________________
        logging.basicConfig(
            filename='app.log',  # Указываем имя файла
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        #***************************************************************************


    def read_conv_write(self):
        self.tg = telega.Telega()

        for i, table in enumerate(self.pg_table_lst):
            table_ms = self.ms_table_list[i]

            print(table, 'to', table_ms, 'Run')

            t1 = datetime.now()

            volume_list = self.volume_list(i)
            volume_str = ', '.join(volume_list) # формируем список полей в датафрейм

            ms_volume_list = self.ms_volume_list(i)
            ms_volume_str = ', '.join(ms_volume_list)  # формируем список полей в датафрейм

            df = self.db_frame(table, volume_str) # получаем датафрейм

            if table_ms == 'product_categories_rus':
                df['icon_url'] =  df['title']
                # Переименование столбца
                df.rename(columns={'icon_url': 'title_rus'}, inplace=True)


            self.convert(df, i) # конвертируем типы при несовпадении

            if self.D_t == 1:
                self.cleanD(table_ms)
            else:
                self.cleanT(table_ms) # стираем таблицы

            self.bulk_insert(df, table_ms)   # заливаем в МС

            self.read_table(table_ms, ms_volume_str) # читаем то что залили

            t2 = datetime.now()
            t3 = t2 - t1
            total_seconds = int(t3.total_seconds())
            minutes, seconds = divmod(total_seconds, 60)
            formatted_time = f'{minutes:02}:{seconds:02}'
            massage = f'{t1}  Proj: {self.project_name} Time:{formatted_time} Table: {table} Complete: {len(df.columns)} x {len(df)}'
            logging.info(massage)

            self.tg.table_send(massage)


        print('complete')


    def read_table(self, table_ms, ms_volume_list):
        with self.engine_ms.connect() as conn:
            sql_read = f"SELECT * FROM {table_ms} {self.fltr_strD}"
            # sql_read = f"SELECT {ms_volume_list} FROM {table_ms} {self.fltr_strD}"
            df_1 = pd.read_sql_query(sql_read, conn)

        return df_1




    def bulk_insert(self, df, table_ms):
        data_dict = df.to_dict(orient='records')  # Преобразование в список словарей

        with self.engine_ms.connect() as conn:
            try:
                # Создание сессии
                Session = sessionmaker(bind=self.engine_ms)
                session = Session()

                # Доступ к классу для таблицы
                meta_table = self.metadata.tables[table_ms]

                # Вставка данных
                session.execute(meta_table.insert(), data_dict)
                session.commit()  # Подтверждение транзакции
                

            except Exception as e:
                # session.rollback()  # Откат транзакции в случае ошибки
                print(f"An error occurred: {e}")
                massage = f' Ошибка при загрузке {table_ms}: {str(e)}'
                logging.info(massage)
                self.tg.table_send(massage)

            finally:
                session.close()  # Закрытие сессии
        return "success"

    def convert(self, df, i): # конвертируем типы при несовпадении
        for j, col in enumerate(df.columns):
            print(col, self.sourse[self.pocet_list[i]][j]["TypeS"])
            try:
                df[col] = df[col].fillna(0)
                df.columns = ['perc' if col == 'percent' else col for col in df.columns]

                type = self.sourse[self.pocet_list[i]][j]["TypeS"]
                if type == 'bigint':
                    df[col] = df[col].astype('Int64')  # Используйте 'Int64' для поддержки NA
                elif type == 'character varying':
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
                massage = f' Ошибка конвертации типа данных {col}: {str(e)}'
                logging.info(massage)
                self.tg.table_send(massage)
        return df

    def cleanT(self, table_ms):
        with self.engine_ms.connect() as conn:
            try:

                trans = conn.begin()  # Начало транзакции
                # ******************** key********************
                key_off = f"ALTER TABLE {table_ms} NOCHECK CONSTRAINT ALL"
                conn.execute(text(key_off))

                sql_delete = f"TRUNCATE TABLE {table_ms}"
                conn.execute(text(sql_delete))

                trans.commit()  # Фиксация транзакции

            except Exception as e:
                massage = f'Проблемы во время удаления таблицы {table_ms}: {e}'
                logging.info(massage)
                self.tg.table_send(massage)

            finally:
                # Включение ограничений
                key_on = f"ALTER TABLE {table_ms} CHECK CONSTRAINT ALL"
                conn.execute(text(key_on))


    def cleanD(self, table_ms):
        with self.engine_ms.connect() as conn:
            try:

                trans = conn.begin()  # Начало транзакции
                # ******************** key********************
                key_off = f"ALTER TABLE {table_ms} NOCHECK CONSTRAINT ALL"
                conn.execute(text(key_off))

                sql_delete = f"""
            DELETE 
            FROM {table_ms} 
            {self.fltr_strD}
            """
                conn.execute(text(sql_delete))

                trans.commit()  # Фиксация транзакции


            except Exception as e:
                massage = f'Проблемы во время удаления таблицы {table_ms}: {e}'
                logging.info(massage)
                self.tg.table_send(massage)


            finally:
                # Включение ограничений
                key_on = f"ALTER TABLE {table_ms} CHECK CONSTRAINT ALL"
                conn.execute(text(key_on))


    def table_list(self, pocet_list):
        self.pg_table_lst = []
        self.ms_table_list = []

        for pocet in pocet_list:
            pg_table = self.sourse[pocet][0]['TableS']
            ms_table = self.sourse[pocet][0]['TableR']

            self.pg_table_lst.append(pg_table)
            self.ms_table_list.append(ms_table)


    def volume_list(self, i):
        pg_volume_list = []
        for j, vol in enumerate(self.sourse[self.pocet_list[i]]):
            pg_volume = self.sourse[self.pocet_list[i]][j]['VolumeS']
            pg_volume= f'"{pg_volume}"'
            pg_volume_list.append(pg_volume)

        return pg_volume_list

    def ms_volume_list(self, i):
        ms_volume_list = []
        for j, vol in enumerate(self.sourse[self.pocet_list[i]]):
            ms_volume = self.sourse[self.pocet_list[i]][j]['VolumeR']
            ms_volume = f'"{ms_volume}"'
            ms_volume_list.append(ms_volume)

        return ms_volume_list

    def prj(self):
        project = os.path.join(self.dir_path, self.project_name + '.json')
        with open(project, 'r') as file:
            self.sourse = json.load(file)
        return self.sourse

    def db_frame(self, table, volume_list):
        try:
            query = f"""
            SELECT {volume_list} 
            FROM {table} 
            {self.fltr_str}
            """
            # df = mpd.read_sql(query, self.engine_pg)
            df = pd.read_sql_query(query, self.engine_pg)
            # logging.info(f'Датафрейм {table} создан')
            return df
        except Exception as e:
            massage = f'Ошибка обработки строки в таблице {table}: {str(e)}'
            print(massage)
            logging.error(massage)
            self.tg.table_send(massage)

            return None

    def mdb_frame(self, table, volume_list):
        try:
            query = f"""
            SELECT {volume_list} 
            FROM {table} 
            {self.fltr_str}
            """
            df = mpd.read_sql(query, self.engine_pg)
            # df = pd.read_sql_query(query, self.engine_pg)
            logging.info(f'Датафрейм {table} создан')
            return df

        except Exception as e:
            massage = f'Ошибка обработки строки в таблице {table}: {str(e)}'
            print(massage)
            logging.error(massage)
            self.tg.table_send(massage)

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
            massage = f'Проблемы с подключением к БД MsSQL: {_ex}'
            error_box.setText(massage)
            logging.error(massage)
            self.tg.table_send(massage)
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
            massage = f'Проблемы с подключением к БД PostgreSQL: {_ex}'
            error_box.setText(massage)
            logging.error(massage)
            self.tg.table_send(massage)
            error_box.exec_()

