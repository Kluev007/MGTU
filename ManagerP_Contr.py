import sys
import os
import fnmatch
import json
import time

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
import logging
from datetime import datetime

from ManagerP import Ui_ProjectManager
# from db_to_db import dbdb
from db_to_map import dbdb
from Spark_ms import ms_db
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import QTimerEvent
from PyQt5.QtWidgets import QMainWindow, QApplication, QMessageBox, QWidget, QInputDialog
from PyQt5.QtGui import QBrush, QColor
from PyQt5.QtCore import QTimerEvent, QTimer, QObject

class ManagerP(QMainWindow):

    def __init__(self):
        super(ManagerP, self).__init__()
        self.ui = Ui_ProjectManager()
        self.ui.setupUi(self)
        self.counter = 0

        self.dir_path = "E:\\ETL PRO\\Projects"
        self.setting_path = "E:\\ETL PRO\\Settings"
        self.surces_path = "E:\\ETL PRO\\Surces"
        self.P_path = None



        self.ui.Open.clicked.connect(self.Hist)

        projects = self.prj_dict()
        self.tableWidget = self.ui.tableWidget

        # Создаем планировщик
        scheduler = BackgroundScheduler()
        # scheduler.add_listener(self.job_listener, EVENT_JOB_RUNNING | EVENT_JOB_ERROR)
        self.fltr_str = []
        self.fltr_strD = []
        self.D_t = []

        for idx, prj in enumerate(projects):
            self.tableWidget.insertRow(idx+1)
            new_item = QtWidgets.QTableWidgetItem(prj)
            self.tableWidget.setItem(idx+1, 0, new_item)
            set = self.prj_set(prj)


            status = QtWidgets.QTableWidgetItem('Stop')
            self.tableWidget.setItem(idx + 1, 1, status)
            time_start = QtWidgets.QTableWidgetItem(f'{set['Start_h']}:{set['Start_m']}')
            self.tableWidget.setItem(idx + 1, 2, time_start)
            time_stop = QtWidgets.QTableWidgetItem(f'{set['Stop_h']}:{set['Stop_m']}')
            self.tableWidget.setItem(idx + 1, 3, time_stop)
            timer = QtWidgets.QTableWidgetItem('00:00:00')
            self.tableWidget.setItem(idx + 1, 4, timer)
            if set['Man_mod'] == 1:
                Man = QtWidgets.QTableWidgetItem('Manual')
                self.tableWidget.setItem(idx + 1, 5, Man)

                # Создаем кнопку
                button = QtWidgets.QPushButton("Start")
                button.setStyleSheet("color: white; background-color: blue;")  # Задаем цвет текста и фона
                font = QtGui.QFont("Arial", 12, QtGui.QFont.Bold)  # Устанавливаем шрифт
                button.setFont(font)

                button.clicked.connect(lambda checked, idx=idx: self.on_button_click(projects[idx], idx))  # Подключение сигнала
                self.tableWidget.setCellWidget(idx + 1, 9, button)  # Добавляем кнопку во второй столбец

            if set['Time_mod'] == 1:
                TimeM = QtWidgets.QTableWidgetItem('Time')
                self.tableWidget.setItem(idx + 1, 6, TimeM)

                # Запланировать выполнение задачи с разными параметрами каждый день в определенное время
                scheduler.add_job(self.on_button_click, 'cron', hour=set['Start_h'], minute=set['Start_m'],
                                  args=[projects[idx], idx], max_instances=3, misfire_grace_time=1800)

            if set['Signal_mod'] == 1:
                    Signal = QtWidgets.QTableWidgetItem('Filter')
                    self.tableWidget.setItem(idx + 1, 7, Signal)
                    filterI = set['Filter']
            else:
                filterI = ""

            if set['Signal_mod2'] == 1:
                    filterD = set['Filter2']
                    D = 1
            else:
                filterD = ""
                D = 0

            if set['Mono_mod'] == 1:
                Mono = QtWidgets.QTableWidgetItem('One')
                self.tableWidget.setItem(idx + 1, 8, Mono)
            if set['Cycle_mod'] == 1:
                Cycle = QtWidgets.QTableWidgetItem('Cycle')
                self.tableWidget.setItem(idx + 1, 8, Cycle)

            self.fltr_str.append(filterI)
            self.fltr_strD.append(filterD)
            self.D_t.append(D)

        scheduler.start()


    def on_button_click(self, prj, idx):
        Str_stut = QtWidgets.QTableWidgetItem('Run')
        self.tableWidget.setItem(idx+1, 1, Str_stut)

        t1 = datetime.now()

        Str_time = QtWidgets.QTableWidgetItem(datetime.now().time().strftime("%H:%M:%S"))
        self.tableWidget.setItem(idx+1, 2, Str_time)

        self.tableWidget.repaint()


        try:
            write = dbdb(prj, self.dir_path, self.fltr_str[idx], self.fltr_strD[idx], self.D_t[idx])
            # wr = ms_db(prj, self.dir_path, self.fltr_str[idx], self.fltr_strD[idx], self.D_t[idx])
            write.read_conv_write()
        except:

            Str_stut = QtWidgets.QTableWidgetItem('Err')
            self.tableWidget.setItem(idx + 1, 1, Str_stut)


        t2 = datetime.now()
        Str_time = QtWidgets.QTableWidgetItem(datetime.now().time().strftime("%H:%M:%S"))
        self.tableWidget.setItem(idx+1, 3, Str_time)

        t3 = t2 - t1

        # Получаем разницу в секундах
        total_seconds = int(t3.total_seconds())
        # Форматируем разницу в минуты и секунды
        minutes, seconds = divmod(total_seconds, 60)

        # Создаем строковое представление времени
        formatted_time = f'{minutes:02}:{seconds:02}'
        S_time = QtWidgets.QTableWidgetItem(formatted_time)
        self.tableWidget.setItem(idx+1, 4, S_time)

        Str_stut = QtWidgets.QTableWidgetItem('Success')
        self.tableWidget.setItem(idx + 1, 1, Str_stut)





    def Hist(self):
        os.startfile(r'E:\Py\ProjManager\app.log')

    def prj_dict(self):
        all_files = os.listdir(self.dir_path)
        json_files = fnmatch.filter(all_files, '*.json')
        self.prj_lst = [fl.split('.')[0] for fl in json_files]
        return self.prj_lst

    def prj_set(self, prj):
        self.fl_path = os.path.join(self.setting_path, prj + '_setting.json')
        with open(self.fl_path, 'r') as file:
            self.settings_dict = json.load(file)
        return  self.settings_dict





if __name__ == "__main__":
    app = QApplication(sys.argv)

    window = ManagerP()
    window.show()
    sys.exit(app.exec_())