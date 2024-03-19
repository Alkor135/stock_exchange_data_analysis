"""
Получение исторических данных по индексу MCFTR с MOEX ISS API и занесение записей в БД
Загружать от 2015-01-01
При попытке загрузить все данные за период в ответе приходят котировки с конца 2016 года, поэтому загрузка реализована
через загрузку каждого дня по итогам на все индексы.
"""

from pathlib import Path
import requests
from datetime import datetime, timedelta, date
from typing import Any

import apimoex
import pandas as pd
import sqlite3

import sqlighter3_MCFTR_day


def get_index_date_results(tradedate: date):
    today_date = datetime.now().date()  # Текущая дата и время

    arguments = {'securities.columns': ("TRADEDATE, OPEN, LOW, HIGH, CLOSE")}

    with requests.Session() as session:
        # print(f'{trade_date=}, {start_date=}')

        while tradedate != today_date:
            # Нет записи с такой датой
            if not sqlighter3_MCFTR_day.tradedate_exists(connection, cursor, tradedate):
                request_url = (f'http://iss.moex.com/iss/history/engines/stock/markets/index/securities.json?'
                               f'date={tradedate.strftime("%Y-%m-%d")}')
                print(f'{request_url=}')
                iss = apimoex.ISSClient(session, request_url, arguments)
                data = iss.get()
                df = pd.DataFrame(data['history'])

                if len(df) != 0:  # Если полученный ответ не пустой, чтобы исключить выходные дни
                    df = df.loc[df['SECID'] == 'MCFTR']  # Выборка нужного инструмента
                    if len(df) == 1:  # Строка с нужным инструментом есть на дату
                        df = df[['TRADEDATE', 'SECID', 'OPEN', 'LOW', 'HIGH', 'CLOSE']]
                        print(df.to_string(max_rows=20, max_cols=20), '\n')
                        # Удаление строк с пустыми OPEN, LOW, HIGH, CLOSE
                        df.dropna(subset=['OPEN', 'LOW', 'HIGH', 'CLOSE'], inplace=True)
                        df = df.reindex()
                        # print(df.iloc[0]['TRADEDATE'])
                        if not df['OPEN'].isnull().values.any():  # Проверка на пустые значения поля 'OPEN'
                            # Запись строки в БД
                            sqlighter3_MCFTR_day.add_tradedate(connection, cursor,
                                                               df.iloc[0]['TRADEDATE'],
                                                               df.iloc[0]['SECID'],
                                                               float(df.iloc[0]['OPEN']),
                                                               float(df.iloc[0]['LOW']),
                                                               float(df.iloc[0]['HIGH']),
                                                               float(df.iloc[0]['CLOSE']))
                            print('Строка записана в БД', '\n')

            tradedate += timedelta(days=1)


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    tiker: str = 'MCFTR'
    path_db: Path = Path(fr'c:\Users\Alkor\gd\data_quote_db\{tiker}_index_day.db')
    start_date: date = datetime.strptime('2015-01-01', "%Y-%m-%d").date()

    connection: Any = sqlite3.connect(path_db, check_same_thread=True)
    cursor: Any = connection.cursor()

    if sqlighter3_MCFTR_day.non_empty_table(connection, cursor):  # Если таблица Day не пустая
        # Меняем стартовую дату на дату последней записи
        start_date = datetime.strptime(sqlighter3_MCFTR_day.get_max_date(connection, cursor),
                                       "%Y-%m-%d").date()

    get_index_date_results(start_date)

    # with connection:
    #     cursor.execute("DELETE FROM Day WHERE TRADEDATE >= '2016-12-15' AND TRADEDATE <= '2016-12-28'")
    #     cursor.execute('DELETE FROM Day WHERE TRADEDATE > LSTTRADE')
