import pandas as pd
import numpy as np
import sqlite3
import datetime
from pathlib import Path

import sqlighter3_minute_to_day_converter


connection = sqlite3.connect(r'c:\Users\Alkor\gd\data_quote_db\RTS_futures_minute.db',
                             check_same_thread=True)  # Создание соединения с БД
with connection:
    df = pd.read_sql('SELECT * FROM Minute', connection)  # Загрузка данных из БД
df = df.drop(['SECID', 'LSTTRADE'], axis=1)  # Удаление лишних колонок
df['date'] = df['TRADEDATE'].map(lambda x: x[:10])
df['time'] = df['TRADEDATE'].map(lambda x: x[11:])
df['date'] = pd.to_datetime(df['date'])
print(df.to_string(max_rows=6, max_cols=25))  # Проверка того, что загрузилось

date_lst = list(df['date'])  # Колонку с датами в список
date_sel = min(date_lst)  # Минимальная дата
date_sel = date_sel.date()
today_date = datetime.datetime.now().date()  # Текущая дата и время

tiker: str = 'RTS'
path_db: Path = Path(fr'c:\Users\Alkor\gd\data_quote_db\{tiker}_futures_minute_to_day_converter.db')
connection = sqlite3.connect(path_db, check_same_thread=True)
cursor = connection.cursor()

# while date_sel.date() != today_date:
while date_sel != today_date:
    # Нет даты в БД
    if not sqlighter3_minute_to_day_converter.tradedate_futures_exists(connection, cursor, date_sel):
        df_date = df.loc[df['date'].dt.date == date_sel]  # Выборка по дате из минутного DF
        print(date_sel, type(date_sel))
        # print(df_date.to_string(max_rows=6, max_cols=25))  # Проверка

        if len(df_date) != 0:  # Если полученный DF не нулевой
            date = df_date.iloc[0, df_date.columns.get_loc('date')].date()
            date = date.strftime("%Y-%m-%d")
            print(f'{date=}')
            open = df_date.iloc[0, df_date.columns.get_loc('OPEN')]
            print(f'{open=}')
            low = min(df_date['LOW'])
            print(f'{low=}')
            high = max(df_date['HIGH'])
            print(f'{high=}')
            close = df_date.iloc[-1, df_date.columns.get_loc('CLOSE')]
            print(f'{close=}')
            volume = df_date[['VOLUME']].sum()[0]  # Сумма объемов
            print(f'{volume=}')
            sqlighter3_minute_to_day_converter.add_tradedate_future(connection, cursor, date, open, low, high, close,
                                                                    int(volume))
    date_sel += datetime.timedelta(days=1)
