import requests
import pandas as pd
from time import sleep
import sqlite3
from pathlib import Path
from typing import List


def areas_parser(url: str, country: str, areas: List[str]) -> dict:
    """Парсит регионы и возвращает id для каждого региона"""
    countries_df = pd.read_json(url)  # парсим сайт по URL
    country_df = countries_df[countries_df.name == country]  # оставляем только заданную страну
    areas_data = country_df.iloc[0].areas  # достаём данные с регионами только заданной страны
    areas_json = str(areas_data).replace("'", '"')  # преобразуем в формат json
    areas_df = pd.read_json(areas_json)  # парсим json в DF
    areas_df = areas_df[areas_df.name.isin(areas)]  # оставляем только заданные регионы
    areas_df = areas_df[['id', 'name']]
    areas_dct = areas_df.set_index('id').name.to_dict()  # преобразовываем DF в словарь
    return areas_dct


def get_query(query_file_path: str) -> str:
    """Парсит SQL-запрос из файла"""
    return (Path('sql') / query_file_path).read_text(encoding='utf-8')


def execute(query: str, db_name: str, params: 'dict | None' = None) -> int:
    try:
        with sqlite3.connect(db_name) as connection:
            connection.row_factory = sqlite3.Row
            cursor = connection.cursor()
            if len(query.strip(';').split(';')) > 1:
                cursor.executescript(query)
            else:
                cursor.execute(query, params if params else {})
    except(Exception) as error:
        print(error)
        connection.rollback()
    else:
        connection.commit()
    finally:
        cursor.close()
    connection.close()
    return cursor.rowcount


def get_table(table_name: str, db_name: str) -> pd.DataFrame:
    try:
        with sqlite3.connect(db_name) as connection:
            df = pd.read_sql(f'select * from {table_name}', connection)
    except Exception as ex:
        print(ex)
        df = pd.DataFrame()
    connection.close()
    return df


def get_view(query: str, db_name: str, params: 'dict | None' = None) -> pd.DataFrame:
    try:
        with sqlite3.connect(db_name) as connection:
            df = pd.read_sql(query, connection, params=params)
    except Exception as ex:
        print(ex)
        df = pd.DataFrame()
    connection.close()
    return df


def get_data_by_api(url: str, params: 'dict | None' = None, attempts: int = 3) -> dict:
    """Парсит данные с сайта по url api с заданными параметрами"""
    for _try in range(attempts):
        # Запускаем запрос
        result = requests.get(url, params=params)
        # Просматриваем запрошенный URL
        print('URL:', result.url)
        # Проверяем ответ
        if result.status_code == 200:
            print('GET request sucessful')
            sleep(5)
            return result.json()
        else:
            print('Returned error code:', result.status_code)
            sleep(20)
            print(f'Attempt {_try + 1} from {attempts}')
    else:
        print('All attempts have been exhausted.',
              'Perhaps the given url is currently unreachable. Try again later', sep='\n')
        return {}


def data_parser(val: 'dict | List[dict] | None'):
    if isinstance(val, dict) or val is None:
        return pd.Series(val, dtype='O')
    elif isinstance(val, list):
        return pd.Series(val[0] if val else val, dtype='O')
    else:
        return val


def persist_df(df: pd.DataFrame, table_name: str, db_name: str, *,
               index: bool = False, index_label: 'str | None' = None,
               if_exists: 'fail|replace|append' = 'append',
               attempts: int = 5):
    """Записывает DF в таблицу БД"""
    for _try in range(attempts):  # sqllite does not supports concurrent access
        try:
            with sqlite3.connect(db_name) as connection:
                while Path('hw1.db-journal').exists():  # await wile temp file in use
                    sleep(.5)
                df.to_sql(table_name, connection,
                          if_exists=if_exists,
                          index=index,
                          index_label=index_label,
                          method='multi')
                connection.commit()
                print(f'Данные записаны в БД в таблицу {table_name}. Записано',
                      len(df), 'строк(и)', end='\n\n')
                break
        except sqlite3.Error as ex:
            print(f"\tPersist failed cause {ex}")
            connection.rollback()
            print(f'\tAttempt {_try + 1} from {attempts}')
    connection.close()


def normalizer(txt: 'str | None') -> 'str | None':
    import re
    if not isinstance(txt, str):
        return None
    txt = txt.lower().strip().strip('.')
    txt = re.sub(r'\s?framework\s?', '', txt)
    if re.search('python', txt):
        return 'python'
    if re.search('rest|api', txt):
        return 'rest api'
    if re.search('fast', txt):
        return 'fast api'
    if re.search('django', txt):
        return 'django'
    if re.search('git', txt):
        return 'git'
    if re.search('sql', txt):
        return 'sql'
    if re.search('docker', txt):
        return 'docker'
    if txt in ('asyncio', 'aiohttp', 'asinc.io', 'асинхронное программирование'):
        return 'асинхронное программирование'
    if txt in ('go', 'golang'):
        return 'go'
    return txt