import zipfile
import sqlite3
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, wait
from time import sleep
from typing import List, Tuple
from zipfile import ZipFile


def unpacker(path: str, files: List[str], batch_size: int) -> List[pd.DataFrame]:
    """Распаковщик данных из zip-архива и парсер JSON"""

    def parser_json(zipobj: ZipFile, json_file: str) -> Tuple[str, pd.DataFrame]:
        """Парсит один файл JSON"""
        with zipobj.open(json_file) as file:
            return (json_file, pd.read_json(file))

    with zipfile.ZipFile(path, 'r') as zipobj:
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            data_sets = []
            futures = []
            current_slice = files

            count = 0
            with open('logging.log', 'a') as log:
                while current_slice:
                    for i in range(batch_size):
                        if i < len(current_slice):
                            futures.append(executor.submit(parser_json, zipobj, current_slice[i]))
                    success_futures, _ = wait(futures)
                    for future in success_futures:
                        filename, df = future.result()
                        count += 3
                        print(f"\t\tFile {filename} loaded ({count})", file=log, flush=True)
                        data_sets.append(df)
                    yield data_sets
                    data_sets = []
                    futures = []
                    current_slice = current_slice[batch_size:]


def parser_data(value: dict) -> dict:
    # Получаем значения КодОКВЭД и НаимОКВЭД. Если нет, устанавливаем значения в None
    main_val = dict(code_okved=value.get('СвОКВЭД', {}).get('СвОКВЭДОсн', {}).get('КодОКВЭД', None),
                    name_okved=value.get('СвОКВЭД', {}).get('СвОКВЭДОсн', {}).get('НаимОКВЭД', None),
                    type_okved='Осн')
    # Фильтруем по 61 ОКВЭД
    code = main_val['code_okved']
    return main_val if code and code.startswith('61') else None


def persist_df(df: pd.DataFrame):
    while True:  # sqllite does not supports concurrent access
        try:
            with sqlite3.connect('hw1.db') as connection:
                while Path('hw1.db-journal').exists():  # await wile temp file in use
                    sleep(.5)
                df.to_sql('telecom_companiesokved', connection,
                          if_exists='append',
                          index=False,
                          method='multi')
                connection.commit()
                break
        except sqlite3.Error as ex:
            with open('logging.log', 'a') as log:
                print(f"\tPersist failed cause {ex}", file=log, flush=True)
        sleep(.5)  # each retry within 500 ms


def process_df(path: str, files: List[str], batch_size: int):
    provider = unpacker(path, files, batch_size)
    try:
        for batch in provider:
            for df in batch:
                """Парсер и обработчик данных"""
                # Парсим атрибут data и записываем данные в общий DF
                df = df.assign(data=df.data.apply(parser_data))
                # Исключаем пустые данные (т.е. те, что не попали под ОКВЭД 61)
                df = df[df.data.notna()]
                # Разбиваем атрибут data на столбцы, и добавляем их в общий DF
                persist_df(df[['ogrn', 'inn', 'kpp', 'name', 'full_name']].join(df.data.apply(pd.Series)))
        return 0
    except Exception as ex:
        with open('logging.log', 'a') as log:
            print(f"Processing fails {ex}", file=log, flush=True)
        return str(ex)
