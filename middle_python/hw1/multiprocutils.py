import zipfile
# import sqlite3
import pandas as pd
from tqdm import tqdm
from multiprocessing.pool import ThreadPool as Pool
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Iterable
from pathlib import Path


def unpacker(path: 'str | Path', files: List[str], max_workers: int) -> pd.DataFrame:
    """Распаковщик данных из zip-архива и парсер JSON"""

    def parser_json(json_file: str) -> pd.DataFrame:
        """Парсит один файл JSON"""
        with zipobj.open(json_file) as file:
            return pd.read_json(file)

    with zipfile.ZipFile(path, 'r') as zipobj:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # загружаем и отмечаем каждый будущий результат своим файлом
            future_to_file = {executor.submit(parser_json, file): file for file in files}
            for future in tqdm(as_completed(future_to_file)):
                file = future_to_file[future]
                try:
                    # data = parser_json(file)
                    data = future.result()
                except Exception as exc:
                    print(f'{file} сгенерировано исключение: {exc}')
                else:
                    yield data


def parser_data(value: dict) -> pd.DataFrame:
    """Парсер атрибута data"""
    # Получаем значения КодОКВЭД и НаимОКВЭД. Если нет, устанавливаем значения в None
    main_val = dict(code_okved=value.get('СвОКВЭД', {}).get('СвОКВЭДОсн', {}).get('КодОКВЭД'),
                    name_okved=value.get('СвОКВЭД', {}).get('СвОКВЭДОсн', {}).get('НаимОКВЭД'),
                    type_okved='Осн')
    # Фильтруем по 61 ОКВЭД
    code = main_val['code_okved']
    return main_val if code and code.startswith('61') else None


def processor_df(df: pd.DataFrame) -> pd.DataFrame:
    """Парсер и обработчик данных"""
    # Парсим атрибут data и записываем данные в общий DF
    df = df.assign(data=df.data.apply(parser_data))
    # Исключаем пустые данные (т.е. те, что не попали под ОКВЭД 61)
    df = df[df.data.notna()]
    # Разбиваем атрибут data на столбцы, и добавляем их в общий DF
    cols = ['ogrn', 'inn', 'kpp', 'name', 'full_name']
    return df[cols].join(df.data.apply(pd.Series))


def mulitproc(iterable: Iterable, nproc: int, chunksize: int):
    """Парсит данные из JSON файлов в DF в параллельном режиме"""
    with Pool(processes=nproc) as process_pool:
        dfs = process_pool.map(processor_df, iterable, chunksize)
    return dfs
