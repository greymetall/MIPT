import pandas as pd
from time import sleep
import json5
from utils import (areas_parser, get_query, execute, get_data_by_api,
                   data_parser, list_to_str, update_table, normalizer)


settings = json5.load(open('settings.json', encoding='utf-8'))

# globals().update(**settings)
db_name = settings['db_name']
num_vac = settings['num_vac']
url_vac = settings['url_vac']

url_areas = settings['url_areas']
country = settings['country']
regions = settings['regions']

url_params = settings['url_params']


areas_dct = areas_parser(url_areas, country, regions)  # получаем id заданных регионов
areas_lst = list(areas_dct)

url_params['area'] = areas_lst


def get_vacancies(url, params, vacancies=None):
    """Получает список вакансий по заданному URL с параметрами"""
    if vacancies is None:
        vacancies = []

    if params.get('page', 0) < params.get('pages', 1):
        data = get_data_by_api(url, params)
        _vacancies = data.get('items', [])
        found, page, pages = data.get('found', 0), data.get('page', 0), data.get('pages', 1)
        params.update(found=found, page=page + 1, pages=pages)
        print(f'\tНайдено: {found}, страниц: {pages}, страница: {page}')
        vacancies += _vacancies

        # Количество вакансий
        print('Количество спарсенных со стр', page,
              'вакансий по заданным параметрам:', len(_vacancies),
              'всего спарсено вакансий:', len(vacancies), end='\n\n')

        if len(vacancies) < (found if found < num_vac else num_vac) and page < pages:
            sleep(3)
            get_vacancies(url, params, vacancies)
    return vacancies


def attributes_processing(vacancies_df: pd.DataFrame):
    """Обработка DF вакансий"""
    # Парсим атрибуты, достаём названия у тех, которые имеют форму словарей/списков
    # и добавляем в DF вакансий
    print()
    for attrib in vacancies_df:
        if attrib in ('employer', 'salary'):
            continue
        print('\tParsing attribute', attrib)
        data = vacancies_df[attrib].apply(data_parser)
        if isinstance(data, pd.DataFrame):
            if 'name' in data:
                vacancies_df[attrib] = data['name']
        else:
            vacancies_df[attrib] = data
    print()


def employers_proccessing(vacancies_df: pd.DataFrame):
    # Парсим работодателей в DF для создания отдельной таблицы
    employers_df = vacancies_df.employer.apply(data_parser)
    # Убираем лишний атрибут logo_urls
    employers_df.drop(columns='logo_urls', inplace=True)

    vacancies_df[
        [
            'company_id',
            'company_name',
            'company_trusted'
        ]
    ] = employers_df[
        [
            'id',
            'name',
            'trusted'
        ]
    ]

    # vacancies_df = pd.concat([vacancies_df,
    #                          employers_df.add_prefix('company_')],
    #                         axis=1)
    # Убираем дубли
    employers_df = employers_df[~employers_df.duplicated()]
    # Запись DF в таблицу БД employers
    update_table('employers', db_name, employers_df)


def key_skills_processing(vacancies_df: pd.DataFrame):
    # Сздаём отдельный DF с ключевыми скиллами
    key_skills_df = vacancies_df[
        ['id', 'key_skills']
    ].rename(
        columns=dict(
            id='vacancy_id',
            key_skills='name'
        )
    )
    # Разбиваем ключевые скиллы, записанные через запятую на списки
    key_skills_df['name'] = key_skills_df.name.apply(lambda x: x.split(',')
                                                     if isinstance(x, str)
                                                     else x)
    # Растягиваем списки на атомарные значения после разбивки
    key_skills_df = key_skills_df.explode('name', ignore_index=True)
    # Нормализуем ключевые слова
    key_skills_df['normalized_name'] = key_skills_df.name.apply(normalizer)

    # Убираем дубли
    key_skills_df = key_skills_df[~key_skills_df.duplicated()]
    # Запись DF в таблицу БД key_skills
    update_table('key_skills', db_name, key_skills_df)


def vacancies_processing(df: pd.DataFrame = pd.DataFrame()):
    """Обработка списка вакансий и преобразование в таблицы"""
    print('\nПарсинг страницы', url_params.get('page'), end='\n\n')
    fill_vac = [{}] * len(df)
    vacancies = get_vacancies(url_vac, url_params, fill_vac)
    found, page, pages = (url_params.get('found', 0),
                          url_params.get('page', 0),
                          url_params.get('pages', 0))
    print(f'Спарсено {len(vacancies)} вакансий, с {page} страниц')

    # Парсим список вакансий в DF и оставляем только нужные атрибуты
    vacancy_attribs = ['id', 'name', 'url', 'alternate_url', 'employer', 'area',
                       'employment', 'salary', 'experience', 'professional_roles',
                       'published_at', 'created_at', 'archived']
    global vacancies_df
    vacancies_df = pd.DataFrame(vacancies)[vacancy_attribs]
    # очистка DF от добавленных пустых строк
    vacancies.clear()  # очистка массива со спарсенными ранее вакансиями
    vacancies_df = vacancies_df.dropna(how='all').reset_index(drop=True)
    vacancies_df.rename(columns=dict(name='position'), inplace=True)
    # Убираем архивные вакансии
    vacancies_df = vacancies_df[~vacancies_df.archived.fillna(False)]

    # Обрабатываем атрибуты
    attributes_processing(vacancies_df)

    # Записываем спарсенные компании в таблицу employers
    employers_proccessing(vacancies_df)

    # Парсим зарплаты
    salary_df = vacancies_df.salary.apply(data_parser)
    vacancies_df = pd.concat([vacancies_df,
                              salary_df.add_prefix('salary_')], axis=1)

    # Парсим дополнительные атрибуты по каждой вакансии по API и записываем в DF вакансий
    print('Парсинг детального описания каждой из вакансий\n')
    vacancies_df['details'] = vacancies_df.url.apply(get_data_by_api)
    details_df = vacancies_df.details.apply(data_parser)
    details_df = details_df[
        ['description',
         'key_skills']
    ].rename(
        columns=dict(
            description='job_description'
        )
    )

    vacancies_df = pd.concat([vacancies_df, details_df], axis=1)

    # Обработка атрибута key_skills
    vacancies_df['key_skills'] = vacancies_df.key_skills.apply(list_to_str)
    # Оставляем только те вакансии, в которых указаны ключевые навыки,
    # которые разместили проверенные работодатели,имеющие аккредитацию IT компании
    vacancies_df = vacancies_df[vacancies_df.company_trusted &
                                # vacancies_df.company_accredited_it_employer &
                                vacancies_df.key_skills.notna()]
    # убираем лишние атрибуты
    vacancies_attribs = [
        'id',
        'position',
        'job_description',
        'url',
        'alternate_url',
        'area',
        'employment',
        'experience',
        'professional_roles',
        'key_skills',
        'salary_from',
        'salary_to',
        'salary_currency',
        'salary_gross',
        'company_id',
        'company_name',
        'published_at',
        'created_at',
        'archived'
    ]
    vacancies_df = pd.concat([df, vacancies_df[vacancies_attribs]], ignore_index=True)
    # Убираем дубли
    vacancies_df = vacancies_df[~vacancies_df.id.duplicated()]
    print('\nРазмер массива вакансий после применённых фильтров:', len(vacancies_df))
    print()
    if len(vacancies_df) < (found if found < num_vac else num_vac) and page < pages:
        # Если количество вакансий после фильтров оказалось меньше 100,
        # и при этом найдено более 100 вакансий, то запускаем процесс заново
        sleep(10)  # ожидание чтобы не попасть на капчу
        vacancies_processing(vacancies_df.copy(deep=True))
        return
    print(f'Всего спарсено {len(vacancies_df)} и будет загружено в таблицу vacancies', end='\n\n')

    update_table('vacancies', db_name, vacancies_df)

    # Записываем спарсенные клчевые навыки в таблицу key_skills
    key_skills_processing(vacancies_df)

    # Очистка БД от мусора
    execute('vacuum', db_name)


def main():
    # Создаём таблицу employers в БД
    execute(get_query('create_employers.sql'), db_name)
    # Создаём таблицу vacancies в БД
    execute(get_query('create_vacancies.sql'), db_name)
    # Создаём таблицу key_skills в БД
    execute(get_query('create_key_skills.sql'), db_name)

    vacancies_processing()


if __name__ == '__main__':
    main()