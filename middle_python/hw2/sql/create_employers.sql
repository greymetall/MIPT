drop table if exists employers;
create table if not exists employers(
    id integer,
    name text,
    url text,
    alternate_url text,
    vacancies_url text,
    accredited_it_employer boolean,
    trusted boolean
)