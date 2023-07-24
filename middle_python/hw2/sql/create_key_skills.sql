drop table if exists key_skills;
create table if not exists key_skills(
    id integer primary key,
    vacancy_id integer,
    name text,
    normalized_name text,
    FOREIGN KEY(vacancy_id) REFERENCES vacancies(id)
)