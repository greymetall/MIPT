drop table if exists vacancies;
create table if not exists vacancies(
    id integer primary key,
    position text,
    job_description text,
    url text,
    alternate_url text,
    area text,
    employment text,
    experience text,
    professional_roles text,
    key_skills text,
    salary_from numeric,
    salary_to numeric,
    salary_currency text,
    salary_gross boolean,
    company_id integer,
    company_name text,
    published_at timestamptz,
    created_at timestamptz,
    archived boolean,
    FOREIGN KEY(company_id) REFERENCES employers(id),
    FOREIGN KEY(company_name) REFERENCES employers(name)
)