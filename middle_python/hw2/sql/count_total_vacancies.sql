select count(distinct id) cnt_vac from vacancies v
join (select id company_id,
             name company_name,
             trusted,
             accredited_it_employer is_accredited
      from employers) e
  on coalesce(v.company_id, '') || coalesce(v.company_name, '') =
     coalesce(e.company_id, '') || coalesce(e.company_name, '')
 and e.trusted and not v.archived --and is_accredited
 and v.key_skills is not null