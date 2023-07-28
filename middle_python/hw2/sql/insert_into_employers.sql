insert into employers(id,
                      name,
                      url,
                      alternate_url,
                      vacancies_url,
                      accredited_it_employer,
                      trusted)
values(:id,
       :name,
       :url,
       :alternate_url,
       :vacancies_url,
       :accredited_it_employer,
       :trusted)