select normalized_name, count(normalized_name) counts
from key_skills
group by normalized_name
order by counts desc
limit 10