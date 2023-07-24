delete from employers
where coalesce(id, '') || coalese(name, '') = coalesce(:id, '') || coalese(:name, '')