delete from employers
where coalesce(id, '') || coalesce(name, '') = coalesce(cast(:id as integer), '') || coalesce(:name, '')