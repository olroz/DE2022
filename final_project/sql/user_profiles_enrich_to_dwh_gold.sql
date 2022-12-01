TRUNCATE TABLE `{{ params.project_id }}.gold.user_profiles_enriched`
;

INSERT `{{ params.project_id }}.gold.user_profiles_enriched` (
    client_id,
    first_name,
    last_name,
    email,
    state,
    phone_number,
    birth_date
)
SELECT
  sc.client_id,
  coalesce(sc.first_name, SPLIT(sp.full_name, ' ')[safe_ordinal(1)]) as first_name,
  coalesce(sc.last_name, SPLIT(sp.full_name, ' ')[safe_ordinal(2)]) as last_name,
  coalesce(sc.email, sp.email) as email,
  coalesce(sc.state, sp.state) as state,
  sp.phone_number,
  sp.birth_date

FROM `{{ params.project_id }}.silver.customers` sc
LEFT JOIN `{{ params.project_id }}.silver.profiles` sp ON sc.email = sp.email
;
