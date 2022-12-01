select count(ss.product_name) as count, ss.product_name, gu.state
from `silver.sales` ss
inner join `gold.user_profiles_enriched` gu 
  on ss.client_id = gu.client_id
where 
  ss.purchase_date between '2022-09-01' and '2022-09-10'
  and ss.product_name='TV'
  and DATE_DIFF(ss.purchase_date, gu.birth_date, YEAR) - 
    IF(EXTRACT(MONTH FROM gu.birth_date)*100 + EXTRACT(DAY FROM gu.birth_date) > EXTRACT(MONTH FROM ss.purchase_date)*100 + EXTRACT(DAY FROM ss.purchase_date),1,0) between 20 and 30
group by ss.product_name, gu.state
order by count desc
limit 1

-- calculating precise age is not overhead because it may be a target of legal regulation
