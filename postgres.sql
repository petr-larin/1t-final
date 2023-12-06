drop table if exists taxi;

create table if not exists taxi(
  v_id int,
  pickup_dt timestamp,
  dropoff_dt timestamp,
  n_pass int,
  dist float,
  rate int,
  store char,
  puloc int,
  doloc int,
  paymt int,
  fare float,
  extra float,
  mta_tax float,
  tip float,
  tolls float,
  impr float,
  total float,
  cong float
);

copy public.taxi
from '/docker-entrypoint-initdb.d/yellow_tripdata_2020-01.csv'
(format csv, header)
where v_id is not null                   -- Неинформативные строки с 5 NULL в каждой, включая кол-во пассажиров.
  and date(pickup_dt) <= '2020-01-31'    -- Поездка либо начинается либо заканчивается 
  and date(dropoff_dt) >= '2020-01-01'   --   в январе 2020 либо и то и другое.
  and total > 0;                         -- Отсеиваем "странные" поездки со стоимостью <= 0; можно
                                         --   этого не делать, но тогда колонки MIN неинформативны.                    

alter table taxi
drop column v_id,
drop column dropoff_dt,
drop column rate,
drop column store,
drop column puloc,
drop column doloc,
drop column paymt,
drop column fare,
drop column extra,
drop column mta_tax,
drop column tolls,
drop column impr,
drop column cong;

select count (*) from taxi;

with raw as(
select 
  date(pickup_dt) as pickup_dt,
  sum(case when n_pass = 0 then 1 else 0 end) as n_0,
  sum(case when n_pass = 1 then 1 else 0 end) as n_1,
  sum(case when n_pass = 2 then 1 else 0 end) as n_2,
  sum(case when n_pass = 3 then 1 else 0 end) as n_3,
  sum(case when n_pass > 3 then 1 else 0 end) as n_4,
  min(case when n_pass = 0 then total else NULL end) as min_0,
  max(case when n_pass = 0 then total else NULL end) as max_0,
  min(case when n_pass = 1 then total else NULL end) as min_1,
  max(case when n_pass = 1 then total else NULL end) as max_1,
  min(case when n_pass = 2 then total else NULL end) as min_2,
  max(case when n_pass = 2 then total else NULL end) as max_2,
  min(case when n_pass = 3 then total else NULL end) as min_3,
  max(case when n_pass = 3 then total else NULL end) as max_3,
  min(case when n_pass > 3 then total else NULL end) as min_4,
  max(case when n_pass > 3 then total else NULL end) as max_4
from taxi
group by date(pickup_dt)
order by date(pickup_dt)
)
select
  date(pickup_dt) as "date",
  round(n_0 * 100.0/(n_0 + n_1 + n_2 + n_3 + n_4), 2) as percent_0,
  round(n_1 * 100.0/(n_0 + n_1 + n_2 + n_3 + n_4), 2) as percent_1,
  round(n_2 * 100.0/(n_0 + n_1 + n_2 + n_3 + n_4), 2) as percent_2,
  round(n_3 * 100.0/(n_0 + n_1 + n_2 + n_3 + n_4), 2) as percent_3,
  round(n_4 * 100.0/(n_0 + n_1 + n_2 + n_3 + n_4), 2) as "percent_4+",
  min_0, max_0, min_1, max_1, min_2, max_2, min_3, max_3, min_4 as "min_4+", max_4 as "max_4+"
from raw