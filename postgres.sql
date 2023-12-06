drop table if exists taxi;

create table taxi(
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
from '/input_data/yellow_tripdata_2020-01.csv'
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

drop table if exists data_mart;
create table data_mart as
with raw as(
select 
  date(pickup_dt) as pickup_dt,
  sum(case when n_pass = 0 then 1 else 0 end) as n_0,
  sum(case when n_pass = 1 then 1 else 0 end) as n_1,
  sum(case when n_pass = 2 then 1 else 0 end) as n_2,
  sum(case when n_pass = 3 then 1 else 0 end) as n_3,
  sum(case when n_pass > 3 then 1 else 0 end) as n_4,
  min(case when n_pass = 0 then total else NULL end) as min_0p,
  max(case when n_pass = 0 then total else NULL end) as max_0p,
  min(case when n_pass = 1 then total else NULL end) as min_1p,
  max(case when n_pass = 1 then total else NULL end) as max_1p,
  min(case when n_pass = 2 then total else NULL end) as min_2p,
  max(case when n_pass = 2 then total else NULL end) as max_2p,
  min(case when n_pass = 3 then total else NULL end) as min_3p,
  max(case when n_pass = 3 then total else NULL end) as max_3p,
  min(case when n_pass > 3 then total else NULL end) as min_4p,
  max(case when n_pass > 3 then total else NULL end) as max_4p
from taxi
group by date(pickup_dt)
)
select
  date(pickup_dt) as pickup_date,
  round(n_0 * 100.0/(n_0 + n_1 + n_2 + n_3 + n_4), 2) as per_0p,
  round(n_1 * 100.0/(n_0 + n_1 + n_2 + n_3 + n_4), 2) as per_1p,
  round(n_2 * 100.0/(n_0 + n_1 + n_2 + n_3 + n_4), 2) as per_2p,
  round(n_3 * 100.0/(n_0 + n_1 + n_2 + n_3 + n_4), 2) as per_3p,
  round(n_4 * 100.0/(n_0 + n_1 + n_2 + n_3 + n_4), 2) as "per_4p+",
  min_0p, max_0p, min_1p, max_1p, min_2p, max_2p, min_3p, max_3p, min_4p as "min_4p+", max_4p as "max_4p+"
from raw
order by pickup_date;

copy data_mart
to '/output_data_pg/result_pg.csv'
(format csv, header);

select * from data_mart;