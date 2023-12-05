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
where v_id is not null;

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
drop column impr;

select count(*) from taxi where dist=0;-- order by pickup_dt desc;-- limit 1000;

select count (*) from taxi;

with sss as(
select 
  date(pickup_dt) as pickup_dt,
  --n_pass,
--  case
--    when n_pass > 3 then 4
--	else n_pass
--  end as n_p,
  sum(
  case
    when n_pass = 0 then 1
    else 0
  end) as n_0,
  sum(
  case
    when n_pass = 1 then 1
    else 0
  end) as n_1,
  sum(
  case
    when n_pass = 2 then 1
    else 0
  end) as n_2,
  sum(
  case
    when n_pass = 3 then 1
    else 0
  end) as n_3,
  sum(
  case
    when n_pass > 3 then 1
    else 0
  end) as n_4,
  max(
  case
    when n_pass = 3 then total
    else NULL
  end) as max_3
from taxi
group by date(pickup_dt)--, n_p
order by date(pickup_dt)

)select *, round(n_3*100.0/(n_0 + n_1 + n_2 + n_3 + n_4),2) as p_3 from sss