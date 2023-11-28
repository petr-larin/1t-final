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

delete from taxi where v_id is NULL;

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

select * from taxi limit 10;

select count (*) from taxi;
