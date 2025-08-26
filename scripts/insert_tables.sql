set search_path to dev;

--Location Dimension
insert into dim_location(state, city, postal_code)
select distinct state, city, postal_code
from transformed_listing
where state is not null and city is not null and postal_code is not null
on conflict(state, city, postal_code) do nothing;

--Office dimension
insert into dim_office(office_name, office_phone)
select distinct listing_office_name, listing_office_phone
from transformed_listing
where listing_office_name is not null
on conflict(office_name,office_phone) do nothing;

-- Property dimension 
insert into dim_property(property_type, bedrooms, bathrooms)
select distinct property_type, bedrooms, bathrooms
from transformed_listing
on conflict(property_type, bedrooms, bathrooms) do nothing;

-- Date dimension 
with bounds as (
    select
        min(listed_date::date)as min_date,
        max(listed_date::date)as max_date
    from transformed_listing
),
date_series as (
    select generate_series (
        (select min_date from bounds),
        (select max_date from bounds),
        interval '1 day'
    )::date as calendar_date
    from bounds
)
insert into dim_date(
    date_sk, calendar_date, year, quarter,
    month_number, month_name, day_of_month, day_of_week
)
select 
    to_char(calendar_date, 'YYYYMMDD')::int as date_sk,
    calendar_date,
    extract(year from calendar_date)::int as year,
    extract(quarter from calendar_date)::int as quarter,
    extract(month from calendar_date)::int as month_number,
    to_char(calendar_date, 'FMMonth') as month_name,
    extract(day from calendar_date)::int as day_of_month,
    extract(isodow from calendar_date)::int as day_of_week
from date_series
on conflict(date_sk) do nothing;

--Build the fact table
insert into fact_listings(
    listing_id, location_fk, office_fk, property_fk,
    full_address, latitude, longitude, listed_date_fk
)
select 
    l.id,
    dloc.location_sk,
    doff.office_sk,
    dprop.property_sk,
    l.full_address,
    l.latitude,
    l.longitude,
    dd.date_sk as listed_date_fk
from transformed_listing l
left join dim_location dloc on dloc.state = l.state 
and dloc.city = l.city and dloc.postal_code = l.postal_code::varchar(20)
left join dim_office doff on doff.office_name = l.listing_office_name
left join dim_property dprop on dprop.property_type = l.property_type
left join dim_date dd on dd.date_sk = to_char(l.listed_date::date, 'YYYYMMDD')::int
on conflict(listing_id) do nothing;