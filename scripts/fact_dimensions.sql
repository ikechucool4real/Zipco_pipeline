set search_path to dev;

create table if not exists dim_location (
  location_sk bigint generated always as identity primary key,
  state varchar(50) not null,
  city  text not null,
  postal_code varchar(20) not null,
  unique(state, city, postal_code)
);

create table if not exists dim_office (
    office_sk bigint generated always as identity primary key,
    office_name text not null,
    office_phone varchar(50),
    unique(office_name, office_phone)
);

create table if not exists dim_property (
    property_sk bigint generated always as identity primary key,
    property_type varchar(100) not null,
    bedrooms int,
    bathrooms int,
    unique(property_type, bedrooms, bathrooms)
);

create table if not exists dim_date (
    date_sk int primary key,
    calendar_date date not null,
    year int not null,
    quarter int not null,
    month_number int not null,
    month_name varchar(20) not null,
    day_of_month int not null,
    day_of_week int not null
);

create table if not exists fact_listings (
    listing_id text primary key,
    location_fk bigint references dim_location(location_sk),
    office_fk bigint references dim_office(office_sk),
    property_fk bigint references dim_property(property_sk),
    full_address text,
    latitude numeric,
    longitude numeric,
    listed_date_fk int references dim_date(date_sk),
    ingested_at timestamptz default now()
);

create index if not exists ix_fact_loc on fact_listings(location_fk);
create index if not exists ix_fact_off on fact_listings(office_fk);
create index if not exists ix_fact_prop on fact_listings(property_fk)