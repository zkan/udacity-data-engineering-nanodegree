CREATE TABLE public.global_temperature (
    dt date,
    AverageTemperature decimal,
    AverageTemperatureUncertainty decimal,
    city varchar(256),
    country varchar(256),
    latitude varchar(256),
    longitude varchar(256)
);

CREATE TABLE public.worldbank (
    country_code varchar(256),
    country_name varchar(256),
    indicator_code varchar(256),
    indicator_name varchar(256),
    value decimal,
    year int4
);
