CREATE TABLE IF NOT EXISTS public.staging_covid (
    "Datekey" INT4,
    "Year" INT4,
    "Month" INT4,
    "Day" INT4,
    "State" VARCHAR(256),
    "Country" VARCHAR(256),
    "Date" VARCHAR(256),
    "Latitude" float,
    "Longitude" float,
    "Confirmed" INT4,
    "Deaths" INT4,
    "Recovered" INT4,
    "Active" INT4,
    "Incidence_Rate" FLOAT,
    "CaseFatalityRatio" FLOAT,
    "Combined_Key" VARCHAR(256)
);

CREATE TABLE IF NOT EXISTS public.dim_date (
    "Datekey" INT4 NOT NULL,
    "Year" INT4,
    "Month" INT4,
    "Day" INT4,
    "Date" Date,
    "Quarter_num" INT4,
    "Quarter" VARCHAR(255),
    "Month_name" VARCHAR(255),
    "Week" INT4,
    "Weekday" VARCHAR(255),
    "Weekday_num" INT4,
    CONSTRAINT "pk_dim_date_datekey" PRIMARY KEY ("Datekey")
);

CREATE TABLE IF NOT EXISTS public.dim_location (
    "Combined_key" VARCHAR(255) NOT NULL,
    "State" VARCHAR(255),
    "Country" VARCHAR(255) NOT NULL,
    "Latitude" FLOAT,
    "Longitude" FLOAT,
    CONSTRAINT "pk_dim_location_combined_key" PRIMARY KEY ("Combined_key")
);

CREATE TABLE IF NOT EXISTS public.fact_covid_cases (
    "id" bigint IDENTITY(1, 1),
    "Combined_key" VARCHAR(255) NOT NULL,
    "Datekey" INT4 NOT NULL,
    "Deaths" INT4,
    "Recovered" INT4,
    "Active" INT4,
    "Incidence_rate" FLOAT,
    "Case_fatality_ratio" FLOAT,
    "New_deaths" INT4,
    "New_recovered" INT4,
    CONSTRAINT "pk_fact_covid_cases_id" PRIMARY KEY ("id"),
    CONSTRAINT "fk_fact_covid_cases_combined_key_foreign" FOREIGN KEY ("Combined_key") REFERENCES "dim_location"("Combined_key"),
    CONSTRAINT "fk_fact_covid_cases_datekey_foreign" FOREIGN KEY ("Datekey") REFERENCES "dim_date"("Datekey")
);
    