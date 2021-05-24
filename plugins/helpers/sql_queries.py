class SqlQueries:

    # Create Staging Table to receive COVID19 Dataset
    create_table_staging_covid = ("""
        CREATE TABLE IF NOT EXISTS public.staging_covid (
            Datekey int4,
            Year int4,
            Month int4,
            Day int4,
            State varchar(256),
  			Country varchar(256),
			Date varchar(256),
  			Latitude float,
  			Longitude float,
  			Confirmed int4,
  			Deaths int4,
  			Recovered int4,
  			Active int4,
  			Incidence_Rate float,
  			CaseFatalityRatio float,
  			Combined_Key varchar(256)
        );
    """)

    # Create Dimension Table Date
    create_table_date = ("""
        CREATE TABLE "dim_date"(
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
    """)
    
    # Create Dimension Table Location
    create_table_location = ("""
        CREATE TABLE "dim_location"(
            "Combined_key" VARCHAR(255) NOT NULL,
            "State" VARCHAR(255),
            "Country" VARCHAR(255) NOT NULL,
            "Latitude" FLOAT,
            "Longitude" FLOAT,
            CONSTRAINT "pk_dim_location_combined_key" PRIMARY KEY ("Combined_key")
        );
    """)
    
    # Create Fact Table Covid Cases
    create_table_covid_cases = ("""
        CREATE TABLE "fact_covid_cases"(
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
    """)
    
    # Populate Dimension table Location
    location_table_insert = ("""
        SELECT DISTINCT combined_key,
				state, 
                country, 
                latitude, 
                longitude
        FROM staging_covid
        ORDER BY country, state, combined_key
    """)

    # Populate Dimension table Date
    time_table_insert = ("""
        SELECT DISTINCT cast(to_char(cast("date" as date), 'YYYYMMDD') as int) as Datekey,
				cast(to_char(cast("date" as date), 'YYYY') as int) as year,
                cast(to_char(cast("date" as date), 'MM') as int) as month,
                cast(to_char(cast("date" as date), 'DD') as int) as day,
                cast("date" as date) as "date",
                cast(date_part(quarter, cast("date" as date)) as int) as quarter_num,
                concat('Q', date_part(quarters, cast("date" as date)) )  as quarter,
                to_char(cast("date" as date), 'Month') as month_name,
				cast(date_part(week, cast("date" as date)) as int) as week,
                to_char(cast("date" as date), 'Day') as Weekday,
				cast(to_char(cast("date" as date), 'D') as int) as WeekdayNum
        FROM staging_covid
        ORDER BY cast("date" as date) ASC
    """)

    # Populate Fact table Covid Cases
    # INSERT INTO fact_covid_cases (combined_key, datekey, deaths, recovered, active, incidence_rate, case_fatality_ratio )
    covid_cases_insert = ("""
        SELECT 
            combined_key,
            cast(to_char(cast("date" as date), 'YYYYMMDD') as int) as Datekey,
            deaths,
            recovered,
            active,
            incidence_rate,
            casefatalityratio
        FROM staging_covid
        ORDER BY date ASC
    """)