class SqlQueries:

    stg_airport_table_create = (
    """CREATE TABLE IF NOT EXISTS public._stg_airport (
            airport_code VARCHAR
            ,type VARCHAR
            ,country_code VARCHAR
            ,name VARCHAR
            ,iso_region VARCHAR
            ,region VARCHAR
            ,continent VARCHAR)
    """)

    stg_temperature_table_create = (
    """CREATE TABLE IF NOT EXISTS public._stg_temperature (
            date DATE
            ,average_temperature NUMERIC
            ,latitude VARCHAR
            ,longitude VARCHAR
            ,country VARCHAR
            ,city VARCHAR)
    """)

    stg_immigration_table_create = (
    """CREATE TABLE IF NOT EXISTS public._stg_immigration (
            visitor_id NUMERIC
            ,immigration_id NUMERIC
            ,age NUMERIC
            ,year NUMERIC
            ,month NUMERIC
            ,gender VARCHAR
            ,city VARCHAR
            ,country NUMERIC
            ,port_of_entry NUMERIC
            ,arrival_date NUMERIC
            ,departure_date NUMERIC
            ,address_code VARCHAR
            ,airline VARCHAR
            ,visa_code NUMERIC
            ,visa_type VARCHAR)
    """
    )



    fc_immigration_table_create = (
    """CREATE TABLE IF NOT EXISTS public._fc_immigration (
            immigration_id NUMERIC
            ,admission_nbr NUMERIC
            ,arrival_date VARCHAR 
            ,airport_code VARCHAR
            ,city VARCHAR
            ,month VARCHAR 
            ,year VARCHAR)
    """)

    dm_immigrant_table_create = (
    """CREATE TABLE IF NOT EXISTS public._dm_immigrant (
            admission_nbr NUMERIC
            ,country NUMERIC
            ,city VARCHAR
            ,age NUMERIC
            ,year NUMERIC
            ,month NUMERIC
            ,gender VARCHAR
            ,arrival_date DATE 
            ,departure_date NUMERIC
            ,airline VARCHAR)
    """)

    dm_time_table_create = (
    """CREATE TABLE IF NOT EXISTS public._dm_time (
            arrival_date DATE 
            ,hour NUMERIC
            ,day VARCHAR
            ,week NUMERIC
            ,month NUMERIC
            ,year NUMERIC
            ,weekday VARCHAR)
    """)


    dm_temperature_table_create = (
    """CREATE TABLE IF NOT EXISTS public._dm_temperature (
            city VARCHAR
            ,date VARCHAR
            ,temperature VARCHAR 
            ,country VARCHAR
            ,year VARCHAR)
    """)
   
    dm_airport_table_create = (
    """CREATE TABLE IF NOT EXISTS public._dm_airport(
            airport_code VARCHAR
            ,airport_name VARCHAR
            ,country VARCHAR
            ,city VARCHAR
            ,state VARCHAR)
    """)

    fc_immigration_table_insert = (
    """SELECT 
            DISTINCT _stg_im.immigration_id as admission_nbr
            ,_stg_im.arrival_date as arrival_arrival
            ,_stg_airport.airport_code 
            ,_stg_im.city
            ,_stg_im.month
            ,_stg_im.year
        FROM _stg_immigration _stg_im
        LEFT JOIN _stg_airport _stg_airport
            ON LOWER(_stg_im.country) = LOWER(_stg_airport.country_code)
    """)

    dm_time_table_insert = (
    """SELECT 
            TO_DATE(CAST(date as text), 'YYYY-MM-DD') as arrival_date 
            ,extract(hour FROM date) as hour
            ,extract(day FROM date) as day
            ,extract(week FROM date) as week
            ,extract(month FROM date) as month
            ,extract(year FROM date) as year
            ,extract(dow FROM date) as dow
        FROM public._stg_temperature
        UNION 
        SELECT 
            TO_DATE(CAST(arrival_date as text), 'YYYY-MM-DD') as arrival_date 
            ,extract(hour FROM TO_DATE(CAST(arrival_date as text), 'YYYY-MM-DD')) as hour 
            ,extract(day FROM TO_DATE(CAST(arrival_date as text), 'YYYY-MM-DD')) as day 
            ,extract(week FROM TO_DATE(CAST(arrival_date as text), 'YYYY-MM-DD')) as week 
            ,extract(month FROM TO_DATE(CAST(arrival_date as text), 'YYYY-MM-DD')) as month 
            ,extract(year FROM TO_DATE(CAST(arrival_date as text), 'YYYY-MM-DD')) as year 
            ,extract(dow FROM TO_DATE(CAST(arrival_date as text), 'YYYY-MM-DD')) as dow 
        FROM public._stg_immigration
    """
    )

    dm_temperature_table_insert = (
    """SELECT 
            DISTINCT UPPER(city)
            date
            ,average_temperature as temperature
            ,country
            ,year
        FROM _stg_temperature
    """)

    dm_immigrant_table_insert = (
    """SELECT 
            DISTINCT immigration_id as admission_nbr
            ,country
            ,city
            ,age
            ,year
            ,month
            ,gender
            ,TO_DATE(CAST(arrival_date as text), 'YYYY-MM-DD') as arrival_date 
            ,departure_date 
            ,airline
        FROM _stg_immigration
    """)

    dm_airport_table_insert = (
    """SELECT 
        DISTINCT airport_code
        ,name as airport_name
        ,country_code as country
        ,'' as city
        ,region as state 
        FROM _stg_airport
    """)

    count_query = ("""SELECT DISTINCT(COUNT({})) FROM {}""")