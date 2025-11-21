select * from actor_films limit 50;

-- not using cascade here as we delete dependency
drop table if exists actors cascade;
drop type if exists film_info cascade;
drop type if exists quality_class cascade;
drop table if exists actors_history_scd cascade;

create type film_info as (
film TEXT,
votes INT,
rating REAL,
filmid TEXT
);

create type quality_class as enum (
'star', -- >8
'good', -- >7 && <=8
'average', -- >6 && <= 7
'bad' -- <=6
);

create table actors (
actorid TEXT,
films film_info[],
quality_class quality_class NOT NULL,
is_active BOOLEAN,
primary key (actorid)
);

select * from actor_films order by year asc limit 1;
-- 1970
select * from actor_films order by year desc limit 1;
-- 2021

insert into actors 
with 
	prev_year as (select * from actors),
	-- curr_year as (select actorid, film, votes, rating, filmid, year from actor_films where year = 1971)
	curr_year as (select actorid, 
						array_agg(row(film,votes,rating,filmid)::film_info order by film) as films,
						avg(rating) as rating,
						max(year) as year
				  from actor_films
				  where year = 1971
				  group by actorid )
select				  				
	coalesce(c.actorid, p.actorid) as actorid,

	case
		when p.films is null and c.actorid is not null then 
			c.films
		when c.year is not null then 
			p.films || c.films
		else
			p.films
	end as films,

	case 
		when c.rating is not null then
			case 
				when c.rating >8 then 'star'
				when c.rating > 7 then 'good'
				when c.rating > 6 then 'average'
				else 'bad'
		end ::quality_class
		else 
			p.quality_class
	end as quality_class,

	case 
		when c.year is not null then TRUE
		else p.is_active
	end as is_active
	
from curr_year c
full outer join prev_year p on p.actorid = c.actorid;

select * from actors;

DO $$
DECLARE 
    y INT;
BEGIN
    FOR y IN 1970..2021 LOOP
		insert into actors 
		with 
			prev_year as (select * from actors),
			-- curr_year as (select actorid, film, votes, rating, filmid, year from actor_films where year = 1971)
			curr_year as (select actorid, 
								array_agg(row(film,votes,rating,filmid)::film_info order by film) as films,
								avg(rating) as rating,
								max(year) as year
						  from actor_films
						  where year = y
						  group by actorid )
		select				  				
			coalesce(c.actorid, p.actorid) as actorid,
		
			case
				when p.films is null and c.actorid is not null then 
					c.films
				when c.year is not null then 
					p.films || c.films
				else
					p.films
			end as films,
		
			case 
				when c.rating is not null then
					case 
						when c.rating >8 then 'star'
						when c.rating > 7 then 'good'
						when c.rating > 6 then 'average'
						else 'bad'
				end ::quality_class
				else 
					p.quality_class
			end as quality_class,
		
			case 
				when c.year is not null then TRUE
				else p.is_active
			end as is_active
			
		from curr_year c
		full outer join prev_year p on p.actorid = c.actorid
		
		ON CONFLICT (actorid) DO UPDATE
		SET
	    films = EXCLUDED.films,
	    quality_class = EXCLUDED.quality_class,
	    is_active = EXCLUDED.is_active;
	end loop;
end;
$$;	

select * from actors limit 1;


-- Create SCD Type 2 history table
CREATE TABLE actors_history_scd (
    actorid TEXT NOT NULL,
    quality_class quality_class NOT NULL,
    is_active BOOLEAN NOT NULL,
    start_date INT NOT NULL,
    end_date INT,
    PRIMARY KEY (actorid, start_date)
);

-- Example PL/pgSQL loop to populate the history table for years 1970–2021
DO $$
DECLARE
    y INT;
BEGIN
    FOR y IN 1970..2021 LOOP

        -- Step 1: expire old rows if quality_class changed
        UPDATE actors_history_scd a
        SET end_date = y,
            is_active = FALSE
        FROM (
            SELECT actorid,
                   CASE
                       WHEN avg(rating) > 8 THEN 'star'
                       WHEN avg(rating) > 7 THEN 'good'
                       WHEN avg(rating) > 6 THEN 'average'
                       ELSE 'bad'
                   END::quality_class AS quality_class,
                   TRUE AS is_active
            FROM actor_films
            WHERE year = y
            GROUP BY actorid
        ) AS curr_year
        WHERE a.actorid = curr_year.actorid
          AND a.is_active = TRUE
          AND a.quality_class IS DISTINCT FROM curr_year.quality_class;

        -- Step 2: insert new rows only if there is no active row
        INSERT INTO actors_history_scd (actorid, quality_class, is_active, start_date, end_date)
        SELECT c.actorid, c.quality_class, c.is_active, y, NULL
        FROM (
            SELECT actorid,
                   CASE
                       WHEN avg(rating) > 8 THEN 'star'
                       WHEN avg(rating) > 7 THEN 'good'
                       WHEN avg(rating) > 6 THEN 'average'
                       ELSE 'bad'
                   END::quality_class AS quality_class,
                   TRUE AS is_active
            FROM actor_films
            WHERE year = y
            GROUP BY actorid
        ) AS c
        LEFT JOIN actors_history_scd a
               ON a.actorid = c.actorid AND a.is_active = TRUE
        WHERE a.actorid IS NULL;  -- insert only if no active row

    END LOOP;
END $$;

-- Verify results
SELECT * FROM actors_history_scd
ORDER BY start_date desc;
