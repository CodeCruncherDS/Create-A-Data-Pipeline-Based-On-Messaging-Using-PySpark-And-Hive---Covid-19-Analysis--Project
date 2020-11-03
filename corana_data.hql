CREATE EXTERNAL TABLE IF NOT EXISTS corana(
`Global_new_confirmed` int,
`Global_new_deaths` int,
`Global_new_recovered` int,
`Global_total_confirmed` int,
`Global_total_deaths` int,
`Global_total_recovered` int,
`Country_code` string,
`Country_name` string,
`Country_new_deaths` int,
`Country_new_recovered` int,
`Country_newconfirmed` int,
`Country_slug` string,
`Country_total_confirmed` int,
`Country_total_deaths` int,
`Country_total_recovered` int,
`Extracted_timestamp` timestamp)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
location '/data/staging/corana_data';