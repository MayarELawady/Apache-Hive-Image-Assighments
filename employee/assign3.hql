Create database assign3;
Use assign3;



create table events(
	artist string, auth string,
	firstName string, gender string,
	itemInSession string, lastName string,
	length string, level string,
	location string, method string,
	page string, registration string,
	sessionId string , song string ,
	status string , ts string,
	userAgent string , userId string)

ROW FORMAT serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde';

load data local inpath 'events.csv' into table events;

Select userId, sessionId,
first_value(song)OVER (PARTITION BY sessionId ORDER BY ts ROWS BETWEEN
     unbounded preceding and unbounded following) AS first_song, 
Last_value (song) OVER (PARTITION BY sessionId ORDER BY ts ROWS BETWEEN
unbounded preceding and unbounded following) AS last_song
FROM events;


Select x.userid, rank () over (order by x.songcount) as myrank
FROM(Select userId, count (distinct song)as songcount
From events 
Where page = 'NextSong'
Group by userid
) x
Order by myrank;


Select x.userid, row_number() over (order by x.songcount) as myrank
FROM(Select userId, count (distinct song)as songcount
From events 
Where page = 'NextSong'
Group by userid
) x
Order by myrank;


Select artist,location ,
count(distinct song) Over (Partition by artist,location) as cn_art_loc,
count(distinct song) Over (Partition by location) as cn_loc,
count(distinct song) Over () as cn_total
From events;


Select artist,location ,
count(distinct song) Over (Partition by artist,location) as cn_art_loc,
count(distinct song) Over (Partition by location) as cn_loc,
count(distinct song) Over (Partition by location) as cn_art,
count(distinct song) Over () as cn_total
From events;


Select x.userid, x.song, 
Lag (x.song) over (partition by x.userid order by x.ts) as prev_song,
Lead (x.song) over (partition by x.userid order by x.ts) as next_song
FROM
(SELECT * FROM events Where page = 'NextSong') x;


Select x.userid,x.song
from
(Select userid, song,ts  
From events
Order by userid,song,ts) x;


Select x.userid,x.song
from
(Select userid, song,ts  
From events
Cluster by userid,song,ts) x;
