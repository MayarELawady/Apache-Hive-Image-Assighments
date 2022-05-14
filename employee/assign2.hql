create database IF NOT EXISTS assign2;
use assign2;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

create table IF NOT EXISTS songs_partitioned (
artist_id string,
artist_latitude string,
artist_location string,
artist_longitude string,
duration double,
num_song int,
song_id string,
title string)
partitioned by (artist_name string, year string)
ROW FORMAT serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde';


!hadoop fs -put songs.csv hdfs://namenode:8020/Datafromlocal;


Alter table songs_partitioned add partition (artist_name ='The Box Tops', year ='1969' )
Location 'hdfs://namenode:8020/Songs_table_partitions';


load data inpath '/Datafromlocal/songs.csv' into table songs_partitioned partition (artist_name ='The Box Tops', year ='1969' );


!hadoop fs -ls hdfs://namenode:8020/Songs_table_partitions/;


create table IF NOT EXISTS songs (artist_id string,
artist_latitude string,
artist_location string,
artist_longitude string,
artist_name string,
duration double,
num_song int,
song_id string,
title string,
year string)
ROW FORMAT serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde';


From songs src
	Insert overwrite table songs_partitioned
	partition (artist_name, year)
	select src.artist_id, src.artist_latitude, src.artist_location,
	src.artist_longitude, src.duration, src.num_song, src.song_id,
	src.title, src.artist_name, src.year;


truncate table songs_partitioned;


!hadoop fs -put songs.csv hdfs://namenode:8020/Datafromlocal;

load data inpath '/Datafromlocal/songs.csv' into table songs;

From songs src
	Insert overwrite table songs_partitioned
	partition (artist_name, year)
	select src.artist_id, src.artist_latitude, src.artist_location,
	src.artist_longitude, src.duration, src.num_song, src.song_id,
	src.title, src.artist_name, src.year;


Truncate table songs_partitioned;


Drop table songs_partitioned;
create table songs_partitioned (
artist_id string,
artist_latitude string,
artist_location string,
artist_longitude string,
duration double,
num_song int,
song_id string,
title string)
partitioned by (year string, artist_name string)
ROW FORMAT serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde';


From songs src
	Insert overwrite table songs_partitioned
	partition (year='1969',artist_name)
	select src.artist_id, src.artist_latitude, src.artist_location,
	src.artist_longitude, src.duration, src.num_song, src.song_id,
	src.title, src.artist_name where year='1969'

Insert overwrite table songs_partitioned
	partition (year='2008',artist_name)
	select src.artist_id, src.artist_latitude, src.artist_location,
	src.artist_longitude, src.duration, src.num_song, src.song_id,
	src.title, src.artist_name where year='2008'

Insert overwrite table songs_partitioned
	partition (year='2003',artist_name)
	select src.artist_id, src.artist_latitude, src.artist_location,
	src.artist_longitude, src.duration, src.num_song, src.song_id,	src.title, src.artist_name where year='2003';


create table avrosongs
stored as avro
as select * from songs;

create table parquetsongs
stored as parquet
as select * from songs;


