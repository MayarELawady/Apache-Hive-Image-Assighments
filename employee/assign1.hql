Create database IF NOT EXISTS assign1;

!hadoop fs -ls /user/hive/warehouse;

create database IF NOT EXISTS assign1_loc location '/hp_db/assign1_loc';

Use assign1;

Create table IF NOT EXISTS assign1_intern_tab (id int, name string, age int, city string)
Row format delimited
Fields terminated by ','
Stored as textfile;


load data local inpath 'employee_details.txt' into table assign1_intern_tab;

Use assign1_loc;

Create external table IF NOT EXISTS assign1_intern_tab (id int, name string, age int, city string)
Row format delimited
Fields terminated by ','
Stored as textfile;


!hadoop fs -put employee.csv hdfs://namenode:8020/Datafromlocal;

Create table IF NOT EXISTS staging (id int, name string, age int, city string)
Row format delimited
fields terminated by ','
stored as textfile;


load data local inpath 'employee_details.txt' into table staging;

Use assign1_loc;
insert into assign1_intern_tab select * from assign1_loc.staging;

use assign1;
insert into assign1_intern_tab select * from assign1_loc.staging;


! wc -l songs.csv;

create table IF NOT EXISTS  songs (artist_id int, artist_lat string, asrtist_loc string, artist_long string, duration string, num_song int, song_id string, title string, year int)
row format delimited
fields terminated by ','
stored as textfile;

load data local inpath 'songs.csv' into table songs;

select * from songs;

select count(*) from songs;


create external table IF NOT EXISTS  songs_extern (artist_id int, artist_lat string, asrtist_loc string, artist_long string, duration string, num_song int, song_id string, title string, year int)

row format delimited

fields terminated by ','

stored as textfile

location 'hdfs://namenode:8020/Datafromlocal';


!hadoop fs -put songs.csv hdfs://namenode:8020/Datafromlocal;
Load data inpath 'hdfs://namenode:8020/Datafromlocal' into table songs_extern;

quit;

hive -S -e "select * from assign1.songs_extern";

hive;

drop table if exists emp_data_tab;

Create table if not exists emp_data_tab (id int, name string, age int, city string)
Row format delimited
Fields terminated by ','
Stored as textfile;

load data local inpath 'employee_details.txt' into table emp_data_tab;


alter table assign1_intern_tab rename to new_db.assign1_intern_tab;
