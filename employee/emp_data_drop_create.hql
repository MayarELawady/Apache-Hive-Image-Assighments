drop table if exists emp_data_tab;

Create table if not exists emp_data_tab (id int, name string, age int, city string)
Row format delimited
Fields terminated by ','
Stored as textfile;

load data local inpath 'employee_details.txt' into table emp_data_tab;