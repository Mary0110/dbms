alter session set "_ORACLE_SCRIPT"=true;
create user c##devtest identified by "devtestpassword";
select user from dual;
grant all privileges to c##devtest;

create table c##devtest.table_one(
    id number GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    field_first number
);
drop table c##devtest.table_two;

create table c##devtest.table_two(
    id number GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY ,
    field_second number,
    constraint fk_field_one_id foreign key(field_second) references table_one(id)
);
drop table table_one;

alter table c##devtest.table_one
add constraint fk_field_two1  foreign key(field_first) references table_two(id);
alter table c##devtest.table_one
drop constraint fk_field_two1;

CREATE OR REPLACE PACKAGE c##devtest.myfirstpackage
IS
PROCEDURE set_record (val IN table_one%ROWTYPE);
FUNCTION get_record (val IN NUMBER) RETURN table_one%ROWTYPE;
END ;

    create table c##devtest.table_three(
    id number GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY ,
    field_third number,
    constraint fk_field_three_id foreign key(field_third) references table_two(id)
);

drop table table_three;


