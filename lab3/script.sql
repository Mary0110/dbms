create or replace procedure c##dev.compare_tables(dev_scheme in varchar2, prod_scheme in varchar2)
    authid current_user
as
    num_of_tables_in_prod number;
        command varchar2;
    begin
        for cur_table_names_row in (select * from
                        (select table_name as d_table_name from all_tables where owner = dev_scheme) dev_tables
                        full join
                        (select table_name as p_table_name from all_tables where owner = prod_scheme) prod_tables
                        on dev_tables.d_table_name = prod_tables.p_table_name)
            loop

            if cur_table_names_row.d_table_name is NULL then
                insert into tables_to_drop values cur_table_names_row.p_table_name ;

            elsif cur_table_names_row.p_table_name is NULL then
                insert into tables_to_create values cur_table_names_row.d_table_name;
            else
                for cur_columns in (select * from
                                (select COLUMN_NAME dev_col_name from ALL_TAB_COLUMNS
                                where OWNER = dev_scheme and TABLE_NAME = cur_table_names_row.d_table_name) dev
                                full join
                                (select COLUMN_NAME prod_col_name from ALL_TAB_COLUMNS
                                where OWNER = prod_scheme AND TABLE_NAME = cur_table_names_row.d_table_name) prod
                                ON dev.dev_col_name = prod.prod_col_name)
                    loop
                         IF cur_columns.dev_col_name IS NULL THEN
                                DBMS_OUTPUT.PUT_LINE(
                                    'ALTER TABLE ' || cur_table_names_row.p_table_name || ' DROP COLUMN ' || cur_columns.prod_col_name || ';');
                         ELSIF cur_columns.prod_col_name IS NULL THEN
                                DBMS_OUTPUT.PUT_LINE(
                                    'ALTER TABLE ' || cur_table_names_row.p_table_name || ' ADD COLUMN ' || describe_column(dev_scheme, cur_table_names_row.p_table_name, cur_columns.dev_col_name) || ';');
                         ELSIF describe_column(dev_scheme, cur_table_names_row.p_table_name, cur_columns.dev_col_name) !=
                               describe_column(prod_scheme, cur_table_names_row.p_table_name, cur_columns.prod_col_name) THEN
                                -- If there is a bug, you can drop and then re-create column.
                                DBMS_OUTPUT.PUT_LINE('ALTER TABLE ' || cur_table_names_row.p_table_name || ' MODIFY ' || describe_column(dev_scheme, cur_table_names_row.p_table_name, cur_columns.dev_col_name) || ';');
                         END IF;
                    end loop;
--                 for cur_column in (select * from ALL_TAB_COLS
--                                             where owner = dev_scheme
--                                             and table_name = cur_row.d_table_name) loop
--                     insert into columns values ()
--
--                     end loop;
--
--                 for column in (select * from ALL_TAB_COLS
--                                         where owner = dev_scheme
--                                         and table_name = dev_table_item.TABLE_NAME) loop
--                     if column.COLUMN_NAME not in (select * from ALL_TAB_COLS
--                                         where owner = prod_scheme
--                                         and table_name = dev_table_item.TABLE_NAME) then
--                         command := 'alter table '||prod_scheme||'.'||dev_table_item.TABLE_NAME||' add column '||column.COLUMN_NAME|| column.DATA_TYPE||';';
--                         execute immediate command;
--                     end if;
--                 end loop;
--             end if;
--         end loop;
--         for prod_table_item in (select * from all_tables where owner = prod_scheme) loop
--             if prod_table_item.TABLE_NAME not in (select * from all_tables where owner = dev_scheme) then
--                 command := 'drop table'||prod_scheme||'.'||prod_table_item.TABLE_NAME||';';
--                 execute immediate command;
--             else
--                 for column in (select * from ALL_TAB_COLS
--                                             where owner = prod_scheme
--                                             and table_name = prod_table_item.TABLE_NAME) loop
--                         if column.COLUMN_NAME not in (select * from ALL_TAB_COLS
--                                             where owner = dev_scheme
--                                             and table_name = prod_table_item.TABLE_NAME) then
--                             command := 'alter table '||prod_scheme||'.'||prod_table_item.TABLE_NAME||' drop column '||column.COLUMN_NAME||';';
--                             execute immediate command;
--                         end if;
--                 end loop;
            end if;
        end loop;
    end;
select * from all_constraints;
select * from all_tables;
select USER from dual;

CREATE OR REPLACE FUNCTION describe_column(schema_name IN VARCHAR2, tab_name IN VARCHAR2, col_name IN VARCHAR2) RETURN VARCHAR2
IS
clmn_dt VARCHAR2(128);
clmn_nullable VARCHAR2(1);
clmn_data_default LONG;
clmn_data_precision NUMBER;
clmn_data_scale NUMBER;
clmn_char_length NUMBER;
descr_res VARCHAR2(500) := '';
comma_in_varchar NUMBER := 0;
BEGIN
    SELECT DATA_TYPE, nullable, data_default, data_precision, data_scale, char_length INTO
    clmn_dt, clmn_nullable, clmn_data_default, clmn_data_precision, clmn_data_scale, clmn_char_length
    FROM ALL_TAB_COLUMNS
    WHERE owner = (schema_name) AND TABLE_NAME = (tab_name) AND COLUMN_NAME = (col_name);

    descr_res := (col_name) || ' ' || clmn_dt;

    IF clmn_data_precision IS NOT NULL or clmn_data_scale IS NOT NULL THEN
        descr_res := descr_res || '(';
        IF clmn_data_precision IS NOT NULL THEN
                descr_res := descr_res || clmn_data_precision;
                comma_in_varchar := 1;
        END IF;

        IF clmn_data_scale IS NOT NULL THEN
            IF comma_in_varchar = 1 THEN
                descr_res := descr_res || ', ';
            END IF;
            descr_res := descr_res || clmn_data_scale || ')';
        END IF;
    END IF;

    -- For CHAR, VARCHAR2, NCHAR, NVARCHAR2.
    IF clmn_char_length > 0 THEN
        descr_res := descr_res || '(' || clmn_char_length || ')';
    END IF;

    IF clmn_data_default IS NOT NULL THEN
            IF clmn_data_default LIKE '%.nextval' THEN
                descr_res := descr_res || ' ' || describe_sequence(schema_name, REGEXP_SUBSTR (clmn_data_default, '(ISEQ\$\$_\d+)'));
            ELSE
                descr_res := descr_res || ' DEFAULT ' || clmn_data_default;
            END IF;
    END IF;

    descr_res := descr_res || describe_inline_constraints(schema_name, tab_name, col_name);

    IF clmn_nullable = 'N' THEN
        descr_res := descr_res || ' NOT NULL';
    END IF;

    RETURN descr_res;
EXCEPTION
    WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('error in describe column function');
            RETURN NULL;
END;


create table tables_to_drop(
    name varchar2(100)
);

create table tables_to_create(
    name varchar2(100)
);

create table columns(
    column_name varchar2(100) primary key,
    table_name varchar(100),
    constr varchar2(100),
    constraint fk_table_name foreign key(table_name) references tables_to_create(name)
);

call c##dev.compare_tables('C##DEV', 'C##PROD');
----------------------------------------------------------------------------------------------
select * from ALL_TAB_COLUMNS;
--upper names
--tables to drop
--