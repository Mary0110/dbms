create or replace procedure c##dev.compare_tables(dev_scheme in varchar2, prod_scheme in varchar2)
    as
    begin
        for cur_table_names_row in (select * from
                        (select table_name as d_table_name from all_tables where owner = dev_scheme) dev_tables
                        full join
                        (select table_name as p_table_name from all_tables where owner = prod_scheme) prod_tables
                        on dev_tables.d_table_name = prod_tables.p_table_name)
            loop
            dbms_output.PUT_LINE(cur_table_names_row.p_table_name);
            if cur_table_names_row.d_table_name is NULL then

                insert into c##dev.tables_to_drop(name)
                values (cur_table_names_row.p_table_name) ;

            elsif cur_table_names_row.p_table_name is NULL then
                insert into tables_to_create(name) values (cur_table_names_row.d_table_name);
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

CREATE OR REPLACE FUNCTION describe_inline_constraints(schema_name IN VARCHAR2, tab_name IN VARCHAR2, col_name IN VARCHAR2) RETURN VARCHAR2
IS
    descr_res VARCHAR2(300);

BEGIN
    FOR cur_inline_constr IN  (SELECT * FROM
                                ((SELECT CONSTRAINT_NAME FROM ALL_CONS_COLUMNS
                                --ALL_CONS_COLUMNS describes columns
                                -- that are accessible to the current user and that
                                -- are specified in constraints.
                                WHERE OWNER = schema_name AND TABLE_NAME = tab_name AND COLUMN_NAME = col_name) all_c
                                INNER JOIN
                            (SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE, SEARCH_CONDITION FROM ALL_CONSTRAINTS
                            WHERE OWNER = (schema_name) AND TABLE_NAME = (tab_name) AND GENERATED = 'GENERATED NAME') dba
                            ON all_c.CONSTRAINT_NAME = dba.CONSTRAINT_NAME)) LOOP
        if cur_inline_constr.CONSTRAINT_TYPE ='P' then
            descr_res := descr_res || ' PRIMARY KEY';
        elsif cur_inline_constr.CONSTRAINT_TYPE ='U' THEN
            descr_res := descr_res ||' UNIQUE';
        elsif cur_inline_constr.CONSTRAINT_TYPE ='C' THEN
            IF cur_inline_constr.SEARCH_CONDITION NOT LIKE '% IS NOT NULL' THEN
                descr_res := descr_res || ' CHECK(' || cur_inline_constr.SEARCH_CONDITION || ')';
            END IF;
        ELSE NULL;
        END if;
    END LOOP;
RETURN descr_res;
END;

CREATE OR REPLACE FUNCTION describe_sequence(schema_name IN VARCHAR2, seq_name IN VARCHAR2) RETURN VARCHAR2
IS
    seq_min_value NUMBER;
    seq_max_value NUMBER;
    seq_increment_by NUMBER;
    gen_type VARCHAR2(10);
    seq_description VARCHAR2(300);
BEGIN
    SELECT MIN_VALUE, MAX_VALUE, INCREMENT_BY INTO seq_min_value, seq_max_value, seq_increment_by FROM ALL_SEQUENCES WHERE SEQUENCE_NAME = seq_name AND SEQUENCE_OWNER = (schema_name);
    SELECT GENERATION_TYPE INTO gen_type FROM ALL_TAB_IDENTITY_COLS WHERE SEQUENCE_NAME = seq_name AND OWNER = (schema_name);
    -- It makes no difference how to do this or the following.
    -- seq_description := 'GENERATED ' || gen_type || ' AS IDENTITY' || ' START WITH ' || min_val || ' INCREMENT BY ' || inc_by || ' MAXVALUE ' || max_val;

    seq_description := 'GENERATED ' || gen_type || ' AS IDENTITY';
    IF seq_min_value != 1 THEN
        seq_description := seq_description || ' START WITH ' || seq_min_value;
    END IF;
    IF seq_increment_by != 1 THEN
        seq_description := seq_description || ' INCREMENT BY ' || seq_increment_by;
    END IF;
    IF seq_max_value != 9999999999999999999999999999 THEN
        seq_description := seq_description || ' MAXVALUE ' || seq_max_value;
    END IF;
    RETURN seq_description;
EXCEPTION
    WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error in get_sequence_description()');
            RETURN NULL;
END;


create table c##dev.tables_to_drop(
    name VARCHAR2(128)
);
drop table tables_to_drop;

create table c##dev.tables_to_create(
    name VARCHAR2(128)
);
drop table tables_to_create;

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
-- --
select table_name from all_tables;
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    DATA_LENGTH,
    DATA_PRECISION,
    DATA_SCALE
FROM ALL_TAB_COLS
where COLUMN_NAME = 'TABLE_NAME';
insert into tables_to_create(name) values (select * from c##dev.al)
;
select *from all_tables where owner = 'c##dev';
call compare_tables('C##DEV', 'C##PROD');
select * from tables_to_create;