CREATE OR REPLACE PROCEDURE compare_schemes(dev_schema_name IN VARCHAR2, prod_schema_name IN VARCHAR2)
IS
BEGIN
    compare_tables(dev_schema_name, prod_schema_name);
    compare_functions(dev_schema_name, prod_schema_name);
    compare_procedures(dev_schema_name, prod_schema_name);
    compare_packages(dev_schema_name, prod_schema_name);
    compare_indexes(dev_schema_name, prod_schema_name);
    EXECUTE IMMEDIATE 'TRUNCATE TABLE tables_to_create';
END compare_schemes;

CREATE OR REPLACE FUNCTION get_index_string(schema_name VARCHAR2, ind_name VARCHAR2)
    RETURN VARCHAR2
IS
CURSOR get_index IS
    SELECT all_ind_col.index_name, all_ind_col.table_name,
            all_ind_col.column_name, all_ind_col.column_position, all_ind.uniqueness
    FROM all_ind_columns all_ind_col
    INNER JOIN all_indexes all_ind
    ON all_ind.index_name = all_ind_col.index_name AND all_ind.owner = all_ind_col.index_owner
    WHERE all_ind_col.index_owner = UPPER(schema_name)
    AND all_ind_col.index_name = UPPER(ind_name)
    ORDER BY all_ind_col.column_position;

index_rec get_index%ROWTYPE;
index_string VARCHAR2(200);
BEGIN
    OPEN get_index;
    FETCH get_index INTO index_rec;
    index_string := index_string || ' ' || index_rec.table_name || '(';
    WHILE get_index%FOUND
    LOOP
        index_string := index_string || index_rec.column_name || ', ';
        FETCH get_index INTO index_rec;
    END LOOP;
    CLOSE get_index;
    index_string := RTRIM(index_string, ', ');
    index_string := index_string || ')';
    RETURN index_string;
END get_index_string;

CREATE OR REPLACE PROCEDURE compare_indexes(dev_schema_name VARCHAR2, prod_schema_name VARCHAR2)
IS
CURSOR get_indexes IS
    SELECT DISTINCT dev_uniqueness, dev_index_name, prod_uniqueness, prod_index_name
    FROM
        (SELECT ai.index_name dev_index_name, ai.uniqueness dev_uniqueness, ai.table_name dev_table_name, aic.column_name dev_column_name
        FROM all_indexes ai
        INNER JOIN all_ind_columns aic
        ON ai.index_name = aic.INDEX_NAME AND ai.owner = aic.INDEX_OWNER
        WHERE ai.owner = UPPER(dev_schema_name)
        AND GENERATED = 'N') dev
    FULL OUTER JOIN
        (SELECT ai.index_name prod_index_name, ai.uniqueness prod_uniqueness, ai.table_name prod_table_name, aic.column_name prod_column_name
        FROM all_indexes ai
        INNER JOIN all_ind_columns aic
        ON ai.index_name = aic.index_name AND ai.owner = aic.index_owner
        WHERE ai.owner = UPPER(prod_schema_name)
        AND GENERATED = 'N') prod
    ON dev.dev_table_name = prod.prod_table_name
    AND dev.dev_column_name = prod.prod_column_name;
    buf VARCHAR2(500);
BEGIN
    FOR rec IN get_indexes
    LOOP
        IF rec.prod_index_name IS NULL THEN
            buf := buf || 'CREATE ';
            IF rec.dev_uniqueness != 'NONUNIQUE' THEN
                buf := buf || rec.dev_uniqueness;
            END IF;
            buf := buf || ' INDEX ' || rec.dev_index_name ||' ON'|| get_index_string(dev_schema_name, rec.dev_index_name) || ';';
            DBMS_OUTPUT.PUT_LINE(buf);
            buf := NULL;
            CONTINUE;
        END IF;

        IF rec.dev_index_name IS NULL THEN
            DBMS_OUTPUT.PUT_LINE('DROP INDEX ' || rec.prod_index_name || ';');
            CONTINUE;
        END IF;
        IF get_index_string(dev_schema_name, rec.dev_index_name)
            !=
            get_index_string(prod_schema_name, rec.prod_index_name)
            OR rec.dev_uniqueness != rec.prod_uniqueness THEN
            DBMS_OUTPUT.PUT_LINE('DROP INDEX ' || rec.prod_index_name || ';');
            buf := buf || 'CREATE ';
            IF rec.dev_uniqueness != 'NONUNIQUE' THEN
                buf := buf || rec.dev_uniqueness;
            END IF;
            buf := buf || ' INDEX ' || rec.dev_index_name ||' ON'|| get_index_string(dev_schema_name, rec.dev_index_name) || ';';
            DBMS_OUTPUT.PUT_LINE(buf);
            buf := NULL;
        END IF;
    END LOOP;
END compare_indexes;

CREATE OR REPLACE PROCEDURE compare_packages(dev_schema_name VARCHAR2, prod_schema_name VARCHAR2)
IS

BEGIN
    FOR object_name_row IN
        (SELECT dev_name, prod_name
        FROM
        (SELECT object_name dev_name
         FROM all_objects
        WHERE owner = dev_schema_name AND object_type = 'PACKAGE') dev
        FULL JOIN
        (SELECT object_name prod_name
         FROM all_objects
        WHERE owner = prod_schema_name AND object_type = 'PACKAGE') prod
    ON dev.dev_name = prod.prod_name)
    LOOP
        IF object_name_row.prod_name IS NULL THEN
            add_object(dev_schema_name, 'PACKAGE', object_name_row.dev_name);
            add_object(dev_schema_name, 'PACKAGE BODY', object_name_row.dev_name);
            CONTINUE;
        END IF ;
        IF object_name_row.dev_name IS NULL THEN
            DBMS_OUTPUT.PUT_LINE('DROP PACKAGE ' || object_name_row.prod_name || ';');
            CONTINUE;
        END IF;
    END LOOP;
END compare_packages;

CREATE OR REPLACE FUNCTION get_callable_text(schema_name IN VARCHAR2, obj_type IN VARCHAR2, obj_name IN VARCHAR2)
    RETURN VARCHAR2
IS
    call_text VARCHAR2(32000) := '';
BEGIN
      FOR text_row in (
            --TRANSLATE( string1, string_to_replace, replacement_string )
            --carriage return + line feed
            SELECT UPPER(TRIM(' ' FROM (TRANSLATE(text, CHR(10) || CHR(13), ' ')))) obj_text
            FROM ALL_SOURCE
            WHERE OWNER = (schema_name) AND NAME = (obj_name)
            AND TYPE = (obj_type) AND TEXT != chr(10)) LOOP
            call_text := call_text || text_row.obj_text;
      END LOOP;
      RETURN call_text;
END;

CREATE OR REPLACE PROCEDURE add_object(dev_schema_name VARCHAR2, object_type VARCHAR2, object_name VARCHAR2)
IS
CURSOR get_object IS
    SELECT TRIM(' ' FROM (TRANSLATE(text, CHR(10) || CHR(13), ' '))) AS obj_text
    FROM all_source
    WHERE owner = dev_schema_name
    AND name = UPPER(object_name) AND type = UPPER(object_type);
check_var VARCHAR2(1000);
BEGIN
    OPEN get_object;
    FETCH get_object INTO check_var;
    CLOSE get_object;
    IF check_var IS NULL THEN
        RETURN;
    END IF;
    DBMS_OUTPUT.PUT_LINE('CREATE OR REPLACE ');
    FOR rec IN get_object
    LOOP
        DBMS_OUTPUT.PUT_LINE(rec.obj_text);
    END LOOP;
END add_object;

CREATE OR REPLACE PROCEDURE compare_callables(dev_schema_name IN VARCHAR2, prod_schema_name IN VARCHAR2, obj_type IN VARCHAR2)
IS
BEGIN
    FOR callable_object_row IN (SELECT * FROM
                (SELECT DISTINCT NAME dev_name
                --ALL_SOURCE describes the text source
                -- of the stored objects accessible to the current user
                FROM ALL_SOURCE WHERE owner = (dev_schema_name) AND type = (obj_type)) dev
                FULL JOIN
                (SELECT DISTINCT NAME prod_name
                FROM ALL_SOURCE WHERE owner = (prod_schema_name) AND type = (obj_type)) prod
                ON dev.dev_name = prod.prod_name) LOOP
        IF callable_object_row.dev_name IS NULL THEN
            DBMS_OUTPUT.PUT_LINE('DROP ' || (obj_type) || ' ' || (callable_object_row.prod_name) || ';');
        ELSIF callable_object_row.prod_name IS NULL THEN
            add_object(dev_schema_name, obj_type, callable_object_row.dev_name);
        ELSIF get_callable_text(dev_schema_name, obj_type, callable_object_row.dev_name) != get_callable_text(prod_schema_name, obj_type, callable_object_row.prod_name) THEN
            DBMS_OUTPUT.PUT_LINE('DROP ' || (obj_type) || ' ' || (callable_object_row.prod_name) || ';');
            add_object(dev_schema_name, obj_type, callable_object_row.dev_name);
        END IF;
    END LOOP;
END compare_callables;
create or replace procedure compare_functions(dev_schema_name in VARCHAR2, prod_schema_name IN VARCHAR2)
IS
BEGIN
    compare_callables(dev_schema_name, prod_schema_name, 'FUNCTION');

END compare_functions;

CREATE OR REPLACE PROCEDURE compare_procedures(dev_schema_name IN VARCHAR2, prod_schema_name IN VARCHAR2)
IS
BEGIN
    compare_callables(dev_schema_name, prod_schema_name, 'PROCEDURE');
END compare_procedures;

create or replace procedure c##dev.compare_tables(dev_scheme in varchar2, prod_scheme in varchar2)
    as
    begin
        for cur_table_names_row in (select * from
                        (select table_name as d_table_name from all_tables where owner = dev_scheme) dev_tables
                        full join
                        (select table_name as p_table_name from all_tables where owner = prod_scheme) prod_tables
                        on dev_tables.d_table_name = prod_tables.p_table_name)
            loop
            if cur_table_names_row.d_table_name is NULL then
                DBMS_OUTPUT.PUT_LINE('DROP TABLE ' || cur_table_names_row.p_table_name || ';');

            elsif cur_table_names_row.p_table_name is NULL then
                insert into tables_to_create(owner,table_name) values (dev_scheme, cur_table_names_row.d_table_name);
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
                                    'ALTER TABLE ' || cur_table_names_row.d_table_name || ' DROP COLUMN ' || cur_columns.prod_col_name || ';');
                         ELSIF cur_columns.prod_col_name IS NULL THEN
                                DBMS_OUTPUT.PUT_LINE(
                                    'ALTER TABLE ' || cur_table_names_row.d_table_name || ' ADD COLUMN ' || describe_column(dev_scheme, cur_table_names_row.d_table_name, cur_columns.dev_col_name) || ';');
                         ELSIF describe_column(dev_scheme, cur_table_names_row.d_table_name, cur_columns.dev_col_name) !=
                               describe_column(prod_scheme, cur_table_names_row.d_table_name, cur_columns.prod_col_name) THEN
                                -- If there is a bug, you can drop and then re-create column.
                                DBMS_OUTPUT.PUT_LINE('ALTER TABLE ' || cur_table_names_row.d_table_name || ' MODIFY ' || describe_column(dev_scheme, cur_table_names_row.p_table_name, cur_columns.dev_col_name) || ';');
                         END IF;
                    end loop;
                end if;

        end loop;
            for cur_table_names_row in (select * from
                        (select table_name as d_table_name from all_tables where owner = dev_scheme) dev_tables
                        inner join
                        (select table_name as p_table_name from all_tables where owner = prod_scheme) prod_tables
                        on dev_tables.d_table_name = prod_tables.p_table_name)
            loop
                    FOR rec in (SELECT * FROM
                                ((SELECT CONSTRAINT_NAME dev_constr_name FROM ALL_CONSTRAINTS
                                WHERE OWNER = (dev_scheme) AND TABLE_NAME = UPPER(cur_table_names_row.d_table_name) AND GENERATED != 'GENERATED NAME') dev
                                FULL JOIN
                                (SELECT CONSTRAINT_NAME prod_constr_name FROM ALL_CONSTRAINTS
                                WHERE OWNER = (prod_scheme) AND TABLE_NAME = UPPER(cur_table_names_row.d_table_name) AND GENERATED != 'GENERATED NAME') prod
                                ON dev.dev_constr_name = prod.prod_constr_name)) LOOP
                        IF rec.dev_constr_name IS NULL THEN
                            DBMS_OUTPUT.PUT_LINE('ALTER TABLE ' || UPPER(cur_table_names_row.d_table_name) || ' DROP CONSTRAINT ' || rec.prod_constr_name || ';');
                        ELSIF rec.prod_constr_name IS NULL THEN
                            DBMS_OUTPUT.PUT_LINE('ALTER TABLE ' || UPPER(cur_table_names_row.d_table_name) || ' ADD ' || get_outline_constraint(dev_scheme, rec.dev_constr_name) || ';');
                        ELSE
                            IF get_outline_constraint(dev_scheme, rec.dev_constr_name) != get_outline_constraint(prod_scheme, rec.prod_constr_name) THEN
                                DBMS_OUTPUT.PUT_LINE('ALTER TABLE ' || UPPER(cur_table_names_row.d_table_name) || ' DROP CONSTRAINT ' || rec.prod_constr_name || ';');
                                DBMS_OUTPUT.PUT_LINE('ALTER TABLE ' || UPPER(cur_table_names_row.d_table_name) || ' ADD ' || get_outline_constraint(dev_scheme, rec.dev_constr_name) || ';');
                            END IF;
                        END IF;
                    END LOOP;
        end loop;

        create_all_tables(dev_scheme);
    end;
select * from all_constraints;
select * from all_tables;
select USER from dual;

CREATE OR REPLACE PROCEDURE cmp_outline_constraints(dev_schema_name IN VARCHAR2, prod_schema_name IN VARCHAR2, tab_name IN VARCHAR2)
IS
    CURSOR cur_get_constr_name IS
    SELECT * FROM
    ((SELECT CONSTRAINT_NAME dev_constr_name FROM ALL_CONSTRAINTS
    WHERE OWNER = UPPER(dev_schema_name) AND TABLE_NAME = UPPER(tab_name) AND GENERATED != 'GENERATED NAME') dev
    FULL OUTER JOIN
    (SELECT CONSTRAINT_NAME prod_constr_name FROM ALL_CONSTRAINTS
    WHERE OWNER = UPPER(prod_schema_name) AND TABLE_NAME = UPPER(tab_name) AND GENERATED != 'GENERATED NAME') prod
    ON dev.dev_constr_name = prod.prod_constr_name);
BEGIN
    FOR rec in cur_get_constr_name LOOP
        IF rec.dev_constr_name IS NULL THEN
            DBMS_OUTPUT.PUT_LINE('ALTER TABLE ' || UPPER(tab_name) || ' DROP CONSTRAINT ' || rec.prod_constr_name || ';');
        ELSIF rec.prod_constr_name IS NULL THEN
            DBMS_OUTPUT.PUT_LINE('ALTER TABLE ' || UPPER(tab_name) || ' ADD ' || get_outline_constraint(dev_schema_name, rec.dev_constr_name) || ';');
        ELSE
            IF get_outline_constraint(dev_schema_name, rec.dev_constr_name) != get_outline_constraint(prod_schema_name, rec.prod_constr_name) THEN
                DBMS_OUTPUT.PUT_LINE('ALTER TABLE ' || UPPER(tab_name) || ' DROP CONSTRAINT ' || rec.prod_constr_name || ';');
                DBMS_OUTPUT.PUT_LINE('ALTER TABLE ' || UPPER(tab_name) || ' ADD ' || get_outline_constraint(dev_schema_name, rec.dev_constr_name) || ';');
            END IF;
        END IF;
    END LOOP;
END cmp_outline_constraints;

CREATE OR REPLACE FUNCTION get_outline_constraint(schema_name IN VARCHAR2, constr_name IN VARCHAR2) RETURN VARCHAR2
IS
constr_type VARCHAR2(1);
BEGIN

    SELECT CONSTRAINT_TYPE INTO constr_type FROM ALL_CONSTRAINTS
    WHERE OWNER = UPPER(schema_name) AND CONSTRAINT_NAME = UPPER(constr_name)
    FETCH FIRST 1 ROWS ONLY;

    IF constr_type = 'R' THEN
        RETURN get_fk_description(schema_name, constr_name);
    ELSE
        RETURN get_not_fk_constraint_desription(schema_name, constr_name);
    END IF;

EXCEPTION
    WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in get_outline_constraint()');
            RETURN NULL;
END get_outline_constraint;

    CREATE OR REPLACE FUNCTION get_outline_constraints_description(schema_name IN VARCHAR2, tab_name IN VARCHAR2, is_create_fk_constr IN NUMBER) RETURN VARCHAR2
IS
    CURSOR cur_get_constraints IS
    SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE FROM ALL_CONSTRAINTS
    WHERE OWNER = UPPER(schema_name) AND TABLE_NAME = UPPER(tab_name) AND GENERATED != 'GENERATED NAME';
    buff VARCHAR2(5000);
BEGIN
    FOR rec in cur_get_constraints LOOP
        IF rec.CONSTRAINT_TYPE = 'R' THEN
            IF is_create_fk_constr = 1 THEN
                buff := buff || get_fk_description(schema_name, rec.CONSTRAINT_NAME) || ',' || CHR(10);
            END IF;
        ELSE
            buff := buff || get_not_fk_constraint_desription(schema_name, rec.CONSTRAINT_NAME) || ',' || CHR(10);
        END IF;
    END LOOP;
    buff := RTRIM(buff, ',' || CHR(10));
    RETURN buff;
END get_outline_constraints_description;

CREATE OR REPLACE FUNCTION get_fk_description(schema_name IN VARCHAR2, constr_name IN VARCHAR2) RETURN VARCHAR2
IS
    buff VARCHAR2(1000) := 0;

    CURSOR cur_get_col(owner_name VARCHAR2, cons_name VARCHAR2) IS
    SELECT TABLE_NAME, COLUMN_NAME FROM ALL_CONS_COLUMNS
    WHERE OWNER = UPPER(owner_name) AND CONSTRAINT_NAME = UPPER(cons_name)
    ORDER BY POSITION;

    r_schema VARCHAR2(128);
    r_constr_name VARCHAR(128);
    del_rule VARCHAR2(9);

    is_write_table_name NUMBER := 1;
BEGIN
    SELECT R_OWNER, R_CONSTRAINT_NAME, DELETE_RULE INTO r_schema, r_constr_name, del_rule FROM ALL_CONSTRAINTS
    WHERE OWNER = UPPER(schema_name) AND CONSTRAINT_NAME = UPPER(constr_name) AND CONSTRAINT_TYPE = 'R'
    FETCH FIRST 1 ROWS ONLY;

    buff := 'CONSTRAINT ' || constr_name || ' FOREIGN KEY(';

    FOR rec IN cur_get_col(schema_name, constr_name) LOOP
        buff := buff || rec.COLUMN_NAME || ', ';
    END LOOP;
    buff := RTRIM(buff, ', ');
    buff := buff || ') REFERENCES ';

    FOR rec IN cur_get_col(r_schema, r_constr_name) LOOP
        IF is_write_table_name = 1 THEN
            buff := buff || rec.TABLE_NAME || '(';
            is_write_table_name := 0;
        END IF;
        buff := buff || rec.COLUMN_NAME || ', ';
    END LOOP;

    buff := RTRIM(buff, ', ');
    buff := buff || ')';
    IF del_rule != 'NO ACTION' THEN
        buff := buff || ' ON DELETE ' || del_rule;
    END IF;
    RETURN buff;
EXCEPTION
    WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in get_fk_description()');
            RETURN NULL;
END get_fk_description;

CREATE OR REPLACE FUNCTION get_not_fk_constraint_desription(schema_name IN VARCHAR2, constr_name IN VARCHAR2) RETURN VARCHAR2
IS
    CURSOR cur_get_col IS
    SELECT COLUMN_NAME FROM ALL_CONS_COLUMNS
    WHERE OWNER = UPPER(schema_name) AND CONSTRAINT_NAME = UPPER(constr_name)
    ORDER BY POSITION;
    constr_type VARCHAR2(1);
    search_cond LONG;
    buff VARCHAR2(300);
BEGIN
    SELECT CONSTRAINT_TYPE, SEARCH_CONDITION INTO constr_type, search_cond FROM ALL_CONSTRAINTS
    WHERE OWNER = UPPER(schema_name) AND CONSTRAINT_NAME = UPPER(constr_name)
    FETCH FIRST 1 ROWS ONLY;

    buff := 'CONSTRAINT ' || constr_name;

    CASE constr_type
        WHEN 'P' THEN
            BEGIN
                buff := buff || ' PRIMARY KEY(';
                for rec in cur_get_col LOOP
                    buff := buff || rec.COLUMN_NAME || ', ';
                END LOOP;
                buff := RTRIM(buff, ', ');
                buff := buff || ')';
            END;
        WHEN 'U' THEN
            BEGIN
                buff := buff ||' UNIQUE(';
                for rec in cur_get_col LOOP
                    buff := buff || rec.COLUMN_NAME || ', ';
                END LOOP;
                buff := RTRIM(buff, ', ');
                buff := buff || ')';
            END;
        WHEN 'C' THEN
            buff := buff || ' CHECK(' || search_cond || ')';
    END CASE;
    RETURN buff;
EXCEPTION
    WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in get_not_fk_constraint_desription()');
            RETURN NULL;
END get_not_fk_constraint_desription;

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

CREATE OR REPLACE FUNCTION is_table_exists_in_tables_to_create(tab_name IN VARCHAR2) RETURN BOOLEAN
IS
    num NUMBER;
BEGIN
    SELECT DISTINCT COUNT(TABLE_NAME) INTO num FROM tables_to_create WHERE TABLE_NAME = UPPER(tab_name);
    IF num > 0 THEN
        RETURN TRUE;
    ELSE
        RETURN FALSE;
    END IF;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN FALSE;
    WHEN OTHERS THEN
        RETURN FALSE;
END is_table_exists_in_tables_to_create;

CREATE OR REPLACE PROCEDURE update_tables_to_create(schema_name IN VARCHAR2)
IS
    CURSOR cur_get_table IS
    SELECT level, CONNECT_BY_ISCYCLE is_cycle, parent_owner, parent_table, child_owner, child_table, constr_name, SYS_CONNECT_BY_PATH(parent_table, '-') cycle_path
    FROM (SELECT pk.OWNER parent_owner, pk.TABLE_NAME parent_table, fk.OWNER child_owner, fk.TABLE_NAME child_table, fk.CONSTRAINT_NAME constr_name
            FROM ALL_CONSTRAINTS pk
            INNER JOIN ALL_CONSTRAINTS fk
            ON pk.OWNER = fk.R_OWNER AND pk.CONSTRAINT_NAME = fk.R_CONSTRAINT_NAME
            WHERE pk.OWNER = UPPER(schema_name))
    CONNECT BY NOCYCLE PRIOR child_table = parent_table;
    tmp_lvl NUMBER := 0;
BEGIN
    FOR rec in cur_get_table LOOP
        IF rec.is_cycle = 1 THEN
            UPDATE TABLES_TO_CREATE SET owner = rec.child_owner, lvl = rec.level,
            is_cycle = rec.is_cycle, fk_name = rec.constr_name, path = rec.cycle_path
            WHERE table_name = rec.child_table;
            CONTINUE;
        END IF;

        IF NOT is_table_exists_in_tables_to_create(rec.child_table) THEN
            CONTINUE;
        END IF;

        SELECT lvl INTO tmp_lvl FROM TABLES_TO_CREATE WHERE table_name = rec.child_table;
        IF rec.level > tmp_lvl THEN
            UPDATE TABLES_TO_CREATE SET owner = rec.child_owner, lvl = rec.level,
            is_cycle = rec.is_cycle, fk_name = rec.constr_name, path = rec.cycle_path
            WHERE table_name = rec.child_table;
        END IF;
    END LOOP;
EXCEPTION
    WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in update_tables_to_create()');
END update_tables_to_create;

CREATE OR REPLACE PROCEDURE create_table(schema_name IN VARCHAR2, tab_name IN VARCHAR2, is_create_fk_constr IN NUMBER)
IS
    CURSOR cur_get_col IS
    SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS
    WHERE OWNER = UPPER(schema_name) AND TABLE_NAME = UPPER(tab_name);
    buff VARCHAR2(5000);
    outline_constr_buff VARCHAR2(3000);
BEGIN
    buff := 'CREATE TABLE ' || tab_name || ' (' || CHR(10);
    FOR rec in cur_get_col LOOP
        buff := buff || describe_column(schema_name, tab_name, rec.COLUMN_NAME) || ',' || CHR(10);
    END LOOP;
    outline_constr_buff := get_outline_constraints_description(schema_name, tab_name, is_create_fk_constr);
    IF outline_constr_buff IS NULL THEN
        buff := RTRIM(buff, ',' || CHR(10));
    ELSE
        buff := buff || outline_constr_buff;
    END IF;
    buff := buff || ');';
    DBMS_OUTPUT.PUT_LINE(buff);
END create_table;

CREATE OR REPLACE FUNCTION is_table_exists(schema_name IN VARCHAR2, tab_name IN VARCHAR2) RETURN BOOLEAN
IS
    num NUMBER;
BEGIN
    SELECT DISTINCT COUNT(TABLE_NAME) INTO num FROM DBA_TABLES WHERE OWNER = UPPER(schema_name) AND TABLE_NAME = UPPER(tab_name);
    IF num > 0 THEN
        RETURN TRUE;
    ELSE
        RETURN FALSE;
    END IF;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN FALSE;
    WHEN OTHERS THEN
        RETURN FALSE;
END is_table_exists;

CREATE OR REPLACE PROCEDURE create_all_tables(schema_name IN VARCHAR2)
IS
is_create_fk_constr NUMBER;
BEGIN
    update_tables_to_create(schema_name);

    FOR rec in (SELECT * FROM TABLES_TO_CREATE ORDER BY lvl) LOOP
        IF rec.is_cycle = 1 THEN
            is_create_fk_constr := 0;
        ELSE
            is_create_fk_constr := 1;
        END IF;
        create_table(schema_name, rec.table_name, is_create_fk_constr);
    END LOOP;

    FOR rec in (SELECT * FROM TABLES_TO_CREATE WHERE is_cycle = 1) LOOP
--         DBMS_OUTPUT.PUT_LINE('ERROR cyclic foreign key: ' || rec.path);
        DBMS_OUTPUT.PUT_LINE('ALTER TABLE ' || rec.table_name || ' ADD' ||  CHR(10) || get_fk_description(schema_name, rec.fk_name) || ';');
    END LOOP;
END create_all_tables;

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
            DBMS_OUTPUT.PUT_LINE('ERROR in get_sequence_description()');
            RETURN NULL;
END;


create table c##dev.tables_to_create(
    tables_to_create_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    owner VARCHAR2(128),
    table_name VARCHAR2(128),
    lvl NUMBER DEFAULT 0,
    is_cycle NUMBER DEFAULT 0,
    fk_name VARCHAR2(128),
    path VARCHAR2(500)

);
drop table tables_to_create;

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

call compare_schemes('C##DEVTEST', 'C##PROD');
----------------------------------------------------------------------------------------------
select * from ALL_TAB_COLUMNS;
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
select *from all_tables where owner = 'c##dev';
call compare_tables('C##DEVTEST', 'C##PROD');
select * from tables_to_create;
select * from all_source where rownum<=100 and type = 'FUNCTION';
select * from C##DEV.tables_to_create;
truncate table C##DEV.tables_to_create;


