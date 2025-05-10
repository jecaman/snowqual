-- ===================================================================================
-- PROCEDURE: CREATE_TASK_FRESHNESS
-- Este procedimiento genera una task que valida si los datos han sido actualizados
-- recientemente, comparando el valor máximo de una columna de fecha con el SLA definido.
-- Si la columna de fecha no está especificada, el check se considera inválido y se elimina.
--
-- Se basa en los siguientes campos de la tabla CONTROL_DEFINITIONS:
--   - CHECK_TYPE: debe ser 'FRESHNESS'
--   - TARGET_SCHEMA: esquema de la tabla objetivo
--   - TARGET_TABLE: tabla a validar
--   - COLUMNS_KEY: columna de tipo fecha a evaluar
--   - REFERENCE_VALUE: SLA máximo en minutos
-- ===================================================================================

CREATE OR REPLACE PROCEDURE CREATE_TASK_FRESHNESS(CHECK_ID STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  target_schema STRING;
  target_table STRING;
  column_date STRING;
  sla_minutes NUMBER;
  schedule STRING;
  task_sql STRING;
  task_name STRING;
BEGIN
  -- Recuperar los parámetros necesarios desde la tabla de definición
  SELECT TARGET_SCHEMA, TARGET_TABLE, COLUMNS_KEY, REFERENCE_VALUE, SCHEDULE
  INTO target_schema, target_table, column_date, sla_minutes, schedule
  FROM CONTROL_DEFINITIONS
  WHERE CHECK_ID = :CHECK_ID;

  -- Validación: si no hay columna de fecha definida, eliminar el check
  IF column_date IS NULL OR TRIM(column_date) = '' THEN
    CALL DROP_CHECK_PR(CHECK_ID);
    RETURN 'ERROR: COLUMNS_KEY no está definida. El check fue eliminado automáticamente.';
  END IF;

  -- Nombre de la task
  LET task_name = 'TASK_' || CHECK_ID;

  -- Generar el SQL dinámico para crear la task
  LET task_sql = '
    CREATE OR REPLACE TASK IDENTIFIER(:1)
    WAREHOUSE = TFG_F1_WH
    SCHEDULE = ''' || schedule || '''
    AS
    INSERT INTO CONTROL_RESULTS (
      CHECK_ID, CHECK_NAME, CHECK_TYPE, RESULT, DETAILS, EXECUTED_AT
    )
    SELECT
      ''' || CHECK_ID || ''',
      ''' || task_name || ''',
      ''FRESHNESS'',
      CASE
        WHEN MAX(' || column_date || ') >= CURRENT_TIMESTAMP - INTERVAL ' || sla_minutes || ' MINUTE
        THEN ''OK''
        ELSE ''KO''
      END,
      OBJECT_CONSTRUCT(
        ''MAX_DATE'', MAX(' || column_date || '),
        ''SLA_MINUTES'', ' || sla_minutes || ',
        ''TOTAL_ROWS'', COUNT(*)
      ),
      CURRENT_TIMESTAMP
    FROM ' || target_schema || '.' || target_table || ';
  ';

  -- Ejecutar la creación de la task
  EXECUTE IMMEDIATE :task_sql USING task_name;

  RETURN 'Task FRESHNESS creada con éxito';
END;
$$;


-- ===================================================================================
-- PROCEDURE: CREATE_TASK_UNIQUENESS
-- Este procedimiento crea una task que valida que no existan registros duplicados
-- en una tabla, en función de las columnas clave definidas en COLUMNS_KEY.
-- Si los parámetros requeridos están vacíos, el procedimiento elimina el control.
--
-- Campos necesarios en CONTROL_DEFINITIONS:
-- CHECK_TYPE       : STRING, debe ser 'UNIQUENESS'
-- TARGET_SCHEMA    : STRING, esquema de la tabla objetivo
-- TARGET_TABLE     : STRING, nombre de la tabla objetivo
-- COLUMNS_KEY      : STRING, columnas que deben ser únicas (separadas por comas)
-- SCHEDULE         : STRING, frecuencia de ejecución
-- ===================================================================================

CREATE OR REPLACE PROCEDURE CREATE_TASK_UNIQUENESS(CHECK_ID STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  target_schema STRING;
  target_table STRING;
  columns_key STRING;
  schedule STRING;
  task_name STRING;
  task_sql STRING;
BEGIN
  -- Obtener parámetros necesarios
  SELECT TARGET_SCHEMA, TARGET_TABLE, COLUMNS_KEY, SCHEDULE
  INTO target_schema, target_table, columns_key, schedule
  FROM CONTROL_DEFINITIONS
  WHERE CHECK_ID = :CHECK_ID;

  -- Validar que COLUMNS_KEY no esté vacío
  IF columns_key IS NULL OR TRIM(columns_key) = '' THEN
    -- Si no hay columnas clave, eliminar el control
    CALL DROP_CHECK_PR(CHECK_ID);
    RETURN 'Control eliminado: COLUMNS_KEY vacío.';
  END IF;

  -- Nombre de la task
  LET task_name = 'TASK_' || CHECK_ID;

  -- Generar SQL dinámico de creación de la task
  LET task_sql = '
    CREATE OR REPLACE TASK IDENTIFIER(:1)
    WAREHOUSE = TFG_F1_WH
    SCHEDULE = ''' || schedule || '''
    AS
    WITH DUPLICADOS AS (
      SELECT ' || columns_key || ', COUNT(*) AS NUM_DUPLICADOS
      FROM ' || target_schema || '.' || target_table || '
      GROUP BY ' || columns_key || '
      HAVING COUNT(*) > 1
    )
    INSERT INTO CONTROL_RESULTS (
      CHECK_ID, CHECK_NAME, CHECK_TYPE, RESULT, DETAILS, EXECUTED_AT
    )
    SELECT
      ''' || CHECK_ID || ''',
      ''' || task_name || ''',
      ''UNIQUENESS'',
      CASE WHEN COUNT(*) = 0 THEN ''OK'' ELSE ''KO'' END,
      OBJECT_CONSTRUCT(
        ''NUM_DUPLICADOS'', COUNT(*),
        ''CLAVES'', ''' || columns_key || ''',
        ''TOTAL_ROWS'', (SELECT COUNT(*) FROM ' || target_schema || '.' || target_table || ')

      ),
      CURRENT_TIMESTAMP
    FROM DUPLICADOS;
  ';

  -- Ejecutar la creación de la task
  EXECUTE IMMEDIATE :task_sql USING task_name;

  RETURN '✅ Task UNIQUENESS creada con éxito';
END;
$$;

