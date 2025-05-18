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
LANGUAGE JAVASCRIPT
AS
$$
var stmt = snowflake.createStatement({
  sqlText: `
    SELECT TARGET_SCHEMA, TARGET_TABLE, COLUMNS_KEY, REFERENCE_VALUE, SCHEDULE, WHERE_CLAUSE
    FROM CONTROL_DEFINITIONS
    WHERE CHECK_ID = ?;
  `,
  binds: [CHECK_ID]
});

var rs = stmt.execute();
if (!rs.next()) {
  return 'No se encontró el CHECK_ID: ' + CHECK_ID;
}

var target_schema = rs.getColumnValue(1);
var target_table = rs.getColumnValue(2);
var column_date = rs.getColumnValue(3);
var sla_minutes = rs.getColumnValue(4);
var schedule = rs.getColumnValue(5);
var where_clause = rs.getColumnValue(6);

if (!column_date || column_date.trim() === '') {
  snowflake.execute({
    sqlText: `CALL DROP_CHECK_PR(?)`,
    binds: [CHECK_ID]
  });
  return 'ERROR: COLUMNS_KEY no está definida. El check fue eliminado automáticamente.';
}

var task_name = 'TASK_' + CHECK_ID;
var where_sql = (where_clause && where_clause.trim() !== '') ? `WHERE ${where_clause}` : '';

var task_sql = `
  CREATE OR REPLACE TASK IDENTIFIER(?)
  WAREHOUSE = TFG_F1_WH
  SCHEDULE = '${schedule}'
  AS
  INSERT INTO CONTROL_RESULTS (
    CHECK_ID, CHECK_NAME, CHECK_TYPE, RESULT, DETAILS, EXECUTED_AT
  )
  SELECT
    '${CHECK_ID}',
    '${task_name}',
    'FRESHNESS',
    CASE
      WHEN MAX(${column_date}) >= CURRENT_TIMESTAMP - INTERVAL ${sla_minutes} MINUTE
      THEN 'OK'
      ELSE 'KO'
    END,
    OBJECT_CONSTRUCT(
      'MAX_DATE', MAX(${column_date}),
      'SLA_MINUTES', ${sla_minutes},
      'TOTAL_ROWS', COUNT(*)
    ),
    CURRENT_TIMESTAMP
  FROM ${target_schema}.${target_table}
  ${where_sql};
`;

snowflake.createStatement({
  sqlText: task_sql,
  binds: [task_name]
}).execute();

return 'Task FRESHNESS creada con éxito';
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
LANGUAGE JAVASCRIPT
AS
$$
var stmt = snowflake.createStatement({
  sqlText: `
    SELECT TARGET_SCHEMA, TARGET_TABLE, COLUMNS_KEY, SCHEDULE, WHERE_CLAUSE
    FROM CONTROL_DEFINITIONS
    WHERE CHECK_ID = ?;
  `,
  binds: [CHECK_ID]
});

var rs = stmt.execute();
if (!rs.next()) {
  return 'No se encontró el CHECK_ID: ' + CHECK_ID;
}

var target_schema = rs.getColumnValue(1);
var target_table = rs.getColumnValue(2);
var columns_key = rs.getColumnValue(3);
var schedule = rs.getColumnValue(4);
var where_clause = rs.getColumnValue(5);

if (!columns_key || columns_key.trim() === '') {
  snowflake.execute({
    sqlText: `CALL DROP_CHECK_PR(?)`,
    binds: [CHECK_ID]
  });
  return 'Control eliminado: COLUMNS_KEY vacío.';
}

var task_name = 'TASK_' + CHECK_ID;
var    = (where_clause && where_clause.trim() !== '') ? `WHERE ${where_clause}` : '';

var task_sql = `
  CREATE OR REPLACE TASK IDENTIFIER(?)
  WAREHOUSE = TFG_F1_WH
  SCHEDULE = '${schedule}'
  AS
  WITH DUPLICADOS AS (
    SELECT ${columns_key}, COUNT(*) AS NUM_DUPLICADOS
    FROM ${target_schema}.${target_table}
    ${where_sql}
    GROUP BY ${columns_key}
    HAVING COUNT(*) > 1
  )
  INSERT INTO CONTROL_RESULTS (
    CHECK_ID, CHECK_NAME, CHECK_TYPE, RESULT, DETAILS, EXECUTED_AT
  )
  SELECT
    '${CHECK_ID}',
    '${task_name}',
    'UNIQUENESS',
    CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'KO' END,
    OBJECT_CONSTRUCT(
      'NUM_DUPLICADOS', COUNT(*),
      'CLAVES', '${columns_key}',
      'TOTAL_ROWS', (SELECT COUNT(*) FROM ${target_schema}.${target_table} ${where_sql})
    ),
    CURRENT_TIMESTAMP
  FROM DUPLICADOS;
`;

snowflake.createStatement({
  sqlText: task_sql,
  binds: [task_name]
}).execute();

return 'Task UNIQUENESS creada con éxito';
$$;


-- ===================================================================================
-- PROCEDURE: CONSISTENCY
-- Este procedimiento compara los resultados de dos consultas (SOURCE y TARGET),
-- identificando claves que estén solo en una de ellas, según los campos definidos
-- en KEY_FIELDS. Permite ejecutar múltiples iteraciones con parámetros dinámicos.
--
-- Parámetros:
-- SOURCE_QUERY     : STRING, consulta SQL parametrizable para el origen
-- TARGET_QUERY     : STRING, consulta SQL parametrizable para el destino
-- KEY_FIELDS       : STRING, lista de campos clave para el JOIN (separados por comas)
-- PARAMS           : STRING, JSON con los valores a sustituir en las queries
--
-- Resultado:
-- Tabla con una columna REASON (OBJECT con errores y metadatos) y RESULT (OK o KO)
-- ===================================================================================
CREATE OR REPLACE PROCEDURE CREATE_TASK_CONSISTENCY(CHECK_ID STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
var stmt = snowflake.createStatement({
  sqlText: `
    SELECT CONSISTENCY_SOURCE_QUERY, CONSISTENCY_TARGET_QUERY, THRESHOLD_DIFF, SCHEDULE
    FROM CONTROL_DEFINITIONS
    WHERE CHECK_ID = ?;
  `,
  binds: [CHECK_ID]
});

var rs = stmt.execute();
if (!rs.next()) {
  return 'No se encontró el CHECK_ID: ' + CHECK_ID;
}

var source_query = rs.getColumnValue(1);
var target_query = rs.getColumnValue(2);
var threshold = rs.getColumnValue(3);
var schedule = rs.getColumnValue(4);

if (!source_query || !target_query) {
  return 'ERROR: Se requieren CONSISTENCY_SOURCE_QUERY y CONSISTENCY_TARGET_QUERY definidos.';
}

if (!threshold || threshold <= 0) {
  threshold = 0.01; // Valor por defecto del 1% si no se define
}

var task_name = 'TASK_' + CHECK_ID;

// Plantilla SQL de la task
var task_sql = `
  CREATE OR REPLACE TASK IDENTIFIER(?)
  WAREHOUSE = TFG_F1_WH
  SCHEDULE = '${schedule}'
  AS
  WITH
  SOURCE AS (${source_query}),
  TARGET AS (${target_query}),
  DIF AS (
    SELECT
      ${buildComparisonBlock()}
    FROM SOURCE, TARGET
  )
  INSERT INTO CONTROL_RESULTS (
    CHECK_ID, CHECK_NAME, CHECK_TYPE, RESULT, DETAILS, EXECUTED_AT
  )
  SELECT
    '${CHECK_ID}',
    '${task_name}',
    'CONSISTENCY',
    CASE WHEN EXISTS (
      SELECT * FROM DIF WHERE ABS(REL_DIFF) > ${threshold}
    ) THEN 'KO' ELSE 'OK' END,
    OBJECT_AGG(METRIC, OBJECT_CONSTRUCT(
      'SOURCE_VALUE', SRC_VAL,
      'TARGET_VALUE', TRGT_VAL,
      'REL_DIFF', REL_DIFF
    )),
    CURRENT_TIMESTAMP
  FROM DIF;
`;

// Función auxiliar inline para generar la lógica de comparación genérica
function buildComparisonBlock() {
  return `
    COLUMN_NAME AS METRIC,
    SRC_VAL,
    TRGT_VAL,
    CASE
      WHEN SRC_VAL = 0 AND TRGT_VAL = 0 THEN 0
      WHEN SRC_VAL = 0 OR TRGT_VAL = 0 THEN 1
      ELSE ABS(SRC_VAL - TRGT_VAL) / NULLIF(ABS(SRC_VAL), 0)
    END AS REL_DIFF
    FROM (
      SELECT
        COLUMN_NAME,
        TO_NUMBER(SRC.VALUE) AS SRC_VAL,
        TO_NUMBER(TRGT.VALUE) AS TRGT_VAL
      FROM
        LATERAL FLATTEN(INPUT => OBJECT_CONSTRUCT(*)) SRC,
        LATERAL FLATTEN(INPUT => OBJECT_CONSTRUCT(*)) TRGT
      WHERE SRC.KEY = TRGT.KEY
    )
  `;
}

snowflake.createStatement({
  sqlText: task_sql,
  binds: [task_name]
}).execute();

return 'Task CONSISTENCY creada con éxito';
$$;