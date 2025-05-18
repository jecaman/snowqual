-- ============================================================
-- TABLA: CONTROL_DEFINITIONS
-- Esta tabla centraliza la definición de todos los controles 
-- de calidad que serán desplegados automáticamente como tasks.
-- Desde aquí se gestionan altas, bajas y modificaciones.
-- ============================================================

CREATE OR REPLACE TABLE CONTROL_DEFINITIONS (
  CHECK_ID STRING NOT NULL,                            -- Identificador único del control
  CHECK_NAME STRING NOT NULL,                          -- Nombre descriptivo del control
  CHECK_TYPE STRING NOT NULL,                          -- Tipo de check: UNIQUENESS, FRESHNESS, etc.
  TARGET_SCHEMA STRING NOT NULL,                       -- Esquema donde se encuentra la tabla objetivo
  TARGET_TABLE STRING NOT NULL,                        -- Nombre de la tabla objetivo
  SOURCE_SCHEMA STRING,                                -- Esquema de tabla de referencia (para CONSISTENCY o FUNCTIONALITY)
  SOURCE_TABLE STRING,                                 -- Tabla de referencia (si aplica)
  COLUMNS_KEY STRING,                                  -- Lista de columnas clave o de control
  CONSISTENCY_SOURCE_QUERY STRING,                     -- Query de origen definida por el usuario
  CONSISTENCY_TARGET_QUERY STRING,                     -- Query de destino definida por el usuario
  THRESHOLD_DIFF FLOAT DEFAULT 0.05                    -- Umbral de discrepancia permitido (porcentaje)
  WHERE_CLAUSE STRING,                                 -- Filtro opcional para delimitar el subconjunto de datos
  SCHEDULE STRING NOT NULL,                            -- Frecuencia de ejecución (por ejemplo, '15 MINUTE')
  IS_ACTIVE BOOLEAN DEFAULT TRUE,                      -- Estado del control (activo o inactivo)
  CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,  -- Fecha de creación del control
  UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,  -- Fecha de última modificación
  CREATED_BY STRING,                                   -- Usuario que creó el control
  UPDATED_BY STRING                                    -- Usuario que realizó la última modificación
);


-- ===================================================================================
-- TABLA: CONTROL_RESULTS
-- Esta tabla almacena los resultados de cada ejecución de control de calidad.
-- Incluye un resumen del resultado (OK, KO, ERROR) y un objeto con los detalles.
-- ===================================================================================

CREATE OR REPLACE TABLE CONTROL_RESULTS (
  CHECK_ID STRING,                         -- ID del control ejecutado
  CHECK_NAME STRING,                       -- Nombre del control
  CHECK_TYPE STRING,                       -- Tipo de control (FRESHNESS, UNIQUENESS, etc.)
  RESULT STRING,                           -- Resultado de la ejecución: OK, KO o ERROR
  DETAILS VARIANT,                         -- Objeto con detalles adicionales del resultado
  EXECUTED_AT TIMESTAMP_LTZ                -- Fecha y hora de ejecución
);

-- ===================================================================================
-- STREAM: CONTROL_DEFINITIONS_CHANGES_STREAM
-- Este stream captura INSERTS, UPDATES y DELETES sobre la tabla CONTROL_DEFINITIONS.
-- Utiliza APPEND_ONLY = FALSE para registrar todas las acciones con metadatos asociados.
-- ===================================================================================

CREATE OR REPLACE STREAM CONTROL_DEFINITIONS_CHANGES_STREAM
  ON TABLE CONTROL_DEFINITIONS
  APPEND_ONLY = FALSE
  SHOW_INITIAL_ROWS = FALSE;

-- ===================================================================================
-- TASK: TASK_DISPATCH_CONTROL_CHANGES
-- Esta task se ejecuta automáticamente cuando hay nuevos cambios en la tabla
-- CONTROL_DEFINITIONS, detectados por el stream CONTROL_DEFINITIONS_CHANGES_STREAM.
-- Llama al procedimiento DISPATCH_CONTROL_CHANGES para procesarlos.
-- ===================================================================================

CREATE OR REPLACE TASK TASK_DISPATCH_CONTROL_CHANGES
  WAREHOUSE = TFG_F1_WH  -- Reemplaza por el warehouse adecuado
  --SCHEDULE:  10 * * * ...
  WHEN SYSTEM$STREAM_HAS_DATA('CONTROL_DEFINITIONS_CHANGES_STREAM')
AS
  CALL CONTROL_CHANGES_PR();

-- ===================================================================================
-- PROCEDURE: CONTROL_CHANGES_PR
-- Dispatcher principal: Detecta cambios en el stream de CONTROL_DEFINITIONS
-- y delega en los procedimientos específicos según sea una inserción o eliminación.
-- ===================================================================================

CREATE OR REPLACE PROCEDURE CONTROL_CHANGES_PR()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
var createTempTable = snowflake.createStatement({
  sqlText: `CREATE OR REPLACE TEMPORARY TABLE CONTROL_DEFINITION_CHANGES_BUFFER AS
            SELECT * FROM CONTROL_DEFINITIONS_CHANGES_STREAM`
});
createTempTable.execute();

var stream_sql = `SELECT * FROM CONTROL_DEFINITION_CHANGES_BUFFER`;
var stmt = snowflake.createStatement({sqlText: stream_sql});
var rs = stmt.execute();

while (rs.next()) {
  var action = rs.getColumnValue("METADATA$ACTION");
  var checkId = rs.getColumnValue("CHECK_ID");

  if (action === "DELETE") {
    var deleteProc = snowflake.createStatement({
      sqlText: `CALL DROP_CHECK_PR(?)`,
      binds: [checkId]
    });
    deleteProc.execute();

  } else if (action === "INSERT") {
    var createProc = snowflake.createStatement({
      sqlText: `CALL CREATE_REPLACE_CHECK_PR(?)`,
      binds: [checkId]
    });
    createProc.execute();
  }
}

return "Cambios procesados correctamente";
$$;



-- ===================================================================================
-- PROCEDURE: DROP_CHECK_PR
-- Borra la task asociada (según su nombre) y elimina el registro de definición
-- a partir del CHECK_ID.
-- ===================================================================================

CREATE OR REPLACE PROCEDURE DROP_CHECK_PR(CHECK_ID STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
var sql_command = `
  SELECT CHECK_NAME
  FROM CONTROL_DEFINITIONS
  WHERE CHECK_ID = ?;
`;
var stmt = snowflake.createStatement({sqlText: sql_command, binds: [CHECK_ID]});
var result = stmt.execute();

if (!result.next()) {
  return 'No se encontró el CHECK_ID: ' + CHECK_ID;
}

var check_name = result.getColumnValue(1);

// Eliminar la task si existe
var drop_task = snowflake.createStatement({
  sqlText: `DROP TASK IF EXISTS IDENTIFIER(?)`,
  binds: [check_name]
});
drop_task.execute();

// Eliminar la fila del control
var delete_control = snowflake.createStatement({
  sqlText: `DELETE FROM CONTROL_DEFINITIONS WHERE CHECK_ID = ?`,
  binds: [CHECK_ID]
});
delete_control.execute();

return 'Task eliminada y control borrado: ' + check_name;
$$;




-- ===================================================================================
-- PROCEDURE: CREATE_REPLACE_CHECK_PR
-- Esta procedure recibe un CHECK_ID y en función del CHECK_TYPE asociado a ese ID
-- llama al procedimiento específico que genera la task del control correspondiente.
-- Actualmente soporta los tipos: FRESHNESS, UNIQUENESS y CONSISTENCY.
-- ===================================================================================

CREATE OR REPLACE PROCEDURE CREATE_REPLACE_CHECK_PR(CHECK_ID STRING, CHECK_NAME STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
var check_type_sql = `
  SELECT CHECK_TYPE
  FROM CONTROL_DEFINITIONS
  WHERE CHECK_ID = ?;
`;

var stmt = snowflake.createStatement({sqlText: check_type_sql, binds: [CHECK_ID]});
var rs = stmt.execute();

if (!rs.next()) {
  return 'No se encontró el CHECK_ID: ' + CHECK_ID;
}

var check_type = rs.getColumnValue(1);
var mensaje = '';

if (check_type === 'FRESHNESS') {
  snowflake.execute({sqlText: `CALL CREATE_TASK_FRESHNESS(?)`, binds: [CHECK_ID]});
  mensaje = 'Task de tipo FRESHNESS creada';

} else if (check_type === 'UNIQUENESS') {
  snowflake.execute({sqlText: `CALL CREATE_TASK_UNIQUENESS(?)`, binds: [CHECK_ID]});
  mensaje = 'Task de tipo UNIQUENESS creada';

} else if (check_type === 'CONSISTENCY') {
  snowflake.execute({sqlText: `CALL CREATE_TASK_CONSISTENCY(?)`, binds: [CHECK_ID]});
  mensaje = 'Task de tipo CONSISTENCY creada';

} else {
  mensaje = 'Tipo de check no soportado: ' + check_type;
}

return mensaje;
$$;






