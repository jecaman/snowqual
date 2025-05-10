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
var stream_sql = `SELECT * FROM CONTROL_DEFINITIONS_CHANGES_STREAM`;
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
LANGUAGE SQL
AS
$$
DECLARE
  CHECK_NAME STRING;
BEGIN
  -- Obtener el nombre de la task a partir del CHECK_ID
  SELECT CHECK_NAME INTO CHECK_NAME
  FROM CONTROL_DEFINITIONS
  WHERE CHECK_ID = :CHECK_ID;

  -- Eliminar la task si existe
  EXECUTE IMMEDIATE 'DROP TASK IF EXISTS IDENTIFIER(:1)' USING CHECK_NAME;

  -- Eliminar la fila del control
  EXECUTE IMMEDIATE 'DELETE FROM CONTROL_DEFINITIONS WHERE CHECK_ID = :1' USING CHECK_ID;

  RETURN 'Task eliminada y control borrado: ' || CHECK_NAME;
END;
$$;



-- ===================================================================================
-- PROCEDURE: CREATE_REPLACE_CHECK_PR
-- Esta procedure recibe un CHECK_ID y en función del CHECK_TYPE asociado a ese ID
-- llama al procedimiento específico que genera la task del control correspondiente.
-- Actualmente soporta los tipos: FRESHNESS, UNIQUENESS y CONSISTENCY.
-- ===================================================================================

CREATE OR REPLACE PROCEDURE CREATE_REPLACE_CHECK_PR(CHECK_ID STRING, CHECK_NAME STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  check_type STRING;
  mensaje STRING;
BEGIN
  -- Recuperamos el tipo de control a partir del ID
  SELECT CHECK_TYPE INTO check_type
  FROM CONTROL_DEFINITIONS
  WHERE CHECK_ID = :CHECK_ID;

  -- Enrutamos según el tipo de check
  IF check_type = 'FRESHNESS' THEN
    CALL CREATE_TASK_FRESHNESS(CHECK_ID);
    LET mensaje = 'Task de tipo FRESHNESS creada';

  ELSIF check_type = 'UNIQUENESS' THEN
    CALL CREATE_TASK_UNIQUENESS(CHECK_ID);
    LET mensaje = 'Task de tipo UNIQUENESS creada';

  ELSIF check_type = 'CONSISTENCY' THEN
    CALL CREATE_TASK_CONSISTENCY(CHECK_ID);
    LET mensaje = 'Task de tipo CONSISTENCY creada';

  ELSE
    LET mensaje = 'Tipo de check no soportado: ' || check_type;
  END IF;

  RETURN mensaje;
END;
$$;





