const express = require('express');
const cors = require('cors');
const oracledb = require('oracledb');
const sql = require('mssql');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 8080;

oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;
oracledb.autoCommit = true;

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, '..', 'frontend', 'build')));

// Ruta base de prueba
app.get('/', (req, res) => {
  res.send('🚀 Servidor funcionando correctamente en Railway');
});

app.listen(PORT, () => console.log(`Servidor corriendo en puerto ${PORT}`));

const dbConfigs = {
  capital: {
    user: 'CAPITAL',
    password: 'CAPITAL123',
    connectString: 'localhost:1521/XE'
  },
  coatepeque: {
    user: 'COATEPEQUE', 
    password: 'COATEPEQUE123',
    connectString: 'localhost:1521/XE'
  }
};

const sqlConfig = {
  user: 'Central',             
  password: 'Central123',      
  server: 'DANIEL',            
  database: 'BD_SOLIDARISMO_CENTRAL', 
  port: 1433,
  options: {
    encrypt: false,               
    trustServerCertificate: true  
  },
  port: 1433,
};

(async () => {
  try {
    let pool = await sql.connect(sqlConfig);
    console.log("Conectado correctamente a SQL Server (Central)");
    await pool.close();
  } catch (err) {
    console.error("Error conectando a SQL Server:", err);
  }
})();


const { getConnection, initializePools } = require('./config/database');


initializePools()
  .then(() => console.log('Pools inicializados correctamente'))
  .catch(err => console.error('Error inicializando pools:', err));


const mapTipoAhorroToDB = (tipo) => {
  const mapping = {
    'voluntario': 'EXTRAORDINARIO',
    'obligatorio': 'QUINCENAL',
    'bonificacion': 'BONIFICACION'
  };
  return mapping[tipo] || 'EXTRAORDINARIO';
};

const mapTipoAhorroFromDB = (tipoDB) => {
  const mapping = {
    'QUINCENAL': 'obligatorio',
    'EXTRAORDINARIO': 'voluntario',
    'BONIFICACION': 'bonificacion'
  };
  return mapping[tipoDB] || 'voluntario';
};

app.get('/api/tipos-ahorro-permitidos', async (req, res) => {
  const { database } = req.query;
  
  const tiposValidos = [
    { valor_db: 'QUINCENAL', valor_frontend: 'obligatorio', descripcion: 'Ahorro quincenal obligatorio' },
    { valor_db: 'EXTRAORDINARIO', valor_frontend: 'voluntario', descripcion: 'Ahorro extraordinario voluntario' },
    { valor_db: 'BONIFICACION', valor_frontend: 'bonificacion', descripcion: 'Ahorro por bonificación' }
  ];
  
  res.json({
    success: true,
    database: database,
    tipos_permitidos: tiposValidos,
    restricciones: {
      capital: 'SYS_C008970',
      coatepeque: 'SYS_C008909',
      descripcion: 'CHECK CONSTRAINT que limita TIPO_AHORRO a valores específicos'
    }
  });
});

app.get('/api/test-connections', async (req, res) => {
  const results = {};
  
  for (const [dbName, config] of Object.entries(dbConfigs)) {
    let connection;
    try {
      console.log(`Probando conexión a ${dbName}...`);
      connection = await oracledb.getConnection(config);
      
      const result = await connection.execute('SELECT 1 as TEST FROM DUAL');
      
      let tableExists = false;
      try {
        const tableCheck = await connection.execute(
          `SELECT COUNT(*) as count FROM user_tables WHERE table_name = 'EMPLEADOS'`
        );
        tableExists = tableCheck.rows[0].COUNT > 0;
      } catch (tableError) {
        tableExists = false;
      }
      
      results[dbName] = {
        success: true,
        test: result.rows[0].TEST,
        user: config.user,
        table_empleados: tableExists,
        message: `Conexión exitosa a ${dbName}${tableExists ? ' (con tabla EMPLEADOS)' : ' (sin tabla EMPLEADOS)'}`
      };
      
    } catch (error) {
      results[dbName] = {
        success: false,
        user: config.user,
        message: `Error: ${error.message}`
      };
    } finally {
      if (connection) {
        try {
          await connection.close();
        } catch (closeError) {
          console.error('Error cerrando conexión:', closeError);
        }
      }
    }
  }
    res.json({
    success: true,
    connections: results,
    timestamp: new Date().toISOString()
  });
});

app.post('/api/login', async (req, res) => {
  const { usuario, contrasena, database } = req.body;

  console.log(`Intento de login: ${usuario} @ ${database}`);

  if (!usuario || !contrasena || !database) {
    return res.status(400).json({
      success: false,
      message: 'Usuario, contraseña y sucursal son requeridos'
    });
  }

  let connection;
  try {
    connection = await getConnection(database);
    
    console.log(`Conectado a ${database}, verificando credenciales...`);

    const result = await connection.execute(
      `SELECT ID_EMPLEADO, CODIGO_EMPLEADO, NOMBRE, APELLIDO, 
              USUARIO, TIPO_USUARIO, ESTADO
       FROM EMPLEADOS 
       WHERE USUARIO = :usuario 
       AND CONTRASENA = :contrasena 
       AND ESTADO = 1`,
      { usuario, contrasena }
    );

    if (result.rows.length === 0) {
      return res.status(401).json({
        success: false,
        message: 'Credenciales incorrectas o usuario inactivo'
      });
    }

    const userData = result.rows[0];
    const user = {
      id: userData.ID_EMPLEADO,
      codigo_empleado: userData.CODIGO_EMPLEADO,
      nombre: userData.NOMBRE,
      apellido: userData.APELLIDO,
      usuario: userData.USUARIO,
      tipo_usuario: userData.TIPO_USUARIO,
      estado: userData.ESTADO
    };

    try {
      await connection.execute(
        `INSERT INTO BITACORA (TABLA_AFECTADA, ACCION, DESCRIPCION, USUARIO_RESPONSABLE)
         VALUES ('SISTEMA', 'LOGIN', 'Inicio de sesión exitoso', :usuario)`,
        { usuario }
      );
      await connection.commit();
      console.log('Registro en bitácora exitoso');
    } catch (bitacoraError) {
      console.log('No se pudo registrar en bitácora:', bitacoraError.message);
    }

    console.log(`Login exitoso para: ${user.nombre} ${user.apellido}`);
    
    res.json({
      success: true,
      user: user,
      message: 'Login exitoso'
    });

  } catch (error) {
    console.error('Error en login:', error);
    res.status(500).json({
      success: false,
      message: 'Error interno del servidor: ' + error.message
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});

app.get('/api/dashboard/estadisticas-detalladas', async (req, res) => {
  const { database } = req.query;

  console.log(`Solicitando estadísticas para sucursal: ${database}`);

  if (!database) {
    return res.status(400).json({
      success: false,
      error: 'Parámetro database es requerido'
    });
  }

  let connection;
  try {
    connection = await getConnection(database);

    const [
      empleadosResult,
      ahorrosResult,
      prestamosResult,
      planillasResult
    ] = await Promise.all([
      connection.execute(`
        SELECT 
          COUNT(CASE WHEN ESTADO = 1 THEN 1 END) as empleados_activos,
          COUNT(*) as total_empleados
        FROM EMPLEADOS
      `),
      
      connection.execute(`
        SELECT 
          NVL(SUM(MONTO_AHORRO), 0) as total_ahorros,
          NVL(AVG(MONTO_AHORRO), 0) as ahorro_promedio
        FROM AHORROS 
        WHERE ESTADO = 1
      `),
      
      connection.execute(`
        SELECT 
          COUNT(CASE WHEN ESTADO = 'APROBADO' THEN 1 END) as prestamos_activos,
          COUNT(CASE WHEN ESTADO = 'SOLICITADO' THEN 1 END) as prestamos_pendientes
        FROM PRESTAMOS
      `),
      
      connection.execute(`
        SELECT COUNT(*) as planillas_procesadas
        FROM PLANILLAS 
        WHERE EXTRACT(YEAR FROM FECHA_PROCESAMIENTO) = EXTRACT(YEAR FROM SYSDATE)
        AND ESTADO = 'PROCESADA'
      `)
    ]);

    const stats = {
      empleadosActivos: empleadosResult.rows[0].EMPLEADOS_ACTIVOS || 0,
      totalEmpleados: empleadosResult.rows[0].TOTAL_EMPLEADOS || 0,
      totalAhorros: parseFloat(ahorrosResult.rows[0].TOTAL_AHORROS) || 0,
      ahorroPromedio: parseFloat(ahorrosResult.rows[0].AHORRO_PROMEDIO) || 0,
      prestamosActivos: prestamosResult.rows[0].PRESTAMOS_ACTIVOS || 0,
      prestamosPendientes: prestamosResult.rows[0].PRESTAMOS_PENDIENTES || 0,
      planillasProcesadas: planillasResult.rows[0].PLANILLAS_PROCESADAS || 0
    };

    console.log(`Estadísticas para ${database}:`, stats);

    res.json({
      success: true,
      data: stats
    });

  } catch (error) {
    console.error(`Error obteniendo estadísticas para ${database}:`, error);
    res.status(500).json({
      success: false,
      error: 'Error obteniendo estadísticas: ' + error.message
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});

app.get('/api/dashboard/actividad-reciente', async (req, res) => {
  const { database, limite = 5 } = req.query;

  console.log(`Solicitando actividad reciente para sucursal: ${database}`);

  if (!database) {
    return res.status(400).json({
      success: false,
      error: 'Parámetro database es requerido'
    });
  }

  let connection;
  try {
    connection = await getConnection(database);

    let result;
    try {
      result = await connection.execute(`
        SELECT 
          ID_BITACORA as id,
          TABLA_AFECTADA as tabla,
          ACCION as accion, 
          DESCRIPCION as descripcion, 
          USUARIO_RESPONSABLE as usuario,
          FECHA_EVENTO as fecha_evento,
          'EXITOSO' as estado
        FROM BITACORA 
        ORDER BY FECHA_EVENTO DESC 
        FETCH FIRST :limite ROWS ONLY
      `, { limite: parseInt(limite) });
    } catch (error) {
      console.log('Tabla BITACORA no disponible, usando consulta alternativa');
      result = await connection.execute(`
        SELECT 
          ROWNUM as id,
          'SISTEMA' as tabla,
          'ACTIVIDAD_SISTEMA' as accion,
          'Actividad del sistema en ' || :database as descripcion,
          'ADMIN' as usuario,
          SYSDATE - (ROWNUM/24) as fecha_evento,
          'EXITOSO' as estado
        FROM DUAL 
        CONNECT BY ROWNUM <= :limite
      `, { database, limite: parseInt(limite) });
    }

    const actividad = result.rows.map(row => ({
      id: row.ID,
      tabla: row.TABLA,
      accion: row.ACCION,
      descripcion: row.DESCRIPCION,
      usuario: row.USUARIO,
      fecha_evento: row.FECHA_EVENTO,
      estado: row.ESTADO
    }));

    console.log(`Actividad reciente para ${database}: ${actividad.length} registros`);

    res.json({
      success: true,
      data: actividad
    });

  } catch (error) {
    console.error(`Error obteniendo actividad reciente para ${database}:`, error);
    res.status(500).json({
      success: false,
      error: 'Error obteniendo actividad: ' + error.message
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});

app.get('/api/dashboard/resumen-general', async (req, res) => {
  const { database } = req.query;

  if (!database) {
    return res.status(400).json({
      success: false,
      error: 'Parámetro database es requerido'
    });
  }

  let connection;
  try {
    connection = await getConnection(database);

    const resumenQuery = `
      SELECT 
        -- Empleados
        (SELECT COUNT(*) FROM EMPLEADOS WHERE ESTADO = 1) as empleados_activos,
        (SELECT COUNT(*) FROM EMPLEADOS) as total_empleados,
        
        -- Ahorros
        (SELECT NVL(SUM(MONTO_AHORRO), 0) FROM AHORROS WHERE ESTADO = 1) as total_ahorros,
        (SELECT NVL(AVG(MONTO_AHORRO), 0) FROM AHORROS WHERE ESTADO = 1) as ahorro_promedio,
        
        -- Préstamos
        (SELECT COUNT(*) FROM PRESTAMOS WHERE ESTADO = 'APROBADO') as prestamos_activos,
        (SELECT COUNT(*) FROM PRESTAMOS WHERE ESTADO = 'SOLICITADO') as prestamos_pendientes,
        
        -- Planillas
        (SELECT COUNT(*) FROM PLANILLAS 
         WHERE EXTRACT(YEAR FROM FECHA_PROCESAMIENTO) = EXTRACT(YEAR FROM SYSDATE)
         AND ESTADO = 'PROCESADA') as planillas_procesadas
        
        -- ELIMINADO: SALDO_ACTUAL ya que no existe en la tabla
      FROM DUAL
    `;

    const result = await connection.execute(resumenQuery);
    const row = result.rows[0];

    const resumen = {
      empleadosActivos: row.EMPLEADOS_ACTIVOS || 0,
      totalEmpleados: row.TOTAL_EMPLEADOS || 0,
      totalAhorros: parseFloat(row.TOTAL_AHORROS) || 0,
      ahorroPromedio: parseFloat(row.AHORRO_PROMEDIO) || 0,
      prestamosActivos: row.PRESTAMOS_ACTIVOS || 0,
      prestamosPendientes: row.PRESTAMOS_PENDIENTES || 0,
      planillasProcesadas: row.PLANILLAS_PROCESADAS || 0
    };

    res.json({
      success: true,
      data: resumen
    });

  } catch (error) {
    console.error('Error obteniendo resumen general:', error);
    res.status(500).json({
      success: false,
      error: 'Error obteniendo resumen: ' + error.message
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});

app.get('/api/dashboard/datos-graficos', async (req, res) => {
  const { database } = req.query;

  if (!database) {
    return res.status(400).json({
      success: false,
      error: 'Parámetro database es requerido'
    });
  }

  let connection;
  try {
    connection = await getConnection(database);

    const ahorrosMensuales = await connection.execute(`
      SELECT 
        TO_CHAR(FECHA_AHORRO, 'YYYY-MM') as mes,
        SUM(MONTO_AHORRO) as total
      FROM AHORROS 
      WHERE FECHA_AHORRO >= ADD_MONTHS(SYSDATE, -12)
      AND ESTADO = 1
      GROUP BY TO_CHAR(FECHA_AHORRO, 'YYYY-MM')
      ORDER BY mes
    `);

    const prestamosPorEstado = await connection.execute(`
      SELECT 
        ESTADO as estado,
        COUNT(*) as cantidad
      FROM PRESTAMOS 
      GROUP BY ESTADO
    `);

    const empleadosPorTipo = await connection.execute(`
      SELECT 
        TIPO_USUARIO as tipo,
        COUNT(*) as cantidad
      FROM EMPLEADOS 
      WHERE ESTADO = 1
      GROUP BY TIPO_USUARIO
    `);

    const datosGraficos = {
      ahorrosMensuales: ahorrosMensuales.rows,
      prestamosPorEstado: prestamosPorEstado.rows,
      empleadosPorTipo: empleadosPorTipo.rows
    };

    res.json({
      success: true,
      data: datosGraficos
    });

  } catch (error) {
    console.error('Error obteniendo datos para gráficos:', error);
    res.status(500).json({
      success: false,
      error: 'Error obteniendo datos gráficos: ' + error.message
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});

app.post('/api/logout', async (req, res) => {
  const { userId, database } = req.body;

  if (!database) {
    return res.status(400).json({
      success: false,
      message: 'Database es requerido'
    });
  }

  let connection;
  try {
    connection = await getConnection(database);

    if (userId) {
      try {
        await connection.execute(
          `INSERT INTO BITACORA (TABLA_AFECTADA, ACCION, DESCRIPCION, USUARIO_RESPONSABLE)
           VALUES ('SISTEMA', 'LOGOUT', 'Cierre de sesión', :userId)`,
          { userId }
        );
        await connection.commit();
      } catch (bitacoraError) {
        console.error('Error registrando logout en bitácora:', bitacoraError);
      }
    }

    res.json({
      success: true,
      message: 'Logout exitoso'
    });

  } catch (error) {
    console.error('Error en logout:', error);
    res.status(500).json({
      success: false,
      message: 'Error en logout: ' + error.message
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});

app.get('/api/empleados', async (req, res) => {
  const { database } = req.query;

  if (!database) {
    return res.status(400).json({
      success: false,
      error: 'Parámetro database es requerido'
    });
  }

  let connection;
  try {
    connection = await getConnection(database);

    const result = await connection.execute(`
      SELECT ID_EMPLEADO, CODIGO_EMPLEADO, NOMBRE, APELLIDO, DPI,
             SALARIO_BASE, PORCENTAJE_AHORRO, FECHA_INGRESO,
             USUARIO, TIPO_USUARIO, ESTADO
      FROM EMPLEADOS 
      ORDER BY CODIGO_EMPLEADO
    `);

    const empleados = result.rows.map(row => ({
      id: row.ID_EMPLEADO,
      codigo_empleado: row.CODIGO_EMPLEADO,
      nombre: row.NOMBRE,
      apellido: row.APELLIDO,
      dpi: row.DPI,
      salario_base: parseFloat(row.SALARIO_BASE),
      porcentaje_ahorro: parseFloat(row.PORCENTAJE_AHORRO),
      fecha_ingreso: row.FECHA_INGRESO,
      usuario: row.USUARIO,
      tipo_usuario: row.TIPO_USUARIO,
      estado: row.ESTADO
    }));

    res.json({
      success: true,
      data: empleados
    });

  } catch (error) {
    console.error('Error obteniendo empleados:', error);
    res.status(500).json({
      success: false,
      error: 'Error obteniendo empleados: ' + error.message
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});

app.post('/api/empleados', async (req, res) => {
  const { database } = req.query;
  const { 
    codigo_empleado, 
    nombre, 
    apellido, 
    dpi, 
    salario_base, 
    porcentaje_ahorro, 
    fecha_ingreso,
    contrasena
  } = req.body;

  console.log('Creando nuevo empleado:', { codigo_empleado, nombre, apellido, database });

  if (!database) {
    return res.status(400).json({
      success: false,
      error: 'Parámetro database es requerido'
    });
  }

  if (!codigo_empleado || !nombre || !apellido || !dpi || !salario_base || !fecha_ingreso || !contrasena) {
    return res.status(400).json({
      success: false,
      error: 'Todos los campos marcados con * son requeridos'
    });
  }

  if (dpi.length !== 13) {
    return res.status(400).json({
      success: false,
      error: 'El DPI debe tener exactamente 13 dígitos'
    });
  }

  let connection;
  try {
    connection = await getConnection(database);

    const checkResult = await connection.execute(
      `SELECT COUNT(*) AS COUNT FROM EMPLEADOS WHERE CODIGO_EMPLEADO = :codigo_empleado`,
      { codigo_empleado },
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    if (checkResult.rows[0].COUNT > 0) {
      return res.status(400).json({
        success: false,
        error: 'El código de empleado ya existe'
      });
    }

    const checkDPI = await connection.execute(
      `SELECT COUNT(*) AS COUNT FROM EMPLEADOS WHERE DPI = :dpi`,
      { dpi },
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    if (checkDPI.rows[0].COUNT > 0) {
      return res.status(400).json({
        success: false,
        error: 'El DPI ya está registrado'
      });
    }

    const usuario = codigo_empleado.toLowerCase();

    const result = await connection.execute(
      `INSERT INTO EMPLEADOS (
        CODIGO_EMPLEADO, NOMBRE, APELLIDO, DPI, 
        SALARIO_BASE, PORCENTAJE_AHORRO, FECHA_INGRESO,
        USUARIO, CONTRASENA, TIPO_USUARIO, ESTADO
      ) VALUES (
        :codigo_empleado, :nombre, :apellido, :dpi,
        :salario_base, :porcentaje_ahorro, TO_DATE(:fecha_ingreso, 'YYYY-MM-DD'),
        :usuario, :contrasena, 'EMPLEADO', 1
      )`,
      {
        codigo_empleado,
        nombre, 
        apellido, 
        dpi,
        salario_base: parseFloat(salario_base),
        porcentaje_ahorro: porcentaje_ahorro ? parseFloat(porcentaje_ahorro) : 5.00,
        fecha_ingreso,
        usuario,
        contrasena,
      }
    );

    await connection.commit();

    try {
      await connection.execute(
        `INSERT INTO BITACORA (TABLA_AFECTADA, ACCION, DESCRIPCION, USUARIO_RESPONSABLE)
         VALUES ('EMPLEADOS', 'INSERT', 'Nuevo empleado creado: ' || :codigo_empleado, 'SISTEMA')`,
        { codigo_empleado }
      );
      await connection.commit();
    } catch (bitacoraError) {
      console.log('No se pudo registrar en bitácora:', bitacoraError.message);
    }

    res.json({
      success: true,
      message: 'Empleado creado exitosamente',
      data: {
        usuario
      }
    });

  } catch (error) {
    console.error('Error creando empleado:', error);
    
    if (connection) {
      try {
        await connection.rollback();
      } catch (rollbackError) {
        console.error('Error en rollback:', rollbackError);
      }
    }
    res.status(500).json({
      success: false,
      error: 'Error creando empleado: ' + error.message
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});

app.put('/api/empleados/:id', async (req, res) => {
  const { id } = req.params;
  const database = req.query.database || 'capital';
  const {
    nombre,
    apellido,
    salario_base,
    porcentaje_ahorro,
    fecha_ingreso
  } = req.body;

  try {
    const connection = await getConnection(database);
    
    const empleadoExists = await connection.execute(
      `SELECT * FROM empleados WHERE id_empleado = :id`,
      { id: parseInt(id) }
    );

    if (empleadoExists.rows.length === 0) {
      return res.status(404).json({
        success: false,
        error: 'Empleado no encontrado'
      });
    }

    const result = await connection.execute(
      `UPDATE empleados 
       SET nombre = :nombre, 
           apellido = :apellido, 
           salario_base = :salario_base, 
           porcentaje_ahorro = :porcentaje_ahorro,
           fecha_ingreso = TO_DATE(:fecha_ingreso, 'YYYY-MM-DD')
       WHERE id_empleado = :id`,
      {
        nombre,
        apellido,
        salario_base,
        porcentaje_ahorro: porcentaje_ahorro || 5.00,
        fecha_ingreso,
        id: parseInt(id)
      }
    );

    await connection.execute(
      `INSERT INTO bitacora (tabla_afectada, id_registro_afectado, accion, descripcion, usuario_responsable) 
       VALUES ('EMPLEADOS', :id, 'UPDATE', 'Actualización de empleado', 'SISTEMA')`,
      { id: parseInt(id) }
    );

    await connection.commit();

    res.json({
      success: true,
      message: 'Empleado actualizado exitosamente',
      data: { id: parseInt(id) }
    });

  } catch (error) {
    console.error('Error actualizando empleado:', error);
    res.status(500).json({
      success: false,
      error: 'Error interno del servidor al actualizar empleado'
    });
  }
});

app.delete('/api/empleados/:id', async (req, res) => {
  const { id } = req.params;
  const database = req.query.database || 'capital';

  try {
    const connection = await getConnection(database);
    
    const empleadoResult = await connection.execute(
      `SELECT * FROM empleados WHERE id_empleado = :id`,
      { id: parseInt(id) }
    );

    if (empleadoResult.rows.length === 0) {
      return res.status(404).json({
        success: false,
        error: 'Empleado no encontrado'
      });
    }

    const result = await connection.execute(
      `UPDATE empleados SET estado = 0 WHERE id_empleado = :id`,
      { id: parseInt(id) }
    );

    await connection.execute(
      `INSERT INTO bitacora (tabla_afectada, id_registro_afectado, accion, descripcion, usuario_responsable) 
       VALUES ('EMPLEADOS', :id, 'DELETE', 'Empleado marcado como inactivo', 'SISTEMA')`,
      { id: parseInt(id) }
    );

    await connection.commit();

    res.json({
      success: true,
      message: 'Empleado eliminado (marcado como inactivo) exitosamente'
    });

  } catch (error) {
    console.error('Error eliminando empleado:', error);
    res.status(500).json({
      success: false,
      error: 'Error interno del servidor al eliminar empleado'
    });
  }
});

app.get('/api/planillas', async (req, res) => {
  const { database } = req.query;

  if (!database) {
    return res.status(400).json({
      success: false,
      error: 'Parámetro database es requerido'
    });
  }

  let connection;
  try {
    connection = await getConnection(database);

    console.log(`Consultando planillas en ${database}...`);

    const result = await connection.execute(`
      SELECT ID_PLANILLA, PERIODO, FECHA_PROCESAMIENTO,
             TOTAL_INGRESOS, TOTAL_DESCUENTOS, TOTAL_NETO, ESTADO
      FROM PLANILLAS 
      ORDER BY FECHA_PROCESAMIENTO DESC
    `);

    console.log(`${result.rows.length} planillas encontradas en BD`);

    const planillas = result.rows.map(row => {
      console.log(`Planilla ID ${row.ID_PLANILLA}:`, {
        periodo: row.PERIODO,
        fecha_procesamiento: row.FECHA_PROCESAMIENTO,
        total_neto: row.TOTAL_NETO,
        estado: row.ESTADO
      });
      
      return {
        id: row.ID_PLANILLA,
        periodo: row.PERIODO,
        fecha_procesamiento: row.FECHA_PROCESAMIENTO,
        total_ingresos: parseFloat(row.TOTAL_INGRESOS) || 0,
        total_descuentos: parseFloat(row.TOTAL_DESCUENTOS) || 0,
        total_neto: parseFloat(row.TOTAL_NETO) || 0,
        estado: row.ESTADO || 'PENDIENTE'
      };
    });

    res.json({
      success: true,
      data: planillas,
      count: planillas.length,
      message: `${planillas.length} planillas cargadas exitosamente`
    });

  } catch (error) {
    console.error('Error obteniendo planillas:', error);
    
    if (error.message.includes('ORA-00942')) {
      console.log('Tabla PLANILLAS no existe, devolviendo array vacío');
      return res.json({
        success: true,
        data: [],
        message: 'Tabla PLANILLAS no existe'
      });
    }

    res.status(500).json({
      success: false,
      error: 'Error obteniendo planillas: ' + error.message
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});

app.get('/api/diagnostic/planillas', async (req, res) => {
  const { database } = req.query;

  if (!database) {
    return res.status(400).json({
      success: false,
      error: 'Parámetro database es requerido'
    });
  }

  let connection;
  try {
    console.log(`Diagnosticando PLANILLAS para: ${database}`);
    
    connection = await getConnection(database);
    console.log('Conexión a BD exitosa');

    const tableCheck = await connection.execute(
      `SELECT COUNT(*) as table_count 
       FROM user_tables 
       WHERE table_name = 'PLANILLAS'`
    );

    const tableExists = tableCheck.rows[0].TABLE_COUNT > 0;
    
    if (!tableExists) {
      return res.json({
        success: false,
        error: 'La tabla PLANILLAS no existe en la base de datos'
      });
    }

    console.log('Tabla PLANILLAS existe');

    const columnsCheck = await connection.execute(
      `SELECT column_name, data_type, nullable 
       FROM user_tab_columns 
       WHERE table_name = 'PLANILLAS' 
       ORDER BY column_id`
    );

    console.log('Estructura de tabla verificada');

    const planillasCheck = await connection.execute(`
      SELECT ID_PLANILLA, PERIODO, FECHA_PROCESAMIENTO,
             TOTAL_INGRESOS, TOTAL_DESCUENTOS, TOTAL_NETO, ESTADO
      FROM PLANILLAS 
      ORDER BY FECHA_PROCESAMIENTO DESC
    `);

    console.log(`Consulta ejecutada: ${planillasCheck.rows.length} registros encontrados`);

    planillasCheck.rows.forEach((planilla, index) => {
      console.log(`Planilla ${index + 1}:`, {
        id: planilla.ID_PLANILLA,
        periodo: planilla.PERIODO,
        fecha_procesamiento: planilla.FECHA_PROCESAMIENTO,
        total_neto: planilla.TOTAL_NETO,
        estado: planilla.ESTADO
      });
    });

    res.json({
      success: true,
      tableExists: true,
      columnCount: columnsCheck.rows.length,
      recordCount: planillasCheck.rows.length,
      columns: columnsCheck.rows,
      sampleData: planillasCheck.rows,
      message: `Diagnóstico exitoso: ${planillasCheck.rows.length} registros en PLANILLAS`
    });

  } catch (error) {
    console.error('Error en diagnóstico:', error);
    res.status(500).json({
      success: false,
      error: 'Error en diagnóstico: ' + error.message
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});

app.get('/api/dashboard/estadisticas-detalladas', async (req, res) => {
  const { database } = req.query;

  console.log(`Solicitando estadísticas para sucursal: ${database}`);

  if (!database) {
    return res.status(400).json({
      success: false,
      error: 'Parámetro database es requerido'
    });
  }

  let connection;
  try {
    connection = await getConnection(database);

    const [
      empleadosResult,
      ahorrosResult,
      prestamosResult,
      planillasResult
    ] = await Promise.all([
      connection.execute(`
        SELECT 
          COUNT(CASE WHEN ESTADO = 1 THEN 1 END) as empleados_activos,
          COUNT(*) as total_empleados
        FROM EMPLEADOS
      `),
      
      connection.execute(`
        SELECT 
          NVL(SUM(MONTO_AHORRO), 0) as total_ahorros,
          NVL(AVG(MONTO_AHORRO), 0) as ahorro_promedio
        FROM AHORROS 
        WHERE ESTADO = 1
      `),
      
      connection.execute(`
        SELECT 
          COUNT(CASE WHEN ESTADO = 'APROBADO' THEN 1 END) as prestamos_activos,
          COUNT(CASE WHEN ESTADO = 'SOLICITADO' THEN 1 END) as prestamos_pendientes
        FROM PRESTAMOS
      `),
      
      connection.execute(`
        SELECT 
          COUNT(CASE WHEN FECHA_PROCESAMIENTO IS NOT NULL AND TOTAL_NETO > 0 THEN 1 END) as planillas_procesadas,
          COUNT(CASE WHEN FECHA_PROCESAMIENTO IS NOT NULL AND TOTAL_NETO = 0 THEN 1 END) as planillas_en_proceso,
          COUNT(CASE WHEN FECHA_PROCESAMIENTO IS NULL THEN 1 END) as planillas_pendientes
        FROM PLANILLAS
      `)
    ]);

    const stats = {
      empleadosActivos: empleadosResult.rows[0].EMPLEADOS_ACTIVOS || 0,
      totalEmpleados: empleadosResult.rows[0].TOTAL_EMPLEADOS || 0,
      totalAhorros: parseFloat(ahorrosResult.rows[0].TOTAL_AHORROS) || 0,
      ahorroPromedio: parseFloat(ahorrosResult.rows[0].AHORRO_PROMEDIO) || 0,
      prestamosActivos: prestamosResult.rows[0].PRESTAMOS_ACTIVOS || 0,
      prestamosPendientes: prestamosResult.rows[0].PRESTAMOS_PENDIENTES || 0,
      planillasProcesadas: planillasResult.rows[0].PLANILLAS_PROCESADAS || 0,
      planillasEnProceso: planillasResult.rows[0].PLANILLAS_EN_PROCESO || 0,
      planillasPendientes: planillasResult.rows[0].PLANILLAS_PENDIENTES || 0
    };

    console.log(`Estadísticas para ${database}:`, stats);

    res.json({
      success: true,
      data: stats
    });

  } catch (error) {
    console.error(`Error obteniendo estadísticas para ${database}:`, error);
    res.status(500).json({
      success: false,
      error: 'Error obteniendo estadísticas: ' + error.message
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});

app.post('/api/planillas/procesar', async (req, res) => {
  const { database } = req.query;
  const { periodo } = req.body;

  if (!database) {
    return res.status(400).json({
      success: false,
      error: 'Parámetro "database" es requerido (CAPITAL o COATEPEQUE)'
    });
  }

  if (!periodo || !/^\d{4}-\d{2}$/.test(periodo)) {
    return res.status(400).json({
      success: false,
      error: 'Debe especificar un período válido con formato YYYY-MM'
    });
  }

  const paquete =
    database.toUpperCase() === 'CAPITAL'
      ? 'PKG_GESTION_SOLIDARISMO_CAPITAL'
      : 'PKG_GESTION_SOLIDARISMO_COATEPEQUE';

  let connection;

  try {
    connection = await getConnection(database);
    console.log(`⚙️ Procesando planilla ${periodo} en base ${database} usando paquete ${paquete}`);

    const result = await connection.execute(
      `
      BEGIN
        ${paquete}.SP_PROCESAR_PLANILLA(:periodo, :resultado, :id_planilla);
      END;
      `,
      {
        periodo,
        resultado: { dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 500 },
        id_planilla: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER }
      }
    );

    // ✅ Capturar correctamente los OUT BINDS
    const out = result.outBinds || {};
    const mensaje = out.RESULTADO || out.resultado || 'SIN MENSAJE';
    const idPlanilla = out.ID_PLANILLA || out.id_planilla || null;

    console.log('📤 OUT BINDS Oracle:', out);
    console.log('🧾 ID_PLANILLA =>', idPlanilla, '| RESULTADO =>', mensaje);

    // ✅ Validar ID
    if (!idPlanilla) {
      console.warn('⚠️ No se devolvió ID_PLANILLA. Se usará el PERIODO para actualizar.');
    }

    // 🔹 Actualizar el estado a PROCESADA
    if (idPlanilla) {
      await connection.execute(
        `
        UPDATE PLANILLAS 
        SET ESTADO = 'PROCESADA',
            FECHA_PROCESAMIENTO = SYSDATE
        WHERE ID_PLANILLA = :id_planilla
        `,
        { id_planilla: idPlanilla }
      );
    } else {
      // respaldo por período
      await connection.execute(
        `
        UPDATE PLANILLAS 
        SET ESTADO = 'PROCESADA',
            FECHA_PROCESAMIENTO = SYSDATE
        WHERE PERIODO = :periodo
        `,
        { periodo }
      );
    }

    await connection.commit();
    console.log(`✅ Planilla ${idPlanilla || periodo} marcada como PROCESADA en ${database}`);

    res.json({
      success: true,
      message: 'Planilla procesada correctamente desde Oracle',
      data: {
        id_planilla: idPlanilla,
        periodo,
        base: database,
        resultado: mensaje
      }
    });

  } catch (error) {
    console.error('❌ Error al procesar planilla:', error);
    res.status(500).json({
      success: false,
      error: 'Error interno: ' + error.message
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (closeErr) {
        console.error('Error cerrando conexión:', closeErr);
      }
    }
  }
});


app.get('/api/ahorros', async (req, res) => {
  const { database } = req.query;

  console.log(`Solicitando ahorros para: ${database}`);

  if (!database) {
    return res.status(400).json({
      success: false,
      error: 'Parámetro database es requerido'
    });
  }

  let connection;
  try {
    console.log('1. Conectando a la base de datos...');
    connection = await getConnection(database);
    console.log('Conectado a la base de datos');

    console.log('2. Verificando tabla AHORROS...');
    let tableExists = true;
    try {
      const tableCheck = await connection.execute(
        `SELECT COUNT(*) as table_count 
         FROM user_tables 
         WHERE table_name = 'AHORROS'`
      );
      tableExists = tableCheck.rows[0].TABLE_COUNT > 0;
      console.log(`Tabla AHORROS existe: ${tableExists}`);
    } catch (tableError) {
      console.log('Error verificando tabla:', tableError.message);
      tableExists = false;
    }

    if (!tableExists) {
      console.log('Tabla AHORROS no existe, devolviendo array vacío');
      return res.json({
        success: true,
        data: [],
        message: 'Tabla AHORROS no existe'
      });
    }

    console.log('3. Ejecutando consulta de ahorros...');
    
    const result = await connection.execute(`
      SELECT 
        a.ID_AHORRO as id_ahorro,
        a.ID_EMPLEADO as id_empleado, 
        e.CODIGO_EMPLEADO as codigo_empleado, 
        e.NOMBRE || ' ' || e.APELLIDO as nombre_empleado,
        a.FECHA_AHORRO as fecha_ahorro, 
        a.MONTO_AHORRO as monto_ahorro, 
        a.TIPO_AHORRO as tipo_ahorro,
        a.PERIODO_PLANILLA as periodo_planilla, 
        a.ESTADO as estado
      FROM AHORROS a
      JOIN EMPLEADOS e ON a.ID_EMPLEADO = e.ID_EMPLEADO
      WHERE a.ESTADO = 1
      ORDER BY a.FECHA_AHORRO DESC
    `);

    console.log(`${result.rows.length} ahorros encontrados en BD`);

    const ahorros = result.rows.map(row => ({
      id_ahorro: row.ID_AHORRO,
      id_empleado: row.ID_EMPLEADO,
      codigo_empleado: row.CODIGO_EMPLEADO,
      nombre_empleado: row.NOMBRE_EMPLEADO,
      fecha_ahorro: row.FECHA_AHORRO,
      monto_ahorro: parseFloat(row.MONTO_AHORRO) || 0,
      tipo_ahorro: mapTipoAhorroFromDB(row.TIPO_AHORRO), 
      periodo_planilla: row.PERIODO_PLANILLA,
      estado: row.ESTADO
    }));

    console.log(`${ahorros.length} ahorros procesados`);

    res.json({
      success: true,
      data: ahorros,
      count: ahorros.length,
      message: `${ahorros.length} ahorros cargados exitosamente`
    });

  } catch (error) {
    console.error('❌ Error obteniendo ahorros:', error);
    
    let errorMessage = 'Error obteniendo ahorros: ' + error.message;
    
    if (error.message.includes('ORA-00942')) { 
      console.log('Tabla AHORROS no existe, devolviendo array vacío');
      return res.json({
        success: true,
        data: [],
        message: 'Tabla AHORROS no existe'
      });
    }
    
    if (error.message.includes('ORA-00904')) {
      errorMessage = 'Error en la estructura de la tabla AHORROS: ' + error.message;
    }

    res.status(500).json({
      success: false,
      error: errorMessage
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
        console.log('Conexión cerrada');
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});

app.post('/api/ahorros', async (req, res) => {
  const { database } = req.query;
  const { 
    id_empleado, 
    monto_ahorro, 
    tipo_ahorro, 
    periodo_planilla,
    fecha_ahorro,
    observaciones
  } = req.body;

  console.log('Creando nuevo ahorro:', { 
    id_empleado, 
    monto_ahorro, 
    tipo_ahorro, 
    periodo_planilla, 
    database 
  });

  if (!database) {
    return res.status(400).json({
      success: false,
      error: 'Parámetro database es requerido'
    });
  }

  if (!id_empleado || !monto_ahorro || !tipo_ahorro || !periodo_planilla) {
    return res.status(400).json({
      success: false,
      error: 'ID empleado, monto, tipo de ahorro y período son requeridos'
    });
  }

  if (monto_ahorro <= 0) {
    return res.status(400).json({
      success: false,
      error: 'El monto del ahorro debe ser mayor a 0'
    });
  }

  let connection;
  try {
    connection = await getConnection(database);

    const empleadoResult = await connection.execute(
      `SELECT CODIGO_EMPLEADO, NOMBRE, APELLIDO FROM EMPLEADOS 
       WHERE ID_EMPLEADO = :id_empleado AND ESTADO = 1`,
      { id_empleado: parseInt(id_empleado) }
    );

    if (empleadoResult.rows.length === 0) {
      return res.status(404).json({
        success: false,
        error: 'Empleado no encontrado o inactivo'
      });
    }

    const empleado = empleadoResult.rows[0];
    const fechaAhorro = fecha_ahorro || new Date().toISOString().split('T')[0];
    
    const tipoAhorroMapeado = mapTipoAhorroToDB(tipo_ahorro);

    const result = await connection.execute(
      `INSERT INTO AHORROS (
        ID_EMPLEADO, FECHA_AHORRO, MONTO_AHORRO, TIPO_AHORRO,
        PERIODO_PLANILLA, ESTADO, ID_DETALLE_PLANILLA
      ) VALUES (
        :id_empleado, TO_DATE(:fecha_ahorro, 'YYYY-MM-DD'), :monto_ahorro, :tipo_ahorro,
        :periodo_planilla, 1, NULL
      )`,
      {
        id_empleado: parseInt(id_empleado),
        fecha_ahorro: fechaAhorro,
        monto_ahorro: parseFloat(monto_ahorro),
        tipo_ahorro: tipoAhorroMapeado, 
        periodo_planilla: periodo_planilla
      }
    );

    await connection.commit();

    try {
      await connection.execute(
        `INSERT INTO BITACORA (TABLA_AFECTADA, ACCION, DESCRIPCION, USUARIO_RESPONSABLE)
         VALUES ('AHORROS', 'INSERT', 'Nuevo ahorro creado para: ' || :codigo_empleado || ' - Monto: ' || :monto, 'SISTEMA')`,
        { 
          codigo_empleado: empleado.CODIGO_EMPLEADO,
          monto: monto_ahorro 
        }
      );
      await connection.commit();
    } catch (bitacoraError) {
      console.log('No se pudo registrar en bitácora:', bitacoraError.message);
    }

    console.log(`Ahorro creado exitosamente para: ${empleado.NOMBRE} ${empleado.APELLIDO}`);
    
    res.json({
      success: true,
      message: 'Ahorro creado exitosamente',
      data: {
        id_empleado: parseInt(id_empleado),
        empleado: `${empleado.NOMBRE} ${empleado.APELLIDO}`,
        monto_ahorro: parseFloat(monto_ahorro),
        tipo_ahorro: tipo_ahorro,
        fecha_ahorro: fechaAhorro,
        periodo_planilla: periodo_planilla
      }
    });

  } catch (error) {
    console.error('Error creando ahorro:', error);
    
    if (connection) {
      try {
        await connection.rollback();
      } catch (rollbackError) {
        console.error('Error en rollback:', rollbackError);
      }
    }

    res.status(500).json({
      success: false,
      error: 'Error creando ahorro: ' + error.message
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});

app.post('/api/ahorros', async (req, res) => {
  const { database } = req.query;
  const { 
    id_empleado, 
    monto_ahorro, 
    tipo_ahorro, 
    periodo_planilla,
    fecha_ahorro,
    observaciones
  } = req.body;

  console.log('Registrando nuevo ahorro:', { 
    id_empleado, 
    monto_ahorro, 
    tipo_ahorro, 
    database 
  });

  if (!database) {
    return res.status(400).json({
      success: false,
      error: 'Parámetro database es requerido'
    });
  }

  if (!id_empleado || !monto_ahorro || !tipo_ahorro || !periodo_planilla) {
    return res.status(400).json({
      success: false,
      error: 'ID empleado, monto, tipo de ahorro y período son requeridos'
    });
  }

  if (monto_ahorro <= 0) {
    return res.status(400).json({
      success: false,
      error: 'El monto del ahorro debe ser mayor a 0'
    });
  }

  let connection;
  try {
    connection = await getConnection(database);

    const empleadoResult = await connection.execute(
      `SELECT CODIGO_EMPLEADO, NOMBRE, APELLIDO FROM EMPLEADOS 
       WHERE ID_EMPLEADO = :id_empleado AND ESTADO = 1`,
      { id_empleado: parseInt(id_empleado) }
    );

    if (empleadoResult.rows.length === 0) {
      return res.status(404).json({
        success: false,
        error: 'Empleado no encontrado o inactivo'
      });
    }

    const empleado = empleadoResult.rows[0];
    const fechaAhorro = fecha_ahorro || new Date().toISOString().split('T')[0];
    
    const tipoAhorroMapeado = mapTipoAhorroToDB(tipo_ahorro);

    const ahorroResult = await connection.execute(
      `INSERT INTO AHORROS (
        ID_EMPLEADO, FECHA_AHORRO, MONTO_AHORRO, TIPO_AHORRO,
        PERIODO_PLANILLA, ESTADO, ID_DETALLE_PLANILLA
      ) VALUES (
        :id_empleado, TO_DATE(:fecha_ahorro, 'YYYY-MM-DD'), :monto_ahorro, :tipo_ahorro,
        :periodo_planilla, 1, NULL
      ) RETURNING ID_AHORRO INTO :id_ahorro`,
      {
        id_empleado: parseInt(id_empleado),
        fecha_ahorro: fechaAhorro,
        monto_ahorro: parseFloat(monto_ahorro),
        tipo_ahorro: tipoAhorroMapeado, 
        periodo_planilla: periodo_planilla,
        id_ahorro: { type: oracledb.NUMBER, dir: oracledb.BIND_OUT }
      }
    );

    const idAhorro = ahorroResult.outBinds.id_ahorro[0];

    const descripcionMovimiento = `Ahorro ${tipo_ahorro} - ${empleado.NOMBRE} ${empleado.APELLIDO} - ${periodo_planilla}${observaciones ? ' - ' + observaciones : ''}`;
    
    const movimientoResult = await connection.execute(
      `INSERT INTO MOVIMIENTOS (
        ID_EMPLEADO, TIPO_MOVIMIENTO, DESCRIPCION, MONTO, 
        FECHA_MOVIMIENTO, ESTADO, ID_AHORRO
      ) VALUES (
        :id_empleado, 'INGRESO', :descripcion, :monto,
        SYSDATE, 'ACTIVO', :id_ahorro
      ) RETURNING ID_MOVIMIENTO INTO :id_movimiento`,
      {
        id_empleado: parseInt(id_empleado),
        descripcion: descripcionMovimiento,
        monto: parseFloat(monto_ahorro),
        id_ahorro: idAhorro,
        id_movimiento: { type: oracledb.NUMBER, dir: oracledb.BIND_OUT }
      }
    );

    const idMovimiento = movimientoResult.outBinds.id_movimiento[0];

    await connection.commit();

    try {
      await connection.execute(
        `INSERT INTO BITACORA (TABLA_AFECTADA, ACCION, DESCRIPCION, USUARIO_RESPONSABLE)
         VALUES ('AHORROS', 'INSERT', 'Nuevo ahorro creado para: ' || :codigo_empleado || ' - Monto: ' || :monto, 'SISTEMA')`,
        { 
          codigo_empleado: empleado.CODIGO_EMPLEADO,
          monto: monto_ahorro 
        }
      );
      await connection.commit();
    } catch (bitacoraError) {
      console.log('No se pudo registrar en bitácora:', bitacoraError.message);
    }

    console.log(`Ahorro creado exitosamente para: ${empleado.NOMBRE} ${empleado.APELLIDO}`);
    
    res.json({
      success: true,
      message: 'Ahorro registrado exitosamente',
      data: {
        id_ahorro: idAhorro,
        id_movimiento: idMovimiento,
        id_empleado: parseInt(id_empleado),
        empleado: `${empleado.NOMBRE} ${empleado.APELLIDO}`,
        monto_ahorro: parseFloat(monto_ahorro),
        tipo_ahorro: tipo_ahorro, 
        fecha_ahorro: fechaAhorro,
        periodo_planilla: periodo_planilla,
        tipo_movimiento: 'INGRESO'
      }
    });

  } catch (error) {
    console.error('Error creando ahorro:', error);
    
    if (connection) {
      try {
        await connection.rollback();
      } catch (rollbackError) {
        console.error('Error en rollback:', rollbackError);
      }
    }

    res.status(500).json({
      success: false,
      error: 'Error creando ahorro: ' + error.message
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});

app.put('/api/ahorros/:id', async (req, res) => {
  const { id } = req.params;
  const { database } = req.query;
  const { 
    monto_ahorro, 
    tipo_ahorro, 
    periodo_planilla,
    fecha_ahorro,
    estado 
  } = req.body;

  console.log('Actualizando ahorro:', { id, monto_ahorro, database });

  if (!database) {
    return res.status(400).json({
      success: false,
      error: 'Parámetro database es requerido'
    });
  }

  let connection;
  try {
    connection = await getConnection(database);

    const ahorroExists = await connection.execute(
      `SELECT a.*, e.CODIGO_EMPLEADO, e.NOMBRE, e.APELLIDO 
       FROM AHORROS a 
       JOIN EMPLEADOS e ON a.ID_EMPLEADO = e.ID_EMPLEADO 
       WHERE a.ID_AHORRO = :id`,
      { id: parseInt(id) }
    );

    if (ahorroExists.rows.length === 0) {
      return res.status(404).json({
        success: false,
        error: 'Ahorro no encontrado'
      });
    }

    const ahorroActual = ahorroExists.rows[0];

    const result = await connection.execute(
      `UPDATE AHORROS 
       SET MONTO_AHORRO = :monto_ahorro,
           TIPO_AHORRO = :tipo_ahorro,
           PERIODO_PLANILLA = :periodo_planilla,
           FECHA_AHORRO = TO_DATE(:fecha_ahorro, 'YYYY-MM-DD'),
           ESTADO = :estado
       WHERE ID_AHORRO = :id`,
      {
        monto_ahorro: monto_ahorro ? parseFloat(monto_ahorro) : ahorroActual.MONTO_AHORRO,
        tipo_ahorro: tipo_ahorro ? mapTipoAhorroToDB(tipo_ahorro) : ahorroActual.TIPO_AHORRO, 
        periodo_planilla: periodo_planilla || ahorroActual.PERIODO_PLANILLA,
        fecha_ahorro: fecha_ahorro || ahorroActual.FECHA_AHORRO.toISOString().split('T')[0],
        estado: estado !== undefined ? parseInt(estado) : ahorroActual.ESTADO,
        id: parseInt(id)
      }
    );

    await connection.commit();

    try {
      await connection.execute(
        `INSERT INTO BITACORA (TABLA_AFECTADA, ACCION, DESCRIPCION, USUARIO_RESPONSABLE)
         VALUES ('AHORROS', 'UPDATE', 'Ahorro actualizado ID: ' || :id || ' - Empleado: ' || :codigo_empleado, 'SISTEMA')`,
        { 
          id: id,
          codigo_empleado: ahorroActual.CODIGO_EMPLEADO
        }
      );
      await connection.commit();
    } catch (bitacoraError) {
      console.log('No se pudo registrar en bitácora:', bitacoraError.message);
    }

    console.log(`Ahorro actualizado exitosamente: ID ${id}`);
    
    res.json({
      success: true,
      message: 'Ahorro actualizado exitosamente',
      data: {
        id: parseInt(id),
        empleado: `${ahorroActual.NOMBRE} ${ahorroActual.APELLIDO}`,
        monto_ahorro: monto_ahorro ? parseFloat(monto_ahorro) : ahorroActual.MONTO_AHORRO
      }
    });

  } catch (error) {
    console.error('Error actualizando ahorro:', error);
    
    if (connection) {
      try {
        await connection.rollback();
      } catch (rollbackError) {
        console.error('Error en rollback:', rollbackError);
      }
    }

    res.status(500).json({
      success: false,
      error: 'Error actualizando ahorro: ' + error.message
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});

app.get('/api/diagnostic/ahorros', async (req, res) => {
  const { database } = req.query;

  if (!database) {
    return res.status(400).json({
      success: false,
      error: 'Parámetro database es requerido'
    });
  }

  let connection;
  try {
    console.log(`Diagnosticando AHORROS para: ${database}`);
    
    connection = await getConnection(database);
    console.log('Conexión a BD exitosa');

    const tableCheck = await connection.execute(
      `SELECT COUNT(*) as table_count 
       FROM user_tables 
       WHERE table_name = 'AHORROS'`
    );

    const tableExists = tableCheck.rows[0].TABLE_COUNT > 0;
    
    if (!tableExists) {
      return res.json({
        success: false,
        error: 'La tabla AHORROS no existe en la base de datos'
      });
    }

    console.log('Tabla AHORROS existe');

    const columnsCheck = await connection.execute(
      `SELECT column_name, data_type, nullable 
       FROM user_tab_columns 
       WHERE table_name = 'AHORROS' 
       ORDER BY column_id`
    );

    console.log('Estructura de tabla verificada');

    const ahorrosCheck = await connection.execute(`
      SELECT a.ID_AHORRO, a.ID_EMPLEADO, e.CODIGO_EMPLEADO, 
             e.NOMBRE || ' ' || e.APELLIDO as NOMBRE_EMPLEADO,
             a.FECHA_AHORRO, a.MONTO_AHORRO, a.TIPO_AHORRO,
             a.PERIODO_PLANILLA, a.ESTADO
      FROM AHORROS a
      JOIN EMPLEADOS e ON a.ID_EMPLEADO = e.ID_EMPLEADO
      ORDER BY a.FECHA_AHORRO DESC
    `);

    console.log(`Consulta ejecutada: ${ahorrosCheck.rows.length} registros encontrados`);

    res.json({
      success: true,
      tableExists: true,
      columnCount: columnsCheck.rows.length,
      recordCount: ahorrosCheck.rows.length,
      columns: columnsCheck.rows,
      sampleData: ahorrosCheck.rows.slice(0, 3),
      message: `Diagnóstico exitoso: ${ahorrosCheck.rows.length} registros en AHORROS`
    });

  } catch (error) {
    console.error('Error en diagnóstico:', error);
    res.status(500).json({
      success: false,
      error: 'Error en diagnóstico: ' + error.message
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});


app.get('/api/ahorros/empleado/:id_empleado', async (req, res) => {
  const { id_empleado } = req.params;
  const { database } = req.query;

  if (!database) {
    return res.status(400).json({
      success: false,
      error: 'Parámetro database es requerido'
    });
  }

  let connection;
  try {
    connection = await getConnection(database);

    const result = await connection.execute(`
      SELECT a.ID_AHORRO, a.FECHA_AHORRO, a.MONTO_AHORRO, a.TIPO_AHORRO,
             a.PERIODO_PLANILLA, a.ESTADO,
             e.CODIGO_EMPLEADO, e.NOMBRE, e.APELLIDO
      FROM AHORROS a
      JOIN EMPLEADOS e ON a.ID_EMPLEADO = e.ID_EMPLEADO
      WHERE a.ID_EMPLEADO = :id_empleado
      ORDER BY a.FECHA_AHORRO DESC
    `, { id_empleado: parseInt(id_empleado) });

    const ahorros = result.rows.map(row => ({
      id: row.ID_AHORRO,
      fecha_ahorro: row.FECHA_AHORRO,
      monto_ahorro: parseFloat(row.MONTO_AHORRO),
      tipo_ahorro: mapTipoAhorroFromDB(row.TIPO_AHORRO), 
      periodo_planilla: row.PERIODO_PLANILLA,
      estado: row.ESTADO,
      empleado: {
        codigo_empleado: row.CODIGO_EMPLEADO,
        nombre: row.NOMBRE,
        apellido: row.APELLIDO
      }
    }));

    res.json({
      success: true,
      data: ahorros
    });

  } catch (error) {
    console.error('Error obteniendo ahorros por empleado:', error);
    res.status(500).json({
      success: false,
      error: 'Error obteniendo ahorros: ' + error.message
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});

app.get('/api/ahorros/resumen/empleado/:id_empleado', async (req, res) => {
  const { id_empleado } = req.params;
  const { database } = req.query;

  if (!database) {
    return res.status(400).json({
      success: false,
      error: 'Parámetro database es requerido'
    });
  }

  let connection;
  try {
    connection = await getConnection(database);

    const result = await connection.execute(`
      SELECT 
        e.CODIGO_EMPLEADO,
        e.NOMBRE || ' ' || e.APELLIDO as NOMBRE_COMPLETO,
        COUNT(a.ID_AHORRO) as total_ahorros,
        SUM(a.MONTO_AHORRO) as total_acumulado,
        AVG(a.MONTO_AHORRO) as promedio_ahorro,
        MIN(a.FECHA_AHORRO) as primera_fecha,
        MAX(a.FECHA_AHORRO) as ultima_fecha
      FROM EMPLEADOS e
      LEFT JOIN AHORROS a ON e.ID_EMPLEADO = a.ID_EMPLEADO AND a.ESTADO = 1
      WHERE e.ID_EMPLEADO = :id_empleado
      GROUP BY e.CODIGO_EMPLEADO, e.NOMBRE, e.APELLIDO
    `, { id_empleado: parseInt(id_empleado) });

    const resumen = result.rows[0] ? {
      codigo_empleado: result.rows[0].CODIGO_EMPLEADO,
      nombre_completo: result.rows[0].NOMBRE_COMPLETO,
      total_ahorros: result.rows[0].TOTAL_AHORROS || 0,
      total_acumulado: parseFloat(result.rows[0].TOTAL_ACUMULADO) || 0,
      promedio_ahorro: parseFloat(result.rows[0].PROMEDIO_AHORRO) || 0,
      primera_fecha: result.rows[0].PRIMERA_FECHA,
      ultima_fecha: result.rows[0].ULTIMA_FECHA
    } : null;

    res.json({
      success: true,
      data: resumen
    });

  } catch (error) {
    console.error('Error obteniendo resumen de ahorros:', error);
    res.status(500).json({
      success: false,
      error: 'Error obteniendo resumen: ' + error.message
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});

app.get('/api/prestamos', async (req, res) => {
  const { database } = req.query;

  if (!database) {
    return res.status(400).json({
      success: false,
      error: 'Parámetro database es requerido'
    });
  }

  let connection;
  try {
    connection = await getConnection(database);

    const result = await connection.execute(`
      SELECT p.ID_PRESTAMO, p.ID_EMPLEADO, e.CODIGO_EMPLEADO,
             e.NOMBRE || ' ' || e.APELLIDO as NOMBRE_EMPLEADO,
             p.FECHA_SOLICITUD, p.MONTO_SOLICITADO, p.MONTO_APROBADO,
             p.PLAZO_MESES, p.TASA_INTERES, p.ESTADO,
             p.FECHA_APROBACION, p.OBSERVACIONES
      FROM PRESTAMOS p
      JOIN EMPLEADOS e ON p.ID_EMPLEADO = e.ID_EMPLEADO
      ORDER BY p.FECHA_SOLICITUD DESC
    `);

    const prestamos = result.rows.map(row => ({
      id: row.ID_PRESTAMO,
      id_empleado: row.ID_EMPLEADO,
      codigo_empleado: row.CODIGO_EMPLEADO,
      nombre_empleado: row.NOMBRE_EMPLEADO,
      fecha_solicitud: row.FECHA_SOLICITUD,
      monto_solicitado: parseFloat(row.MONTO_SOLICITADO),
      monto_aprobado: row.MONTO_APROBADO ? parseFloat(row.MONTO_APROBADO) : null,
      plazo_meses: row.PLAZO_MESES,
      tasa_interes: parseFloat(row.TASA_INTERES),
      estado: row.ESTADO,
      fecha_aprobacion: row.FECHA_APROBACION,
      observaciones: row.OBSERVACIONES
    }));

    res.json({
      success: true,
      data: prestamos
    });

  } catch (error) {
    console.error('Error obteniendo préstamos:', error);
    res.status(500).json({
      success: false,
      error: 'Error obteniendo préstamos: ' + error.message
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});

app.put('/api/prestamos/:id/liquidar', async (req, res) => {
  const { id } = req.params;
  const { database } = req.query;

  if (!database) {
    return res.status(400).json({
      success: false,
      error: 'Parámetro database es requerido'
    });
  }

  let connection;
  try {
    connection = await getConnection(database);

    const result = await connection.execute(
      `UPDATE PRESTAMOS 
       SET ESTADO = 'LIQUIDADO',
           OBSERVACIONES = CONCAT(NVL(OBSERVACIONES, ''), ' - Préstamo liquidado el ' || TO_CHAR(SYSDATE, 'DD/MM/YYYY'))
       WHERE ID_PRESTAMO = :id`,
      { id: parseInt(id) }
    );

    if (result.rowsAffected === 0) {
      return res.status(404).json({
        success: false,
        error: 'Préstamo no encontrado'
      });
    }

    try {
      await connection.execute(
        `INSERT INTO BITACORA (TABLA_AFECTADA, ACCION, DESCRIPCION, USUARIO_RESPONSABLE)
         VALUES ('PRESTAMOS', 'LIQUIDAR', 'Préstamo liquidado ID: ' || :id, 'SISTEMA')`,
        { id: parseInt(id) }
      );
      await connection.commit();
    } catch (bitacoraError) {
      console.log('No se pudo registrar en bitácora:', bitacoraError.message);
    }

    await connection.commit();

    res.json({
      success: true,
      message: 'Préstamo marcado como liquidado exitosamente',
      data: { id_prestamo: parseInt(id), estado: 'LIQUIDADO' }
    });

  } catch (error) {
    console.error('Error liquidando préstamo:', error);
    
    if (connection) {
      try {
        await connection.rollback();
      } catch (rollbackError) {
        console.error('Error en rollback:', rollbackError);
      }
    }

    res.status(500).json({
      success: false,
      error: 'Error liquidando préstamo: ' + error.message
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});

app.put('/api/prestamos/:id/liquidar', async (req, res) => {
  const { id } = req.params;
  const { database } = req.query;

  if (!database) {
    return res.status(400).json({
      success: false,
      error: 'Parámetro database es requerido'
    });
  }

  let connection;
  try {
    connection = await getConnection(database);

    const result = await connection.execute(
      `UPDATE PRESTAMOS 
       SET ESTADO = 'LIQUIDADO',
           OBSERVACIONES = CONCAT(NVL(OBSERVACIONES, ''), ' - Préstamo liquidado el ' || TO_CHAR(SYSDATE, 'DD/MM/YYYY'))
       WHERE ID_PRESTAMO = :id`,
      { id: parseInt(id) }
    );

    if (result.rowsAffected === 0) {
      return res.status(404).json({
        success: false,
        error: 'Préstamo no encontrado'
      });
    }

    try {
      await connection.execute(
        `INSERT INTO BITACORA (TABLA_AFECTADA, ACCION, DESCRIPCION, USUARIO_RESPONSABLE)
         VALUES ('PRESTAMOS', 'LIQUIDAR', 'Préstamo liquidado ID: ' || :id, 'SISTEMA')`,
        { id: parseInt(id) }
      );
      await connection.commit();
    } catch (bitacoraError) {
      console.log('No se pudo registrar en bitácora:', bitacoraError.message);
    }

    await connection.commit();

    res.json({
      success: true,
      message: 'Préstamo marcado como liquidado exitosamente',
      data: { id_prestamo: parseInt(id), estado: 'LIQUIDADO' }
    });

  } catch (error) {
    console.error('Error liquidando préstamo:', error);
    
    if (connection) {
      try {
        await connection.rollback();
      } catch (rollbackError) {
        console.error('Error en rollback:', rollbackError);
      }
    }

    res.status(500).json({
      success: false,
      error: 'Error liquidando préstamo: ' + error.message
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});

app.get('/api/prestamos', async (req, res) => {
  const { database } = req.query;

  if (!database) {
    return res.status(400).json({
      success: false,
      error: 'Parámetro database es requerido'
    });
  }

  let connection;
  try {
    connection = await getConnection(database);

    const result = await connection.execute(`
      SELECT 
        p.ID_PRESTAMO,
        p.ID_EMPLEADO,
        p.FECHA_SOLICITUD,
        p.MONTO_SOLICITADO,
        p.MONTO_APROBADO,
        p.PLAZO_MESES,
        p.TASA_INTERES,
        p.ESTADO,
        p.FECHA_APROBACION,
        p.OBSERVACIONES,
        e.CODIGO_EMPLEADO,
        e.NOMBRE,
        e.APELLIDO
      FROM PRESTAMOS p
      JOIN EMPLEADOS e ON p.ID_EMPLEADO = e.ID_EMPLEADO
      ORDER BY p.FECHA_SOLICITUD DESC
    `);

    const prestamos = result.rows.map(row => ({
      id_prestamo: row.ID_PRESTAMO,
      id_empleado: row.ID_EMPLEADO,
      fecha_solicitud: row.FECHA_SOLICITUD,
      monto_solicitado: parseFloat(row.MONTO_SOLICITADO),
      monto_aprobado: row.MONTO_APROBADO ? parseFloat(row.MONTO_APROBADO) : null,
      plazo_meses: row.PLAZO_MESES,
      tasa_interes: parseFloat(row.TASA_INTERES),
      estado: row.ESTADO,
      fecha_aprobacion: row.FECHA_APROBACION,
      observaciones: row.OBSERVACIONES,
      empleado: {
        codigo_empleado: row.CODIGO_EMPLEADO,
        nombre: row.NOMBRE,
        apellido: row.APELLIDO
      }
    }));

    res.json({
      success: true,
      data: prestamos
    });

  } catch (error) {
    console.error('Error obteniendo préstamos:', error);
    res.status(500).json({
      success: false,
      error: 'Error obteniendo préstamos: ' + error.message
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});

app.post('/api/prestamos/solicitar', async (req, res) => {
  const { database } = req.query;
  const { 
    id_empleado, 
    monto_solicitado, 
    plazo_meses, 
    observaciones 
  } = req.body;

  console.log('Solicitando préstamo:', { 
    id_empleado, 
    monto_solicitado, 
    plazo_meses, 
    database 
  });

  if (!database) {
    return res.status(400).json({
      success: false,
      error: 'Parámetro database es requerido'
    });
  }

  if (!id_empleado || !monto_solicitado || !plazo_meses) {
    return res.status(400).json({
      success: false,
      error: 'ID empleado, monto solicitado y plazo son requeridos'
    });
  }

  if (monto_solicitado <= 0) {
    return res.status(400).json({
      success: false,
      error: 'El monto solicitado debe ser mayor a 0'
    });
  }

  if (plazo_meses < 1 || plazo_meses > 36) {
    return res.status(400).json({
      success: false,
      error: 'El plazo debe estar entre 1 y 36 meses'
    });
  }

  let connection;
  try {
    connection = await getConnection(database);

    const secuenciaPrestamos = database === 'coatepeque' 
      ? 'SEQ_PRESTAMOS_COATEPEQUE' 
      : 'SEQ_PRESTAMOS_CAPITAL';

    console.log(`Usando secuencia: ${secuenciaPrestamos}`);

    const empleadoResult = await connection.execute(
      `SELECT CODIGO_EMPLEADO, NOMBRE, APELLIDO FROM EMPLEADOS 
       WHERE ID_EMPLEADO = :id_empleado AND ESTADO = 1`,
      { id_empleado: parseInt(id_empleado) }
    );

    if (empleadoResult.rows.length === 0) {
      return res.status(404).json({
        success: false,
        error: 'Empleado no encontrado o inactivo'
      });
    }

    const empleado = empleadoResult.rows[0];

    const result = await connection.execute(
      `INSERT INTO PRESTAMOS (
        ID_PRESTAMO,
        ID_EMPLEADO, 
        FECHA_SOLICITUD, 
        MONTO_SOLICITADO, 
        PLAZO_MESES, 
        TASA_INTERES, 
        ESTADO, 
        OBSERVACIONES
      ) VALUES (
        ${secuenciaPrestamos}.NEXTVAL,
        :id_empleado, 
        SYSDATE, 
        :monto_solicitado,
        :plazo_meses, 
        0, 
        'SOLICITADO', 
        :observaciones
      ) RETURNING ID_PRESTAMO INTO :id_prestamo`,
      {
        id_empleado: parseInt(id_empleado),
        monto_solicitado: parseFloat(monto_solicitado),
        plazo_meses: parseInt(plazo_meses),
        observaciones: observaciones || '',
        id_prestamo: { type: oracledb.NUMBER, dir: oracledb.BIND_OUT }
      },
      { autoCommit: false }
    );

    const idPrestamo = result.outBinds.id_prestamo[0];
    console.log('Préstamo solicitado con ID:', idPrestamo);

    await connection.commit();

    res.json({
      success: true,
      message: 'Solicitud de préstamo enviada exitosamente',
      data: {
        id_prestamo: idPrestamo,
        id_empleado: parseInt(id_empleado),
        empleado: `${empleado.NOMBRE} ${empleado.APELLIDO}`,
        monto_solicitado: parseFloat(monto_solicitado),
        plazo_meses: parseInt(plazo_meses),
        estado: 'SOLICITADO'
      }
    });

  } catch (error) {
    console.error('Error solicitando préstamo:', error);
    
    if (connection) {
      try {
        await connection.rollback();
      } catch (rollbackError) {
        console.error('Error en rollback:', rollbackError);
      }
    }

    res.status(500).json({
      success: false,
      error: 'Error solicitando préstamo: ' + error.message
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});

app.post('/api/prestamos/aprobar', async (req, res) => {
  const { database } = req.query;
  const { id_prestamo, monto_aprobado, tasa_interes, observaciones } = req.body;

  console.log('Aprobando préstamo:', { id_prestamo, monto_aprobado, tasa_interes, database });

  if (!database) {
    return res.status(400).json({ success: false, error: 'Parámetro "database" es requerido' });
  }

  if (!id_prestamo || !monto_aprobado) {
    return res.status(400).json({ success: false, error: 'ID de préstamo y monto aprobado son requeridos' });
  }

  let connection;
  try {
    connection = await getConnection(database);

    const paquete = database === 'coatepeque'
      ? 'PKG_GESTION_SOLIDARISMO_COATEPEQUE'
      : 'PKG_GESTION_SOLIDARISMO_CAPITAL';

    const result = await connection.execute(
      `
      BEGIN
        ${paquete}.SP_APROBAR_PRESTAMO(
          P_ID_PRESTAMO => :id_prestamo,
          P_MONTO_APROBADO => :monto_aprobado,
          P_TASA_INTERES => :tasa_interes,
          P_OBSERVACIONES => :observaciones,
          P_RESULTADO => :resultado
        );
      END;
      `,
      {
        id_prestamo: parseInt(id_prestamo),
        monto_aprobado: parseFloat(monto_aprobado),
        tasa_interes: tasa_interes ? parseFloat(tasa_interes) : 0,
        observaciones: observaciones || null,
        resultado: { dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 500 }
      }
    );

    await connection.commit();

    const resultado = result.outBinds.resultado;
    console.log('Resultado SP_APROBAR_PRESTAMO:', resultado);

    res.json({
      success: true,
      message: resultado || 'Préstamo aprobado exitosamente'
    });

  } catch (error) {
    console.error('Error aprobando préstamo:', error);
    res.status(500).json({
      success: false,
      error: 'Error aprobando préstamo: ' + error.message
    });
  } finally {
    if (connection) {
      try { await connection.close(); } catch (err) { console.error('Error cerrando conexión:', err); }
    }
  }
});


app.get('/api/prestamos/:id/saldo-pendiente', async (req, res) => {
  const { id } = req.params;
  const { database } = req.query;

  if (!database) {
    return res.status(400).json({
      success: false,
      error: 'Parámetro database es requerido'
    });
  }

  let connection;
  try {
    connection = await getConnection(database);

    const prestamoResult = await connection.execute(
      `SELECT MONTO_APROBADO FROM PRESTAMOS WHERE ID_PRESTAMO = :id`,
      { id: parseInt(id) }
    );

    if (prestamoResult.rows.length === 0) {
      return res.status(404).json({
        success: false,
        error: 'Préstamo no encontrado'
      });
    }

    const montoAprobado = parseFloat(prestamoResult.rows[0].MONTO_APROBADO) || 0;

    const pagosResult = await connection.execute(
      `SELECT NVL(SUM(MONTO), 0) as TOTAL_PAGADO 
       FROM MOVIMIENTOS 
       WHERE ID_PRESTAMO = :id AND TIPO_MOVIMIENTO = 'INGRESO' AND ESTADO = 'ACTIVO'`,
      { id: parseInt(id) }
    );

    const totalPagado = parseFloat(pagosResult.rows[0].TOTAL_PAGADO) || 0;

    const saldoPendiente = montoAprobado - totalPagado;

    res.json({
      success: true,
      data: {
        id_prestamo: parseInt(id),
        monto_aprobado: montoAprobado,
        total_pagado: totalPagado,
        saldo_pendiente: saldoPendiente,
        porcentaje_pagado: montoAprobado > 0 ? (totalPagado / montoAprobado) * 100 : 0
      }
    });

  } catch (error) {
    console.error('Error calculando saldo pendiente:', error);
    res.status(500).json({
      success: false,
      error: 'Error calculando saldo pendiente: ' + error.message
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});


app.get('/api/movimientos/prestamos', async (req, res) => {
  const { database } = req.query;

  if (!database) {
    return res.status(400).json({
      success: false,
      error: 'Parámetro database es requerido'
    });
  }

  let connection;
  try {
    connection = await getConnection(database);

    const result = await connection.execute(`
      SELECT 
        m.ID_MOVIMIENTO,
        m.ID_EMPLEADO,
        m.TIPO_MOVIMIENTO,
        m.DESCRIPCION,
        m.MONTO,
        m.FECHA_MOVIMIENTO,
        m.ESTADO,
        m.ID_PRESTAMO,
        m.ID_AHORRO,
        e.CODIGO_EMPLEADO,
        e.NOMBRE,
        e.APELLIDO,
        p.MONTO_APROBADO,
        p.ESTADO as ESTADO_PRESTAMO,
        a.MONTO_AHORRO as MONTO_AHORRO_ASOCIADO,
        a.TIPO_AHORRO as TIPO_AHORRO_ASOCIADO
      FROM MOVIMIENTOS m
      JOIN EMPLEADOS e ON m.ID_EMPLEADO = e.ID_EMPLEADO
      LEFT JOIN PRESTAMOS p ON m.ID_PRESTAMO = p.ID_PRESTAMO
      LEFT JOIN AHORROS a ON m.ID_AHORRO = a.ID_AHORRO
      WHERE m.ID_PRESTAMO IS NOT NULL
      ORDER BY m.FECHA_MOVIMIENTO DESC
    `);

    const movimientos = result.rows.map(row => ({
      id_movimiento: row.ID_MOVIMIENTO,
      id_empleado: row.ID_EMPLEADO,
      tipo_movimiento: row.TIPO_MOVIMIENTO,
      descripcion: row.DESCRIPCION,
      monto: parseFloat(row.MONTO),
      fecha_movimiento: row.FECHA_MOVIMIENTO,
      estado: row.ESTADO,
      id_prestamo: row.ID_PRESTAMO,
      id_ahorro: row.ID_AHORRO,
      empleado: {
        codigo_empleado: row.CODIGO_EMPLEADO,
        nombre: row.NOMBRE,
        apellido: row.APELLIDO
      },
      prestamo: {
        monto_aprobado: row.MONTO_APROBADO ? parseFloat(row.MONTO_APROBADO) : null,
        estado: row.ESTADO_PRESTAMO
      },
      ahorro_asociado: row.ID_AHORRO ? {
        monto_ahorro: parseFloat(row.MONTO_AHORRO_ASOCIADO),
        tipo_ahorro: row.TIPO_AHORRO_ASOCIADO
      } : null
    }));

    res.json({
      success: true,
      data: movimientos
    });

  } catch (error) {
    console.error('Error obteniendo movimientos de préstamos:', error);
    res.status(500).json({
      success: false,
      error: 'Error obteniendo movimientos: ' + error.message
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});

app.post('/api/prestamos/pagar-cuota', async (req, res) => {
  const { database } = req.query;
  const { id_prestamo, monto_pago, descripcion } = req.body;

  console.log('Pagando cuota de préstamo:', { id_prestamo, monto_pago, database });

  if (!database) {
    return res.status(400).json({ success: false, error: 'Parámetro "database" es requerido' });
  }

  if (!id_prestamo || !monto_pago) {
    return res.status(400).json({ success: false, error: 'ID de préstamo y monto de pago son requeridos' });
  }

  if (monto_pago <= 0) {
    return res.status(400).json({ success: false, error: 'El monto de pago debe ser mayor a 0' });
  }

  let connection;
  try {
    connection = await getConnection(database);

    const prestamoResult = await connection.execute(
      `SELECT p.ID_EMPLEADO, e.CODIGO_EMPLEADO, e.NOMBRE, e.APELLIDO
         FROM PRESTAMOS p
         JOIN EMPLEADOS e ON p.ID_EMPLEADO = e.ID_EMPLEADO
        WHERE p.ID_PRESTAMO = :id_prestamo
          AND p.ESTADO = 'APROBADO'`,
      { id_prestamo: parseInt(id_prestamo) }
    );

    if (prestamoResult.rows.length === 0) {
      return res.status(404).json({
        success: false,
        error: 'Préstamo no encontrado o no está aprobado'
      });
    }

    const empleado = prestamoResult.rows[0];
    const descripcionMovimiento = `Pago de préstamo - ${empleado.NOMBRE} ${empleado.APELLIDO} - ${descripcion || 'Cuota de préstamo'}`;

    const movimientoSQL = `
      INSERT INTO MOVIMIENTOS (
        ID_EMPLEADO,
        TIPO_MOVIMIENTO,
        DESCRIPCION,
        MONTO,
        FECHA_MOVIMIENTO,
        ESTADO,
        ID_PRESTAMO
      ) VALUES (
        :id_empleado,
        'INGRESO',
        :descripcion,
        :monto,
        SYSDATE,
        'ACTIVO',
        :id_prestamo
      )
      RETURNING ID_MOVIMIENTO INTO :id_movimiento
    `;

    const movimientoResult = await connection.execute(movimientoSQL, {
      id_empleado: empleado.ID_EMPLEADO,
      descripcion: descripcionMovimiento,
      monto: parseFloat(monto_pago),
      id_prestamo: parseInt(id_prestamo),
      id_movimiento: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER }
    });

    const idMovimiento = movimientoResult.outBinds.id_movimiento[0];

    await connection.execute(
      `INSERT INTO BITACORA (
        TABLA_AFECTADA, ACCION, DESCRIPCION, USUARIO_RESPONSABLE
      ) VALUES (
        'PRESTAMOS',
        'PAGO_CUOTA',
        'Pago de cuota registrado para empleado ' || :codigo_empleado || ' - Monto: ' || :monto,
        'SISTEMA'
      )`,
      {
        codigo_empleado: empleado.CODIGO_EMPLEADO,
        monto: monto_pago
      }
    );

    await connection.commit();

    console.log(`Pago de cuota registrado correctamente. Movimiento ID: ${idMovimiento}`);

    res.json({
      success: true,
      message: 'Pago de cuota registrado exitosamente',
      data: {
        id_prestamo: parseInt(id_prestamo),
        id_movimiento: idMovimiento,
        id_empleado: empleado.ID_EMPLEADO,
        empleado: `${empleado.NOMBRE} ${empleado.APELLIDO}`,
        monto_pago: parseFloat(monto_pago),
        tipo_movimiento: 'INGRESO'
      }
    });

  } catch (error) {
    console.error('Error registrando pago de cuota:', error);
    if (connection) {
      try { await connection.rollback(); } catch (rollbackError) { console.error('Error en rollback:', rollbackError); }
    }

    res.status(500).json({
      success: false,
      error: 'Error registrando pago de cuota: ' + error.message
    });

  } finally {
    if (connection) {
      try { await connection.close(); } catch (error) { console.error('Error cerrando conexión:', error); }
    }
  }
});


app.get('/api/debug/secuencias', async (req, res) => {
  const { database } = req.query;
  
  let connection;
  try {
    connection = await getConnection(database);
    
    const result = await connection.execute(`
      SELECT sequence_name, last_number, increment_by
      FROM user_sequences
      ORDER BY sequence_name
    `);
    
    console.log(`Secuencias en ${database}:`, result.rows);
    
    res.json({
      success: true,
      database: database,
      secuencias: result.rows,
      message: `Diagnóstico de secuencias en ${database}`
    });
    
  } catch (error) {
    console.error('Error en diagnóstico de secuencias:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});

app.get('/api/debug/tipos-ahorro', async (req, res) => {
  const { database } = req.query;
  
  let connection;
  try {
    connection = await getConnection(database);
    
    const result = await connection.execute(`
      SELECT 
        TIPO_AHORRO,
        COUNT(*) as cantidad,
        SUM(MONTO_AHORRO) as total
      FROM AHORROS 
      WHERE ESTADO = 1
      GROUP BY TIPO_AHORRO
    `);
    
    console.log('Tipos de ahorro en base de datos:', result.rows);
    
    res.json({
      success: true,
      tipos_ahorro: result.rows,
      message: 'Diagnóstico de tipos de ahorro en la base de datos'
    });
    
  } catch (error) {
    console.error('Error en diagnóstico:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});

app.get('/api/planillas/:id/detalles', async (req, res) => {
  const { database } = req.query;
  const { id } = req.params;

  if (!database) {
    return res.status(400).json({ success: false, error: 'Database requerida' });
  }

  let connection;
  try {
    connection = await getConnection(database);

    const planillaQuery = await connection.execute(
      `SELECT 
          ID_PLANILLA AS "id_planilla",
          PERIODO AS "periodo",
          FECHA_PROCESAMIENTO AS "fecha_procesamiento",
          ESTADO AS "estado",
          TOTAL_INGRESOS AS "total_ingresos",
          TOTAL_DESCUENTOS AS "total_descuentos",
          TOTAL_NETO AS "total_neto"
       FROM PLANILLAS
       WHERE ID_PLANILLA = :id`,
      { id }
    );

    const planilla = planillaQuery.rows[0];

    const detallesQuery = await connection.execute(
      `SELECT 
          d.ID_DETALLE AS "id_detalle",
          d.ID_PLANILLA AS "id_planilla",
          e.ID_EMPLEADO AS "id_empleado",
          e.CODIGO_EMPLEADO AS "codigo_empleado",
          e.NOMBRE || ' ' || e.APELLIDO AS "nombre_empleado",
          d.SALARIO_BASE AS "salario_base",
          d.BONIFICACIONES AS "bonificaciones",
          d.IGSS AS "igss",
          d.ISR AS "isr",
          d.DESCUENTO_VARIABLE AS "descuento_variable",
          d.AHORRO_SOLIDARISMO AS "ahorro_solidarismo",
          d.TOTAL_INGRESOS AS "total_ingresos",
          d.TOTAL_DESCUENTOS AS "total_descuentos",
          d.NETO_PAGAR AS "neto_pagar"
       FROM DETALLE_PLANILLA d
       JOIN EMPLEADOS e ON d.ID_EMPLEADO = e.ID_EMPLEADO
       WHERE d.ID_PLANILLA = :id
       ORDER BY e.NOMBRE`,
      { id }
    );

    const detalles = detallesQuery.rows;

    const resumen = {
      total_ingresos: detalles.reduce((sum, d) => sum + (d.total_ingresos || 0), 0),
      total_descuentos: detalles.reduce((sum, d) => sum + (d.total_descuentos || 0), 0),
      total_neto: detalles.reduce((sum, d) => sum + (d.neto_pagar || 0), 0)
    };

    res.json({
      success: true,
      data: {
        planilla,
        detalles,
        total_empleados: detalles.length,
        resumen
      }
    });

  } catch (error) {
    console.error('Error obteniendo detalles:', error);
    res.status(500).json({ success: false, error: error.message });
  } finally {
    if (connection) await connection.close();
  }
});


app.post('/api/planillas/:id/agregar-empleado', async (req, res) => {
  const { id } = req.params;
  const { database } = req.query;
  const { 
    id_empleado, 
    bonificaciones = 0, 
    descuento_variable = 0,
    igss = 0,
    isr = 0,
    ahorro_solidarismo = 0 
  } = req.body;

  console.log(`Agregando empleado a planilla ID: ${id}, Base de datos: ${database}`);
  console.log('Datos recibidos:', req.body);

  if (!database) {
    return res.status(400).json({
      success: false,
      error: 'Parámetro database es requerido'
    });
  }

  if (!id || id.trim() === '' || id === 'undefined' || id === 'null') {
    return res.status(400).json({
      success: false,
      error: 'ID de planilla no proporcionado'
    });
  }

  const planillaId = parseInt(id);
  if (isNaN(planillaId) || planillaId <= 0) {
    return res.status(400).json({
      success: false,
      error: 'ID de planilla inválido'
    });
  }

  if (!id_empleado) {
    return res.status(400).json({
      success: false,
      error: 'ID de empleado es requerido'
    });
  }

  let connection;
  try {
    connection = await getConnection(database);
    
    const planillaResult = await connection.execute(
      `SELECT ID_PLANILLA, PERIODO, ESTADO FROM PLANILLAS WHERE ID_PLANILLA = :id_planilla`,
      { id_planilla: planillaId }
    );

    if (planillaResult.rows.length === 0) {
      return res.status(404).json({
        success: false,
        error: `Planilla con ID ${planillaId} no encontrada`
      });
    }

    const empleadoResult = await connection.execute(
      `SELECT ID_EMPLEADO, CODIGO_EMPLEADO, NOMBRE, APELLIDO, SALARIO_BASE, ESTADO 
       FROM EMPLEADOS 
       WHERE ID_EMPLEADO = :id_empleado`,
      { id_empleado: parseInt(id_empleado) }
    );

    if (empleadoResult.rows.length === 0) {
      return res.status(404).json({
        success: false,
        error: `Empleado con ID ${id_empleado} no encontrado`
      });
    }

    const empleado = empleadoResult.rows[0];
    if (empleado.ESTADO !== 1) {
      return res.status(400).json({
        success: false,
        error: `El empleado ${empleado.NOMBRE} ${empleado.APELLIDO} no está activo`
      });
    }

    const existeEnPlanilla = await connection.execute(
      `SELECT ID_DETALLE FROM DETALLE_PLANILLA 
       WHERE ID_PLANILLA = :id_planilla AND ID_EMPLEADO = :id_empleado`,
      { id_planilla: planillaId, id_empleado: parseInt(id_empleado) }
    );

    if (existeEnPlanilla.rows.length > 0) {
      return res.status(400).json({
        success: false,
        error: `El empleado ya está agregado a esta planilla`
      });
    }

    const salarioBase = parseFloat(empleado.SALARIO_BASE) || 0;
    const bonificacionesNum = parseFloat(bonificaciones) || 0;
    const igssNum = parseFloat(igss) || 0;
    const isrNum = parseFloat(isr) || 0;
    const descuentoVariableNum = parseFloat(descuento_variable) || 0; 
    const ahorroSolidarismoNum = parseFloat(ahorro_solidarismo) || 0;

    const totalIngresos = salarioBase + bonificacionesNum;
    const totalDescuentos = igssNum + isrNum + descuentoVariableNum + ahorroSolidarismoNum;
    const netoPagar = totalIngresos - totalDescuentos;

    console.log('Cálculos realizados:', {
      salarioBase,
      bonificaciones: bonificacionesNum,
      totalIngresos,
      igss: igssNum,
      isr: isrNum,
      descuentoVariable: descuentoVariableNum, 
      ahorroSolidarismo: ahorroSolidarismoNum,
      totalDescuentos,
      netoPagar
    });

    const insertResult = await connection.execute(`
      INSERT INTO DETALLE_PLANILLA (
        ID_PLANILLA, ID_EMPLEADO, SALARIO_BASE, BONIFICACIONES, 
        IGSS, ISR, DESCUENTO_VARIABLE, AHORRO_SOLIDARISMO,
        TOTAL_INGRESOS, TOTAL_DESCUENTOS, NETO_PAGAR
      ) VALUES (
        :id_planilla, :id_empleado, :salario_base, :bonificaciones,
        :igss, :isr, :descuento_variable, :ahorro_solidarismo,
        :total_ingresos, :total_descuentos, :neto_pagar
      ) RETURNING ID_DETALLE INTO :id_detalle
    `, {
      id_planilla: planillaId,
      id_empleado: parseInt(id_empleado),
      salario_base: salarioBase,
      bonificaciones: bonificacionesNum,
      igss: igssNum,
      isr: isrNum,
      descuento_variable: descuentoVariableNum, 
      ahorro_solidarismo: ahorroSolidarismoNum,
      total_ingresos: totalIngresos,
      total_descuentos: totalDescuentos,
      neto_pagar: netoPagar,
      id_detalle: { type: oracledb.NUMBER, dir: oracledb.BIND_OUT }
    }, { autoCommit: true });

    const idDetalle = insertResult.outBinds.id_detalle[0];
    console.log(`Detalle de planilla creado con ID: ${idDetalle}`);

    await actualizarTotalesPlanilla(connection, planillaId);

    res.json({
      success: true,
      data: {
        id_detalle: idDetalle,
        mensaje: 'Empleado agregado exitosamente a la planilla',
        calculos: {
          total_ingresos: totalIngresos,
          total_descuentos: totalDescuentos,
          neto_pagar: netoPagar
        }
      }
    });

  } catch (error) {
    console.error('Error agregando empleado a planilla:', error);
    
    let errorMessage = 'Error agregando empleado a planilla: ' + error.message;
    
    if (error.message.includes('ORA-00001')) {
      errorMessage = 'Error: El empleado ya existe en esta planilla';
    } else if (error.message.includes('ORA-02291')) {
      errorMessage = 'Error: Referencia inválida (empleado o planilla no existe)';
    } else if (error.message.includes('ORA-00942')) {
      errorMessage = 'Error: La tabla no existe en la base de datos';
    }

    res.status(500).json({
      success: false,
      error: errorMessage
    });

  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});

async function actualizarTotalesPlanilla(connection, planillaId) {
  try {
    const totalesResult = await connection.execute(`
      SELECT 
        SUM(TOTAL_INGRESOS) as TOTAL_INGRESOS,
        SUM(TOTAL_DESCUENTOS) as TOTAL_DESCUENTOS,
        SUM(NETO_PAGAR) as TOTAL_NETO
      FROM DETALLE_PLANILLA 
      WHERE ID_PLANILLA = :id_planilla
    `, { id_planilla: planillaId });

    const totalIngresos = parseFloat(totalesResult.rows[0].TOTAL_INGRESOS) || 0;
    const totalDescuentos = parseFloat(totalesResult.rows[0].TOTAL_DESCUENTOS) || 0;
    const totalNeto = parseFloat(totalesResult.rows[0].TOTAL_NETO) || 0;

    await connection.execute(`
      UPDATE PLANILLAS 
      SET TOTAL_INGRESOS = :total_ingresos,
          TOTAL_DESCUENTOS = :total_descuentos,
          TOTAL_NETO = :total_neto
      WHERE ID_PLANILLA = :id_planilla
    `, {
      total_ingresos: totalIngresos,
      total_descuentos: totalDescuentos,
      total_neto: totalNeto,
      id_planilla: planillaId
    }, { autoCommit: true });

    console.log(`Totales actualizados para planilla ${planillaId}:`, {
      totalIngresos,
      totalDescuentos,
      totalNeto
    });

  } catch (error) {
    console.error('Error actualizando totales de planilla:', error);
    throw error;
  }
}

app.delete('/api/planillas/:id/empleado/:idEmpleado', async (req, res) => {
  const { id, idEmpleado } = req.params;
  const { database } = req.query;

  console.log(`Eliminando empleado ${idEmpleado} de planilla ${id}, Base de datos: ${database}`);

  if (!database) {
    return res.status(400).json({
      success: false,
      error: 'Parámetro database es requerido'
    });
  }

  const planillaId = parseInt(id);
  const empleadoId = parseInt(idEmpleado);

  if (isNaN(planillaId) || planillaId <= 0) {
    return res.status(400).json({
      success: false,
      error: 'ID de planilla inválido'
    });
  }

  if (isNaN(empleadoId) || empleadoId <= 0) {
    return res.status(400).json({
      success: false,
      error: 'ID de empleado inválido'
    });
  }

  let connection;
  try {
    connection = await getConnection(database);

    const detalleResult = await connection.execute(
      `SELECT ID_DETALLE FROM DETALLE_PLANILLA 
       WHERE ID_PLANILLA = :id_planilla AND ID_EMPLEADO = :id_empleado`,
      { id_planilla: planillaId, id_empleado: empleadoId }
    );

    if (detalleResult.rows.length === 0) {
      return res.status(404).json({
        success: false,
        error: 'Empleado no encontrado en esta planilla'
      });
    }

    await connection.execute(
      `DELETE FROM DETALLE_PLANILLA 
       WHERE ID_PLANILLA = :id_planilla AND ID_EMPLEADO = :id_empleado`,
      { id_planilla: planillaId, id_empleado: empleadoId },
      { autoCommit: true }
    );

    await actualizarTotalesPlanilla(connection, planillaId);

    res.json({
      success: true,
      data: {
        mensaje: 'Empleado eliminado exitosamente de la planilla'
      }
    });

  } catch (error) {
    console.error('Error eliminando empleado de planilla:', error);
    
    let errorMessage = 'Error eliminando empleado de planilla: ' + error.message;
    
    if (error.message.includes('ORA-00942')) {
      errorMessage = 'Error: La tabla no existe en la base de datos';
    }

    res.status(500).json({
      success: false,
      error: errorMessage
    });

  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (error) {
        console.error('Error cerrando conexión:', error);
      }
    }
  }
});

app.get('/api/health', (req, res) => {
  res.json({
    success: true,
    message: 'Backend funcionando correctamente',
    timestamp: new Date().toISOString()
  });
});

app.get("/api/central-solidarismo", async (req, res) => {
  const sql = require("mssql");

  const sqlConfig = {
    user: "Central",
    password: "Central123",
    server: "DANIEL",
    database: "BD_SOLIDARISMO_CENTRAL",
    port: 1433,
    options: {
      encrypt: false,
      trustServerCertificate: true,
    },
  };

  try {
    const pool = await sql.connect(sqlConfig);
    const result = await pool.request().query(`
      SELECT 
        e.ID_EMPLEADO,
        e.CODIGO_EMPLEADO,
        (e.NOMBRE + ' ' + e.APELLIDO) AS NOMBRE_COMPLETO,
        e.SUCURSAL,
        ISNULL(SUM(a.MONTO_AHORRO), 0) AS TOTAL_AHORROS,
        ISNULL(SUM(p.MONTO_APROBADO), 0) AS TOTAL_PRESTAMOS,
        ISNULL(SUM(d.MONTO_DEVUELTO), 0) AS TOTAL_DEVOLUCIONES
      FROM EMPLEADOS_CENTRAL e
      LEFT JOIN AHORROS_CENTRAL a ON e.ID_EMPLEADO = a.ID_EMPLEADO
      LEFT JOIN PRESTAMOS_CENTRAL p ON e.ID_EMPLEADO = p.ID_EMPLEADO
      LEFT JOIN DEVOLUCIONES_CENTRAL d ON e.ID_EMPLEADO = d.ID_EMPLEADO
      GROUP BY 
        e.ID_EMPLEADO, e.CODIGO_EMPLEADO, e.NOMBRE, e.APELLIDO, e.SUCURSAL
      ORDER BY e.SUCURSAL, e.NOMBRE
    `);

    res.json({
      success: true,
      data: result.recordset,
    });
  } catch (err) {
    console.error("Error obteniendo datos centrales:", err);
    res.status(500).json({
      success: false,
      error: err.message,
    });
  }
});


app.get("/api/sincronizar-solidarismo", async (req, res) => {
  const oracledb = require("oracledb");
  const sql = require("mssql");

  const oracleCapital = {
    user: "CAPITAL",
    password: "CAPITAL123",
    connectString: "localhost:1521/XE",
  };

  const oracleCoatepeque = {
    user: "COATEPEQUE",
    password: "COATEPEQUE123",
    connectString: "localhost:1521/XE",
  };

  const sqlConfig = {
    user: "Central",
    password: "Central123",
    server: "DANIEL",
    database: "BD_SOLIDARISMO_CENTRAL",
    port: 1433,
    options: {
      encrypt: false,
      trustServerCertificate: true,
    },
  };

 
  async function sincronizarSucursal(nombreSucursal, connOracle) {
    const connSql = await sql.connect(sqlConfig);
    console.log(`Sincronizando sucursal: ${nombreSucursal}`);

    oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;

   
    const empleados = await connOracle.execute(`
      SELECT 
        TO_CHAR(ID_EMPLEADO) AS ID_EMPLEADO,
        NVL(CODIGO_EMPLEADO, 'SIN_CODIGO') AS CODIGO_EMPLEADO,
        NVL(NOMBRE, 'SIN_NOMBRE') AS NOMBRE,
        NVL(APELLIDO, 'SIN_APELLIDO') AS APELLIDO,
        NVL(SALARIO_BASE, 0) AS SALARIO_BASE,
        NVL(FECHA_INGRESO, SYSDATE) AS FECHA_INGRESO
      FROM EMPLEADOS
    `);
        for (const e of empleados.rows) {
  await connSql
    .request()
    .input("ID_EMPLEADO", sql.NVarChar, e.ID_EMPLEADO)
    .input("CODIGO_EMPLEADO", sql.NVarChar, e.CODIGO_EMPLEADO)
    .input("NOMBRE", sql.NVarChar, e.NOMBRE)
    .input("APELLIDO", sql.NVarChar, e.APELLIDO)
    .input("SALARIO_BASE", sql.Decimal(10, 2), e.SALARIO_BASE)
    .input("FECHA_INGRESO", sql.Date, e.FECHA_INGRESO)
    .input("SUCURSAL", sql.NVarChar, nombreSucursal)
    .input("PORCENTAJE_AHORRO", sql.Decimal(5, 2), 5)
    .query(`
      MERGE EMPLEADOS_CENTRAL AS TARGET
      USING (SELECT @ID_EMPLEADO AS ID_EMPLEADO, @SUCURSAL AS SUCURSAL) AS SOURCE
      ON TARGET.ID_EMPLEADO = SOURCE.ID_EMPLEADO AND TARGET.SUCURSAL = SOURCE.SUCURSAL
      WHEN MATCHED THEN
        UPDATE SET 
          CODIGO_EMPLEADO=@CODIGO_EMPLEADO, 
          NOMBRE=@NOMBRE, 
          APELLIDO=@APELLIDO,
          SALARIO_BASE=@SALARIO_BASE, 
          FECHA_INGRESO=@FECHA_INGRESO,
          SUCURSAL=@SUCURSAL,
          PORCENTAJE_AHORRO=@PORCENTAJE_AHORRO
      WHEN NOT MATCHED THEN
        INSERT (ID_EMPLEADO, CODIGO_EMPLEADO, NOMBRE, APELLIDO, SALARIO_BASE, FECHA_INGRESO, SUCURSAL, PORCENTAJE_AHORRO)
        VALUES (@ID_EMPLEADO, @CODIGO_EMPLEADO, @NOMBRE, @APELLIDO, @SALARIO_BASE, @FECHA_INGRESO, @SUCURSAL, @PORCENTAJE_AHORRO);
    `);
}

    console.log(`Empleados sincronizados: ${empleados.rows.length}`);

    const ahorros = await connOracle.execute(`
      SELECT 
        TO_CHAR(ID_AHORRO) AS ID_AHORRO,
        TO_CHAR(ID_EMPLEADO) AS ID_EMPLEADO,
        FECHA_AHORRO,
        MONTO_AHORRO,
        TIPO_AHORRO
      FROM AHORROS
    `);

    for (const a of ahorros.rows) {
      if (!a.ID_AHORRO) continue;
      await connSql
        .request()
        .input("ID_AHORRO", sql.NVarChar, a.ID_AHORRO)
        .input("ID_EMPLEADO", sql.NVarChar, a.ID_EMPLEADO)
        .input("FECHA_AHORRO", sql.Date, a.FECHA_AHORRO)
        .input("MONTO_AHORRO", sql.Decimal(10, 2), a.MONTO_AHORRO || 0)
        .input("TIPO_AHORRO", sql.NVarChar, a.TIPO_AHORRO)
        .input("SUCURSAL", sql.NVarChar, nombreSucursal)
        .query(`
          MERGE AHORROS_CENTRAL AS TARGET
          USING (SELECT @ID_AHORRO AS ID_AHORRO) AS SOURCE
          ON TARGET.ID_AHORRO = SOURCE.ID_AHORRO
          WHEN MATCHED THEN
            UPDATE SET ID_EMPLEADO=@ID_EMPLEADO, FECHA_AHORRO=@FECHA_AHORRO, MONTO_AHORRO=@MONTO_AHORRO,
                       TIPO_AHORRO=@TIPO_AHORRO, SUCURSAL=@SUCURSAL
          WHEN NOT MATCHED THEN
            INSERT (ID_AHORRO, ID_EMPLEADO, FECHA_AHORRO, MONTO_AHORRO, TIPO_AHORRO, SUCURSAL)
            VALUES (@ID_AHORRO, @ID_EMPLEADO, @FECHA_AHORRO, @MONTO_AHORRO, @TIPO_AHORRO, @SUCURSAL);
        `);
    }

    console.log(`Ahorros sincronizados: ${ahorros.rows.length}`);

    const prestamos = await connOracle.execute(`
      SELECT 
        TO_CHAR(ID_PRESTAMO) AS ID_PRESTAMO,
        TO_CHAR(ID_EMPLEADO) AS ID_EMPLEADO,
        NVL(MONTO_APROBADO, 0) AS MONTO_APROBADO,
        NVL(ESTADO, 'SIN_ESTADO') AS ESTADO,
        NVL(FECHA_APROBACION, SYSDATE) AS FECHA_APROBACION
      FROM PRESTAMOS
    `);

    for (const p of prestamos.rows) {
      if (!p.ID_PRESTAMO) continue;
      await connSql
        .request()
        .input("ID_PRESTAMO", sql.NVarChar, p.ID_PRESTAMO)
        .input("ID_EMPLEADO", sql.NVarChar, p.ID_EMPLEADO)
        .input("MONTO_APROBADO", sql.Decimal(10, 2), p.MONTO_APROBADO || 0)
        .input("ESTADO", sql.NVarChar, p.ESTADO)
        .input("FECHA_APROBACION", sql.Date, p.FECHA_APROBACION)
        .input("SUCURSAL", sql.NVarChar, nombreSucursal)
        .query(`
          MERGE PRESTAMOS_CENTRAL AS TARGET
          USING (SELECT @ID_PRESTAMO AS ID_PRESTAMO) AS SOURCE
          ON TARGET.ID_PRESTAMO = SOURCE.ID_PRESTAMO
          WHEN MATCHED THEN
            UPDATE SET 
              ID_EMPLEADO=@ID_EMPLEADO, 
              MONTO_APROBADO=@MONTO_APROBADO, 
              ESTADO=@ESTADO,
              FECHA_APROBACION=@FECHA_APROBACION, 
              SUCURSAL=@SUCURSAL
          WHEN NOT MATCHED THEN
            INSERT (ID_PRESTAMO, ID_EMPLEADO, MONTO_APROBADO, ESTADO, FECHA_APROBACION, SUCURSAL)
            VALUES (@ID_PRESTAMO, @ID_EMPLEADO, @MONTO_APROBADO, @ESTADO, @FECHA_APROBACION, @SUCURSAL);
        `);
    }

    console.log(`Préstamos sincronizados: ${prestamos.rows.length}`);

    await connSql.close();
    console.log(`Sucursal ${nombreSucursal} sincronizada correctamente`);
  }

  try {
    console.log("Limpiando tablas centrales...");
    const pool = await sql.connect(sqlConfig);
    await pool.request().query(`
      DELETE FROM PRESTAMOS_CENTRAL;
      DELETE FROM AHORROS_CENTRAL;
      DELETE FROM EMPLEADOS_CENTRAL;
    `);
    console.log("Tablas centrales limpiadas correctamente.");

    const capitalConn = await oracledb.getConnection(oracleCapital);
    const coatepequeConn = await oracledb.getConnection(oracleCoatepeque);

    await sincronizarSucursal("CAPITAL", capitalConn);
    await sincronizarSucursal("COATEPEQUE", coatepequeConn);

    await capitalConn.close();
    await coatepequeConn.close();

    console.log("Sincronización completa con SQL Server Central");
    res.json({ success: true, message: "Sincronización completada correctamente." });
  } catch (err) {
    console.error("Error en la sincronización:", err);
    res.status(500).json({ success: false, error: err.message });
  }
});


app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, '..', 'frontend', 'build', 'index.html'));
});

app.listen(PORT, () => {
  console.log(`Servidor corriendo en puerto ${PORT}`);
  console.log('Bases de datos configuradas para Oracle XE:');
  console.log('CAPITAL:');
  console.log('- Servicio: XE');
  console.log('- Usuario: CAPITAL');
  console.log('- Password: CAPITAL123');
  console.log('COATEPEQUE:');
  console.log('- Servicio: XE');
  console.log('- Usuario: COATEPEQUE');
  console.log('- Password: COATEPEQUE123');
  console.log('\n Endpoints disponibles:');
  console.log('- GET  /api/test-connections → Probar conexiones');
  console.log('- POST /api/login → Iniciar sesión');
  console.log('- GET  /api/dashboard/estadisticas-detalladas → Estadísticas por sucursal');
  console.log('- GET  /api/dashboard/actividad-reciente → Actividad por sucursal');
  console.log('- GET  /api/dashboard/resumen-general → Resumen general por sucursal');
  console.log('- GET  /api/dashboard/datos-graficos → Datos para gráficos por sucursal');
  console.log('- GET  /api/empleados → Listar empleados por sucursal');
  console.log('- POST /api/empleados → Crear empleado por sucursal');
  console.log('- PUT  /api/empleados/:id → Actualizar empleado por sucursal');
  console.log('- DELETE /api/empleados/:id → Eliminar empleado por sucursal');
  console.log('- GET  /api/ahorros → Listar ahorros por sucursal');
  console.log('- POST /api/ahorros → Crear ahorro por sucursal');
  console.log('- PUT  /api/ahorros/:id → Actualizar ahorro por sucursal');
  console.log('- DELETE /api/ahorros/:id → Eliminar ahorro por sucursal');
  console.log('- GET  /api/ahorros/empleado/:id → Ahorros por empleado');
  console.log('- GET  /api/ahorros/resumen/empleado/:id → Resumen ahorros por empleado');
  console.log('- GET  /api/planillas → Listar planillas por sucursal');
  console.log('- GET  /api/prestamos → Listar préstamos por sucursal');
  console.log('- GET  /api/debug/tipos-ahorro → Diagnóstico de tipos de ahorro');
  console.log('\n Servidor listo para recibir conexiones');
});