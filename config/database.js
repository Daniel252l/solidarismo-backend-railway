const oracledb = require('oracledb');

oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;
oracledb.autoCommit = true;

const dbConfigs = {
  capital: {
    user: process.env.ORACLE_USER || 'CAPITAL',
    password: process.env.ORACLE_PASSWORD || 'CAPITAL123',
    connectString: process.env.ORACLE_CONNECTION_STRING || 'localhost:1521/XE'
  },
  coatepeque: {
    user: process.env.ORACLE_USER_COATEPEQUE || 'COATEPEQUE',
    password: process.env.ORACLE_PASSWORD_COATEPEQUE || 'COATEPEQUE123', 
    connectString: process.env.ORACLE_CONNECTION_STRING_COATEPEQUE || 'localhost:1521/XE'
  }
};

const connectionPools = {};

async function initializePools() {
  try {
    for (const [dbName, config] of Object.entries(dbConfigs)) {
      try {
        connectionPools[dbName] = await oracledb.createPool({
          ...config,
          poolMin: 2,
          poolMax: 10,
          poolIncrement: 1,
          poolTimeout: 60
        });
        console.log(`Pool de conexión creado para: ${dbName}`);
      } catch (poolError) {
        console.error(`Error creando pool para ${dbName}:`, poolError.message);
      }
    }
  } catch (error) {
    console.error('Error inicializando pools:', error);
  }
}

async function getConnection(database) {
  try {
    if (!database) {
      throw new Error('Debe especificar el nombre de la base de datos (CAPITAL o COATEPEQUE)');
    }

    const dbKey = database.toLowerCase();

    if (!connectionPools[dbKey]) {
      throw new Error(`No existe pool para la base de datos: ${database}`);
    }

    const connection = await connectionPools[dbKey].getConnection();

    await connection.execute('SELECT 1 as TEST FROM DUAL');

    console.log(`Conectado correctamente a ${dbKey.toUpperCase()}`);
    return connection;

  } catch (error) {
    console.error(`Error obteniendo conexión para ${database}:`, error.message);
    throw error;
  }
}


async function testAllConnections() {
  const results = {};
  
  for (const [dbName, config] of Object.entries(dbConfigs)) {
    try {
      console.log(`Probando conexión a ${dbName}...`);
      const connection = await oracledb.getConnection(config);
      
      const result = await connection.execute('SELECT 1 as TEST FROM DUAL');
      
      const tablesResult = await connection.execute(`
        SELECT COUNT(*) as table_count 
        FROM user_tables 
        WHERE table_name IN ('EMPLEADOS', 'AHORROS', 'PRESTAMOS')
      `);
      
      await connection.close();
      
      results[dbName] = {
        success: true,
        test: result.rows[0].TEST,
        table_count: tablesResult.rows[0].TABLE_COUNT,
        message: `Conexión exitosa a ${dbName} (${config.user})`
      };
      
    } catch (error) {
      results[dbName] = {
        success: false,
        message: `Error conectando a ${dbName} (${config.user}): ${error.message}`
      };
    }
  }
  
  return results;
}

module.exports = {
  initializePools,
  getConnection,
  testAllConnections,
  dbConfigs
};