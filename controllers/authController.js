import { getDB } from '../config/database.js';

export const login = async (req, res) => {
  try {
    const { usuario, contrasena } = req.body;
    const connection = getDB();
    
    const result = await connection.execute(
      `BEGIN 
         PKG_SEGURIDAD.SP_AUTENTICAR_USUARIO(
           :usuario, :contrasena, NULL, NULL, 
           :token, :id_usuario, :rol, :resultado
         );
       END;`,
      {
        usuario,
        contrasena,
        token: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
        id_usuario: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER },
        rol: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
        resultado: { dir: oracledb.BIND_OUT, type: oracledb.STRING }
      }
    );

    if (result.outBinds.resultado.includes('ERROR')) {
      return res.status(401).json({ error: result.outBinds.resultado });
    }

    res.json({
      token: result.outBinds.token,
      usuario: {
        id: result.outBinds.id_usuario,
        nombre: usuario,
        rol: result.outBinds.rol
      },
      message: result.outBinds.resultado
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};