const jwt = require('jsonwebtoken');
const SECRET_KEY = "clave_secreta_para_la_hc_distribuida"; // Cambia esto por lo que quieras

// Middleware para proteger las rutas de tu API/FHIR
function verificarToken(req, res, next) {
    const authHeader = req.headers['authorization'];

    // Validar si viene el header Authorization: Bearer <token>
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
        return res.status(401).json({ error: "Acceso denegado. Token no proporcionado (OAuth2/SMART on FHIR)." });
    }

    const token = authHeader.split(' ')[1];

    try {
        // Verificar firma y expiración del JWT
        const verificado = jwt.verify(token, SECRET_KEY);
        req.usuario = verificado; // Guardamos los scopes/datos del usuario en la petición
        next(); // Continuar al controlador del FIRH
    } catch (err) {
        return res.status(403).json({ error: "Token inválido o expirado." });
    }
}

// Función rápida para generar un token (Úsala en tu ruta de Login o pon un endpoint temporal)
function generarTokenSimulado() {
    // Simulamos los "scopes" exigidos por SMART on FHIR (ej: lectura y escritura de pacientes)
    const payload = {
        iss: "https://auth.hcdistribuida.com",
        sub: "medico_sincelejo_01",
        scope: "patient/*.read patient/*.write encounter/*.read"
    };
    // Expira en 2 horas para la demo
    return jwt.sign(payload, SECRET_KEY, { expiresIn: '2h' });
}

module.exports = { verificarToken, generarTokenSimulado };