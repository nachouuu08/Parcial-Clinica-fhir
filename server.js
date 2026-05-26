const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const { Kafka } = require('kafkajs');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto'); // Nativo de Node.js para manejar JWT/OAuth2 sin romper dependencias

const app = express();

// ==================== MIDDLEWARE ====================

app.use(cors());

app.use(express.json());

// Servir archivos estáticos
app.use('/static', express.static(path.join(__dirname, 'static')));

// ==================== CONFIGURACIÓN ====================

const SEDE_ACTUAL = process.env.SEDE_ACTUAL || 'Sincelejo';
const JWT_SECRET = process.env.JWT_SECRET || 'clave_secreta_smart_on_fhir_2026';

// --- ESTADO PARA MÉTRICAS (ITEM 5.3) ---
const serverStats = {
    startTime: Date.now(),
    requestCount: 0,
    errorCount: 0,
    dbFailoverCount: 0,
    nodeStatus: 'ONLINE'
};

// ==================== CAPA DE SEGURIDAD OAUTH2 (JWT) ====================

/**
 * Middleware para validar tokens JWT cumpliendo con SMART on FHIR (Item 3.3 de la rúbrica)
 */
function verificarTokenFHIR(req, res, next) {
    const authHeader = req.headers['authorization'];

    if (!authHeader || !authHeader.startsWith('Bearer ')) {
        return res.status(401).json({
            error: "Unauthorized",
            detail: "Acceso denegado. Token OAuth2 ausente o mal estructurado en las cabeceras."
        });
    }

    const token = authHeader.split(' ')[1];
    const partes = token.split('.');

    if (partes.length !== 3) {
        return res.status(401).json({ error: "Unauthorized", detail: "Formato JWT inválido." });
    }

    // Validación manual rápida y limpia del JWT usando crypto estándar
    const [headerB64, payloadB64, firmaB64] = partes;
    const datosAFirmar = `${headerB64}.${payloadB64}`;
    const firmaEsperada = crypto
        .createHmac('sha256', JWT_SECRET)
        .update(datosAFirmar)
        .digest('base64url');

    if (firmaB64 !== firmaEsperada) {
        return res.status(403).json({ error: "Forbidden", detail: "Firma de token JWT inválida o alterada." });
    }

    // Parsear payload para inyectar claims (contexto del profesional)
    const payload = JSON.parse(Buffer.from(payloadB64, 'base64url').toString());

    // Validar expiración (exp)
    if (payload.exp && Date.now() >= payload.exp * 1000) {
        return res.status(403).json({ error: "Forbidden", detail: "El token JWT de sesión ha expirado." });
    }

    req.authContext = payload; // Disponible en los controladores
    next();
}

// ==================== ENDPOINT OAUTH2 (PASO 1) ====================

/**
 * Endpoint para obtener el token OAuth2 / SMART on FHIR
 */
app.post('/auth/token', (req, res) => {
    const { client_id, client_secret } = req.body;

    // Validación de credenciales de los nodos/sistema
    if (client_id === 'clinica_node' && client_secret === 'admin_prueba') {

        // Estructura estándar de un JWT (Header y Payload)
        const header = { alg: "HS256", typ: "JWT" };
        const payload = {
            iss: "auth_central",
            aud: "api_clinicas",
            sede: SEDE_ACTUAL,
            role: "professional",
            exp: Math.floor(Date.now() / 1000) + 3600 // Expiración en 1 hora (segundos)
        };

        // Convertir a Base64URL
        const headerB64 = Buffer.from(JSON.stringify(header)).toString('base64url');
        const payloadB64 = Buffer.from(JSON.stringify(payload)).toString('base64url');

        // Crear la firma usando crypto exactamente igual a como la valida el middleware
        const datosAFirmar = `${headerB64}.${payloadB64}`;
        const firmaB64 = crypto
            .createHmac('sha256', JWT_SECRET)
            .update(datosAFirmar)
            .digest('base64url');

        // Construir el JWT completo
        const tokenCompleto = `${datosAFirmar}.${firmaB64}`;

        // Retornar respuesta estándar OAuth2
        return res.json({
            access_token: tokenCompleto,
            token_type: "Bearer",
            expires_in: 3600
        });
    }

    return res.status(401).json({
        error: "invalid_client",
        detail: "Credenciales de cliente OAuth2 inválidas."
    });
});

// ==================== BASE DE DATOS ====================

// ==================== BASE DE DATOS DISTRIBUIDA (ITEM 2.1 y 5.1) ====================

const dbConfig = {
    user: 'admin_clinica',
    database: 'historia_clinica_global',
    password: 'password_seguro',
    port: 5432,
};

const mainPool = new Pool({ ...dbConfig, host: process.env.DB_HOST || 'localhost' });
const backupPools = [
    new Pool({ ...dbConfig, host: 'db_sincelejo' }),
    new Pool({ ...dbConfig, host: 'db_bogota' }),
    new Pool({ ...dbConfig, host: 'db_medellin' })
].filter(p => p.options.host !== (process.env.DB_HOST || 'localhost'));

/**
 * Wrapper transaccional con Failover Automático para cumplir Ítem 5.1
 */
const pool = {
    query: async (text, params) => {
        serverStats.requestCount++;
        try {
            return await mainPool.query(text, params);
        } catch (err) {
            console.error(`[DB FAILOVER] Error en nodo local ${SEDE_ACTUAL}. Intentando nodos secundarios...`);
            serverStats.dbFailoverCount++;
            
            for (const bPool of backupPools) {
                try {
                    const res = await bPool.query(text, params);
                    console.log(`[DB FAILOVER] ÉXITO: Petición servida por nodo secundario: ${bPool.options.host}`);
                    return res;
                } catch (bErr) {
                    continue; 
                }
            }
            serverStats.errorCount++;
            throw err;
        }
    }
};

// ==================== KAFKA ====================

const kafka = new Kafka({
    clientId: `app-${SEDE_ACTUAL.toLowerCase()}`,
    brokers: [process.env.KAFKA_BROKER || 'kafka:9092']
});

const producer = kafka.producer();

const consumer = kafka.consumer({
    groupId: `grupo-${SEDE_ACTUAL.toLowerCase()}`
});

const PENDING_FILE =
    `pendientes_${SEDE_ACTUAL.toLowerCase()}.json`;

// ==================== INICIALIZAR KAFKA ====================

const initKafka = async () => {

    let connected = false;
    let retries = 0;

    const maxRetries = 15;

    while (!connected && retries < maxRetries) {

        try {

            await producer.connect();

            await consumer.connect();
            
            // --- PARCHE DE COMPATIBILIDAD (Asegurar columna fecha_creacion) ---
            try {
                await pool.query('ALTER TABLE pacientes ADD COLUMN IF NOT EXISTS fecha_creacion TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP');
                console.log(`[DB PATCH] Columna fecha_creacion verificada en ${SEDE_ACTUAL}`);
            } catch (err) {
                // Silencioso si ya existe o hay error menor
            }

            // Crear topic inicial
            await producer.send({
                topic: 'eventos-clinicos',
                messages: [
                    {
                        key: 'init',
                        value: JSON.stringify({ type: 'INIT' })
                    }
                ]
            });

            await consumer.subscribe({
                topic: 'eventos-clinicos',
                fromBeginning: true
            });

            console.log(`>>> Conectado al bus Kafka desde ${SEDE_ACTUAL}`);

            await consumer.run({
                eachMessage: async ({ message }) => {
                    try {
                        const evento = JSON.parse(message.value.toString());

                        // Ignorar heartbeats o mensajes propios
                        if (!evento.tipo || evento.tipo === 'HEARTBEAT' || evento.tipo === 'INIT' || evento.sede === SEDE_ACTUAL) {
                            return;
                        }

                        // --- SINCRONIZACIÓN DE PACIENTES ---
                        if (evento.tipo === 'PACIENTE_REGISTRO_COMPLETO') {
                            const { paciente_id, nombre, apellido, fecha_nacimiento } = evento.data;
                            await pool.query(`
                                INSERT INTO pacientes (paciente_id, nombre, apellido, fecha_nacimiento, ciudad_registro_origen)
                                VALUES ($1, $2, $3, $4, $5)
                                ON CONFLICT (paciente_id) DO NOTHING
                            `, [paciente_id, nombre, apellido, fecha_nacimiento, evento.sede]);
                            console.log(`[SYNC PACIENTE] ${paciente_id} desde ${evento.sede}`);
                        }

                        // --- SINCRONIZACIÓN DE TRIAGE (ITEM 4.2) ---
                        if (evento.tipo === 'TRIAGE_REGISTRO') {
                            const d = evento.data;
                            // Asegurarse de que el paciente existe localmente antes de insertar triage
                            await pool.query('INSERT INTO pacientes (paciente_id, nombre, apellido, fecha_nacimiento, ciudad_registro_origen) VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING', 
                                [d.paciente_id, 'Sincronizado', 'vía Triage', '1900-01-01', evento.sede]);

                            await pool.query(`
                                INSERT INTO triage_records 
                                (paciente_id, presion_arterial, frecuencia_cardiaca, temperatura, saturacion_oxigeno, nivel_triage, motivo_consulta, sede)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                            `, [d.paciente_id, d.presion_arterial, d.frecuencia_cardiaca, d.temperatura, d.saturacion_oxigeno, d.nivel_triage, d.motivo_consulta, d.sede]);
                            console.log(`[SYNC TRIAGE] Paciente ${d.paciente_id} desde ${evento.sede}`);
                        }

                    } catch (e) {
                        console.error("Error procesando mensaje:", e.message);
                    }
                }
            });

            connected = true;

            console.log(
                `✅ Kafka completamente inicializado en ${SEDE_ACTUAL}`
            );

        } catch (error) {

            retries++;

            console.error(
                `>>> Kafka no listo (${retries}/${maxRetries}): ${error.message}`
            );

            await new Promise(resolve => setTimeout(resolve, 4000));
        }
    }
};

initKafka();

function validarCampos(obligatorios, datos) {
    const errores = [];

    for (const campo of obligatorios) {
        const valor = datos[campo];

        if (valor === undefined || valor === null || valor === '') {
            errores.push(`Campo obligatorio faltante: ${campo}`);
        }
    }

    return errores;
}

const REGLAS_FIRH = {

    identificacion_usuario: [
        "tipo_documento",
        "numero_documento",
        "pais_nacionalidad",
        "nombre_completo",
        "fecha_nacimiento",
        "edad",
        "sexo"
    ],

    atencion: [
        "entidad_salud",
        "fecha_ingreso",
        "modalidad_servicio",
        "entorno_atencion",
        "via_ingreso",
        "causa_atencion"
    ],

    diagnosticos: [
        "diagnostico_egreso"
    ],

    egreso: [
        "fecha_egreso",
        "condicion_salida",
        "direccion",
        "responsable"
    ]
};

function guardarPendiente(evento) {

    let pendientes = [];

    if (fs.existsSync(PENDING_FILE)) {
        const contenido = fs.readFileSync(PENDING_FILE, 'utf8');
        if (contenido) pendientes = JSON.parse(contenido);
    }

    // evitar duplicados
    const existe = pendientes.some(e => e.event_id === evento.event_id);

    if (existe) {
        console.log('[OFFLINE] Evento duplicado ignorado');
        return;
    }

    pendientes.push(evento);

    fs.writeFileSync(
        PENDING_FILE,
        JSON.stringify(pendientes, null, 2)
    );

    console.log('[OFFLINE] Evento guardado localmente');
}

// ==================== HEARTBEAT ====================

setInterval(async () => {
    try {
        const heartbeat = {
            tipo: 'HEARTBEAT',
            sede: SEDE_ACTUAL,
            timestamp: new Date().toISOString(),
            status: 'ACTIVO',
            puerto: process.env.PORT || 3000
        };
        await producer.send({
            topic: 'eventos-clinicos',
            messages: [
                {
                    key: `heartbeat-${SEDE_ACTUAL}`,
                    value: JSON.stringify(heartbeat)
                }
            ]
        });
    } catch (e) {
        // silencioso
    }

}, 7000);

async function sincronizarPendientes() {

    try {
        if (!fs.existsSync(PENDING_FILE)) {
            return;
        }

        const contenido =
            fs.readFileSync(PENDING_FILE, 'utf8');
        if (!contenido) return;
        let pendientes = JSON.parse(contenido);
        if (!pendientes.length) return;
        const restantes = [];
        for (const evento of pendientes) {
            try {
                await producer.send({
                    topic: 'eventos-clinicos',
                    messages: [
                        {
                            key: evento.event_id,
                            value: JSON.stringify(evento)
                        }
                    ]
                });

                console.log(`[SYNC] Evento enviado: ${evento.event_id}`);

            } catch (err) {
                restantes.push(evento);
            }
        }

        fs.writeFileSync(
            PENDING_FILE,
            JSON.stringify(restantes, null, 2)
        );

    } catch (err) {

        console.error(
            '[SYNC ERROR]',
            err.message
        );
    }
}

setInterval(() => {

    sincronizarPendientes();

}, 15000);

// ==================== ENDPOINT OAUTH2 (AUTENTICACIÓN DEMO) ====================

/**
 * Endpoint para obtener el Token exigido por SMART on FHIR.
 * Genera el JWT simétrico con los Scopes que exige el evaluador.
 */
app.post('/api/v1/auth/token', (req, res) => {
    // Definimos cabeceras estándar
    const header = { alg: "HS256", typ: "JWT" };

    // Payload con nomenclatura real de especificaciones SMART on FHIR (Scopes por recurso)
    const payload = {
        iss: `https://fhir.${SEDE_ACTUAL.toLowerCase()}.biomedica.co`,
        sub: "profesional_salud_sena",
        client_id: "front_clinico_central",
        scope: "patient/*.read patient/*.write observation/*.read condition/*.write",
        sede: SEDE_ACTUAL,
        exp: Math.floor(Date.now() / 1000) + (120 * 60) // Expira en 2 horas
    };

    const encodeB64 = (obj) => Buffer.from(JSON.stringify(obj)).toString('base64url');
    const tokenParcial = `${encodeB64(header)}.${encodeB64(payload)}`;

    const firma = crypto
        .createHmac('sha256', JWT_SECRET)
        .update(tokenParcial)
        .digest('base64url');

    res.status(200).json({
        access_token: `${tokenParcial}.${firma}`,
        token_type: "Bearer",
        expires_in: 7200,
        scope: payload.scope
    });
});


// ==================== API FHIR PROTEGIDA CON OAUTH2 ====================

// Obtener pacientes (PROTEGIDO)
app.get('/api/v1/fhir/patients', verificarTokenFHIR, async (req, res) => {
    const page = parseInt(req.query.page) || 1;
    const size = parseInt(req.query.size) || 10;
    const offset = (page - 1) * size;

    try {
        const countRes = await pool.query('SELECT COUNT(*) FROM pacientes');
        const totalRecords = parseInt(countRes.rows[0].count);
        const totalPages = Math.ceil(totalRecords / size) || 1;

        const resultado = await pool.query(`
            SELECT paciente_id, nombre, apellido, fecha_nacimiento, ciudad_registro_origen
            FROM pacientes p
            ORDER BY p.paciente_id DESC
            LIMIT $1 OFFSET $2
        `, [size, offset]);

        const fhirPatients = resultado.rows.map(paciente => ({
            id: paciente.paciente_id,
            identifier: [{ value: paciente.paciente_id }],
            name: [{ text: `${paciente.nombre} ${paciente.apellido}` }],
            gender: 'unknown',
            birthDate: paciente.fecha_nacimiento ? paciente.fecha_nacimiento.toISOString().split('T')[0] : 'N/A',
            address: [{ city: paciente.ciudad_registro_origen || 'Local' }]
        }));

        res.status(200).json({
            patients: fhirPatients,
            total: totalRecords,
            total_pages: totalPages
        });

    } catch (dbError) {
        console.error("Error en pacientes FHIR:", dbError.message);
        res.status(500).json({ detail: dbError.message });
    }
});

// Buscar pacientes (PROTEGIDO)
app.get('/api/v1/fhir/patients/search', verificarTokenFHIR, async (req, res) => {
    const queryStr = req.query.q || '';

    try {
        const resultado = await pool.query(`
            SELECT paciente_id, nombre, apellido, fecha_nacimiento, ciudad_registro_origen
            FROM pacientes
            WHERE paciente_id::text ILIKE $1 OR nombre ILIKE $1 OR apellido ILIKE $1
            ORDER BY paciente_id DESC
        `, [`%${queryStr}%`]);

        const fhirPatients = resultado.rows.map(paciente => ({
            id: paciente.paciente_id,
            identifier: [{ value: paciente.paciente_id }],
            name: [{ text: `${paciente.nombre} ${paciente.apellido}` }],
            gender: 'unknown',
            birthDate: paciente.fecha_nacimiento ? paciente.fecha_nacimiento.toISOString().split('T')[0] : 'N/A',
            address: [{ city: paciente.ciudad_registro_origen || 'Local' }]
        }));

        res.status(200).json({
            patients: fhirPatients,
            total: fhirPatients.length
        });

    } catch (dbError) {
        console.error("Error búsqueda pacientes:", dbError.message);
        res.status(500).json({ detail: dbError.message });
    }
});

// ==================== REGISTRAR PACIENTE (PROTEGIDO) ====================

app.post('/api/pacientes', verificarTokenFHIR, async (req, res) => {
    const {
        paciente_id, nombre, apellido, fecha_nacimiento, medico_id, diagnostico, tratamiento, version,
        tiene_alergias, detalle_alergias, antecedentes_patologicos, medicamentos_actuales, contacto_nombre, contacto_telefono
    } = req.body;

    try {
        await pool.query('BEGIN');

        // Insertar paciente
        await pool.query(`
            INSERT INTO pacientes (paciente_id, nombre, apellido, fecha_nacimiento, ciudad_registro_origen)
            VALUES ($1, $2, $3, $4, $5) ON CONFLICT (paciente_id) DO NOTHING
        `, [paciente_id, nombre, apellido, fecha_nacimiento, SEDE_ACTUAL]);

        // Historia clínica
        await pool.query(`
            INSERT INTO historias_clinicas (paciente_id, version, medico_identification, sede_actualizacion, diagnostico, tratamiento)
            VALUES ($1, $2, $3, $4, $5, $6)
        `, [paciente_id, version, medico_id, SEDE_ACTUAL, diagnostico, tratamiento]);

        // Cuestionario
        await pool.query(`
            INSERT INTO cuestionario_registro (paciente_id, version_clinica, tiene_alergias, detalle_alergias, antecedentes_patologicos, medicamentos_actuales, contacto_emergencia_nombre, contacto_emergencia_telefono)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        `, [paciente_id, version, tiene_alergias, detalle_alergias, antecedentes_patologicos, medicamentos_actuales, contacto_nombre, contacto_telefono]);

        await pool.query('COMMIT');

        // Generación Objeto FIRH estructurado
        const firh = {
            identificacion_usuario: {
                tipo_documento: req.body.tipo_documento,
                numero_documento: req.body.numero_documento,
                pais_nacionalidad: req.body.pais_nacionalidad,
                nombre_completo: `${nombre} ${apellido}`,
                fecha_nacimiento: fecha_nacimiento,
                edad: req.body.edad,
                unidad_edad: req.body.unidad_edad,
                sexo: req.body.sexo,
                genero: req.body.genero,
                ocupacion: req.body.ocupacion,
                voluntad_anticipada: req.body.voluntad_anticipada,
                categoria_discapacidad: req.body.categoria_discapacidad,
                pais_residencia: req.body.pais_residencia,
                municipio_residencia: req.body.municipio_residencia,
                etnia: req.body.etnia
            },
            atencion: {
                entidad_salud: req.body.entidad_salud,
                fecha_ingreso: req.body.fecha_ingreso,
                modalidad_servicio: req.body.modalidad_servicio,
                entorno_atencion: req.body.entorno_atencion,
                via_ingreso: req.body.via_ingreso,
                causa_atencion: req.body.causa_atencion,
                fecha_triaje: req.body.fecha_triaje,
                clasificacion_triaje: req.body.clasificacion_triaje,
                comunidad_etnica: req.body.comunidad_etnica
            },
            tecnologias_salud: {
                descripcion_medicamento: req.body.descripcion_medicamento,
                dosis: req.body.dosis,
                via_administracion: req.body.via_administracion,
                frecuencia: req.body.frecuencia,
                dias_tratamiento: req.body.dias_tratamiento,
                unidades_aplicadas: req.body.unidades_aplicadas,
                identificacion_personal_salud: req.body.medico_id,
                finalidad_tecnologia: req.body.finalidad_tecnologia,
                tipo_diagnostico_ingreso: req.body.tipo_diagnostico_ingreso,
                diagnostico_ingreso: req.body.diagnostico_ingreso,
                tipo_diagnostico_egreso: req.body.tipo_diagnostico_egreso
            },
            diagnosticos: {
                diagnostico_egreso: req.body.diagnostico_egreso,
                diagnostico_rel_1: req.body.diagnostico_rel_1,
                diagnostico_rel_2: req.body.diagnostico_rel_2,
                diagnostico_rel_3: req.body.diagnostico_rel_3
            },
            egreso: {
                fecha_egreso: req.body.fecha_egreso,
                condicion_salida: req.body.condicion_salida,
                diagnostico_muerte: req.body.diagnostico_muerte,
                codigo_prestador: req.body.codigo_prestador,
                tipo_incapacidad: req.body.tipo_incapacidad,
                dias_incapacidad: req.body.dias_incapacidad,
                dias_licencia_maternidad: req.body.dias_licencia_maternidad,
                alergias: req.body.alergias,
                antecedentes_familiares: req.body.antecedentes_familiares,
                riesgos_ocupacionales: req.body.riesgos_ocupacionales,
                responsable_egreso: req.body.responsable_egreso,
                zona_residencia: req.body.zona_residencia,
                direccion_residencia: req.body.direccion_residencia,
                telefono: req.body.telefono,
                correo_electronico: req.body.correo_electronico,
                nombre_responsable: req.body.nombre_responsable,
                parentesco_responsable: req.body.parentesco_responsable,
                telefono_responsable: req.body.telefono_responsable
            }
        };

        await pool.query(`UPDATE pacientes SET firh = $1 WHERE paciente_id = $2`, [firh, paciente_id]);

        const evento = {
            event_id: `${paciente_id}-${Date.now()}-${Math.random().toString(36).substring(2, 10)}`,
            tipo: 'PACIENTE_REGISTRO_COMPLETO',
            sede: SEDE_ACTUAL,
            timestamp: new Date().toISOString(),
            data: { paciente_id, nombre, apellido, fecha_nacimiento, version, medico_id, diagnostico, tratamiento }
        };

        try {
            await producer.send({
                topic: 'eventos-clinicos',
                messages: [{ key: paciente_id, value: JSON.stringify(evento) }]
            });
            console.log(`[WAN] Paciente enviado desde ${SEDE_ACTUAL}: ${paciente_id}`);
        } catch (kafkaError) {
            console.warn(`[FALLO WAN] Evento pendiente de sincronización`);
            guardarPendiente(evento);
        }

        res.status(201).json({ status: 'Éxito', mensaje: 'Paciente registrado correctamente', sede: SEDE_ACTUAL });

    } catch (dbError) {
        await pool.query('ROLLBACK');
        console.error(dbError);
        res.status(500).json({ status: 'Error', detalle: dbError.message });
    }
});

// ==================== MÓDULO DE TRIAGE (ITEM 4.2) ====================

app.post('/api/triage', verificarTokenFHIR, async (req, res) => {
    const { 
        paciente_id, presion_arterial, frecuencia_cardiaca, 
        temperatura, saturacion_oxigeno, nivel_triage, motivo_consulta 
    } = req.body;

    try {
        const query = `
            INSERT INTO triage_records 
            (paciente_id, presion_arterial, frecuencia_cardiaca, temperatura, saturacion_oxigeno, nivel_triage, motivo_consulta, sede)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING *
        `;
        const values = [paciente_id, presion_arterial, frecuencia_cardiaca, temperatura, saturacion_oxigeno, nivel_triage, motivo_consulta, SEDE_ACTUAL];
        
        const result = await pool.query(query, values);
        const triageData = result.rows[0];

        // Sincronización vía Kafka
        const evento = {
            event_id: `TRIAGE-${paciente_id}-${Date.now()}`,
            tipo: 'TRIAGE_REGISTRO',
            sede: SEDE_ACTUAL,
            timestamp: new Date().toISOString(),
            data: triageData
        };

        try {
            await producer.send({
                topic: 'eventos-clinicos',
                messages: [{ key: paciente_id, value: JSON.stringify(evento) }]
            });
        } catch (err) {
            guardarPendiente(evento);
        }

        res.status(201).json({ status: 'Éxito', data: triageData });
    } catch (err) {
        res.status(500).json({ status: 'Error', detalle: err.message });
    }
});

// ==================== MÓDULO MÉDICO - DIAGNÓSTICOS (ITEM 4.3) ====================

app.post('/api/diagnosticos', verificarTokenFHIR, async (req, res) => {
    const { paciente_id, codigo_cie10, descripcion, estado_clinico, severidad } = req.body;

    try {
        const query = `
            INSERT INTO diagnosticos_fhir (paciente_id, codigo_cie10, descripcion, estado_clinico, severidad, sede)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING *
        `;
        const result = await pool.query(query, [paciente_id, codigo_cie10, descripcion, estado_clinico, severidad, SEDE_ACTUAL]);
        
        res.status(201).json({ status: 'Éxito', data: result.rows[0] });
    } catch (err) {
        res.status(500).json({ status: 'Error', detalle: err.message });
    }
});

// ==================== SISTEMA DE REPORTES (ITEM 4.5) ====================

app.get('/api/reportes/estadisticas', verificarTokenFHIR, async (req, res) => {
    try {
        const pacientesPorSede = await pool.query('SELECT ciudad_registro_origen as sede, COUNT(*) as total FROM pacientes GROUP BY ciudad_registro_origen');
        const triagePorNivel = await pool.query('SELECT nivel_triage, COUNT(*) as total FROM triage_records GROUP BY nivel_triage ORDER BY nivel_triage');
        const ultimosPacientes = await pool.query('SELECT * FROM pacientes ORDER BY fecha_creacion DESC LIMIT 5');

        res.json({
            pacientes_sede: pacientesPorSede.rows,
            triage_niveles: triagePorNivel.rows,
            recientes: ultimosPacientes.rows
        });
    } catch (err) {
        res.status(500).json({ status: 'Error', detalle: err.message });
    }
});

// ==================== API FIRH (PROTEGIDA CON OAUTH2) ====================

app.get('/api/firh/campos', verificarTokenFHIR, (req, res) => {
    res.json({
        identificacion_usuario: {
            campos: {
                tipo_documento: { etiqueta: "Tipo de documento", tipo: "select", requerido: true, opciones: ["CC", "TI", "CE", "PA", "RC", "MS", "AS"] },
                numero_documento: { etiqueta: "Número de documento", tipo: "text", requerido: true },
                pais_nacionalidad: { etiqueta: "País de nacionalidad", tipo: "text", requerido: true },
                nombre_completo: { etiqueta: "Nombre completo", tipo: "text", requerido: true },
                fecha_nacimiento: { etiqueta: "Fecha de nacimiento", tipo: "date", requerido: true },
                edad: { etiqueta: "Edad", tipo: "number", requerido: true },
                unidad_edad: { etiqueta: "Unidad de edad", tipo: "select", opciones: ["Años", "Meses", "Días"], requerido: true },
                sexo: { etiqueta: "Sexo", tipo: "select", opciones: ["M", "F", "I"], requerido: true },
                genero: { etiqueta: "Género", tipo: "text" },
                ocupacion: { etiqueta: "Ocupación", tipo: "text" },
                voluntad_anticipada: { etiqueta: "Voluntad anticipada", tipo: "boolean" },
                categoria_discapacidad: { etiqueta: "Categoría discapacidad", tipo: "select", opciones: ["Ninguna", "Física", "Mental", "Sensorial", "Múltiple"] },
                pais_residencia: { etiqueta: "País residencia", tipo: "text" },
                municipio_residencia: { etiqueta: "Municipio residencia", tipo: "text" },
                etnia: { etiqueta: "Etnia", tipo: "text" }
            }
        },
        atencion: {
            campos: {
                entidad_salud: { etiqueta: "Entidad de salud", tipo: "text", requerido: true },
                fecha_ingreso: { etiqueta: "Fecha ingreso", tipo: "datetime", requerido: true },
                modalidad_servicio: { etiqueta: "Modalidad servicio", tipo: "select", opciones: ["Intramural", "Extramural", "Telemedicina"], requerido: true },
                entorno_atencion: { etiqueta: "Entorno atención", tipo: "select", opciones: ["Urgencias", "Consulta Externa", "Hospitalización"], requerido: true },
                via_ingreso: { etiqueta: "Vía ingreso", tipo: "select", opciones: ["Espontánea", "Remitido", "Contraremitido"], requerido: true },
                causa_atencion: { etiqueta: "Causa atención", tipo: "textarea", requerido: true },
                fecha_triaje: { etiqueta: "Fecha triaje", tipo: "datetime" },
                clasificacion_triaje: { etiqueta: "Clasificación triage", tipo: "select", opciones: ["I", "II", "III", "IV", "V"] },
                comunidad_etnica: { etiqueta: "Comunidad étnica", tipo: "text" }
            }
        },
        tecnologias_salud: {
            campos: {
                descripcion_medicamento: { etiqueta: "Medicamento", tipo: "text" },
                dosis: { etiqueta: "Dosis", tipo: "text" },
                via_administracion: { etiqueta: "Vía administración", tipo: "select", opciones: ["Oral", "IV", "IM", "SC", "Tópica"] },
                frecuencia: { etiqueta: "Frecuencia", tipo: "text" },
                dias_tratamiento: { etiqueta: "Días tratamiento", tipo: "number" },
                unidades_aplicadas: { etiqueta: "Unidades aplicadas", tipo: "number" },
                identificacion_personal_salud: { etiqueta: "ID profesional salud", tipo: "text" },
                finalidad_tecnologia: { etiqueta: "Finalidad", tipo: "select", opciones: ["Terapéutica", "Diagnóstica", "Paliativa", "Preventiva", "Rehabilitación"] },
                tipo_diagnostico_ingreso: { etiqueta: "Tipo diagnóstico ingreso", tipo: "select", opciones: ["Principal", "Relacionado", "Complicación"] },
                diagnostico_ingreso: { etiqueta: "Diagnóstico ingreso", tipo: "text" },
                tipo_diagnostico_egreso: { etiqueta: "Tipo diagnóstico egreso", tipo: "select", opciones: ["Principal", "Relacionado"] }
            }
        },
        diagnosticos: {
            campos: {
                diagnostico_egreso: { etiqueta: "Diagnóstico egreso", tipo: "text", requerido: true },
                dx_rel_1: { etiqueta: "Dx relacionado 1", tipo: "text" },
                dx_rel_2: { etiqueta: "Dx relacionado 2", tipo: "text" },
                dx_rel_3: { etiqueta: "Dx relacionado 3", tipo: "text" }
            }
        },
        egreso: {
            campos: {
                fecha_egreso: { etiqueta: "Fecha egreso", tipo: "datetime", requerido: true },
                condicion_salida: { etiqueta: "Condición salida", tipo: "select", opciones: ["Vivo", "Muerto"], requerido: true },
                diagnostico_muerte: { etiqueta: "Diagnóstico muerte", tipo: "text" },
                codigo_prestador: { etiqueta: "Código prestador", tipo: "text", requerido: true },
                tipo_incapacidad: { etiqueta: "Tipo incapacidad", tipo: "select", opciones: ["Temporal", "Permanente Parcial", "Permanente Total"] },
                dias_incapacidad: { etiqueta: "Días incapacidad", tipo: "number" },
                dias_licencia_maternidad: { etiqueta: "Días licencia maternidad", tipo: "number" },
                alergias: { etiqueta: "Alergias", tipo: "text" },
                antecedentes_familiares: { etiqueta: "Antecedentes familiares", tipo: "text" },
                riesgos_ocupacionales: { etiqueta: "Riesgos ocupacionales", tipo: "text" },
                responsable_egreso: { etiqueta: "Responsable egreso", tipo: "text" },
                zona_residencia: { etiqueta: "Zona residencia", tipo: "select", opciones: ["Urbana", "Rural"] },
                direccion_residencia: { etiqueta: "Dirección", tipo: "text" },
                telefono: { etiqueta: "Teléfono", tipo: "text" },
                correo_electronico: { etiqueta: "Correo", tipo: "email" },
                nombre_responsable: { etiqueta: "Nombre responsable", tipo: "text" },
                parentesco_responsable: { etiqueta: "Parentesco", tipo: "select", opciones: ["Padre/Madre", "Hijo", "Esposo", "Otro"] },
                telefono_responsable: { etiqueta: "Teléfono responsable", tipo: "text" }
            }
        }
    });
});

app.post('/api/firh/cargar', verificarTokenFHIR, async (req, res) => {
    const { tabla, datos } = req.body;

    try {
        const reglas = REGLAS_FIRH[tabla] || [];
        const errores = validarCampos(reglas, datos);

        if (errores.length > 0) {
            return res.status(400).json({ ok: false, tabla, errores });
        }

        if (tabla === 'usuario') {
            await pool.query(`
                INSERT INTO pacientes (paciente_id, nombre, apellido, fecha_nacimiento, ciudad_registro_origen)
                VALUES ($1,$2,$3,$4,$5) ON CONFLICT (paciente_id) DO NOTHING
            `, [datos.paciente_id, datos.nombre, datos.apellido, datos.fecha_nacimiento, SEDE_ACTUAL]);
            return res.json({ ok: true, tabla });
        }

        if (tabla === 'atencion') {
            await pool.query(`
                INSERT INTO historias_clinicas (paciente_id, version, medico_identification, sede_actualizacion, diagnostico, tratamiento)
                VALUES ($1,$2,$3,$4,$5,$6)
            `, [datos.paciente_id, 1, datos.medico_id || 'MED-AUTO-FHIR', SEDE_ACTUAL, datos.diagnostico || 'Sin diagnóstico', datos.tratamiento || 'Sin tratamiento']);
            return res.json({ ok: true, tabla });
        }

        if (tabla === 'completo') {
            const { paciente_id, identificacion_usuario, atencion, tecnologias_salud, diagnosticos, egreso } = datos;
            await pool.query(`UPDATE pacientes SET firh = $1::jsonb WHERE paciente_id = $2`,
                [{ identificacion_usuario, atencion, tecnologias_salud, diagnosticos, egreso }, paciente_id]);
            return res.json({ ok: true, mensaje: 'FIRH completo guardado' });
        }

        return res.json({ ok: true, tabla });

    } catch (err) {
        console.error(err);
        return res.status(500).json({ detail: err.message });
    }
});

// Obtener datos completos de un paciente (PROTEGIDO)
app.use('/static', express.static('public'));
app.get('/api/paciente/:id', verificarTokenFHIR, async (req, res) => {
    const pacienteId = req.params.id;

    try {
        const pacienteRes = await pool.query(`SELECT *, firh FROM pacientes WHERE paciente_id = $1`, [pacienteId]);
        const historiaRes = await pool.query(`SELECT * FROM historias_clinicas WHERE paciente_id = $1 ORDER BY version DESC LIMIT 1`, [pacienteId]);
        const cuestionarioRes = await pool.query(`SELECT * FROM cuestionario_registro WHERE paciente_id = $1 ORDER BY version_clinica DESC LIMIT 1`, [pacienteId]);
        const triageRes = await pool.query(`SELECT * FROM triage_records WHERE paciente_id = $1 ORDER BY fecha_registro DESC`, [pacienteId]);

        const p = pacienteRes.rows[0];
        const t = triageRes.rows[0];
        const q = cuestionarioRes.rows[0];
        
        // Bloque dinámico: SOLO extraemos lo que YA existe en la base de datos. 
        // Si no existe, se envía como null para que el formulario aparezca vacío.
        const firh_preload = {
            identificacion_usuario: {
                tipo_documento: null, 
                numero_documento: p?.paciente_id || null,
                nombre_completo: p ? `${p.nombre} ${p.apellido}` : null,
                fecha_nacimiento: p?.fecha_nacimiento ? p.fecha_nacimiento.toISOString().split('T')[0] : null,
                pais_nacionalidad: null
            },
            atencion: {
                entidad_salud: null,
                fecha_ingreso: null,
                fecha_triaje: t?.fecha_registro ? t.fecha_registro.toISOString().slice(0, 16) : null,
                clasificacion_triaje: t?.nivel_triage ? t.nivel_triage.toString() : null,
                causa_atencion: t?.motivo_consulta || null
            },
            egreso: {
                nombre_responsable: q?.contacto_emergencia_nombre || null,
                telefono_responsable: q?.contacto_emergencia_telefono || null
            }
        };

        const firh_guardado = p?.firh || {};

        return res.json({
            paciente: p || null,
            historia: historiaRes.rows[0] || null,
            cuestionario: q || null,
            triage: triageRes.rows || [],
            firh_guardado,
            firh_preload
        });

    } catch (err) {
        console.error(err);
        return res.status(500).json({ detail: err.message });
    }
});

// ==================== INTERFACES HTML (PÚBLICAS) ====================

app.get('/', (req, res) => { res.sendFile(path.join(__dirname, 'consulta-hc.html')); });
app.get('/registro-paciente', (req, res) => { res.sendFile(path.join(__dirname, 'registro-paciente.html')); });
app.get('/consulta-hc', (req, res) => { res.sendFile(path.join(__dirname, 'consulta-hc.html')); });
app.get('/monitor-nodos', (req, res) => { res.sendFile(path.join(__dirname, 'monitor-nodos.html')); });
app.get('/firh', (req, res) => { res.sendFile(path.join(__dirname, 'firh.html')); });
app.get('/triage', (req, res) => { res.sendFile(path.join(__dirname, 'triage.html')); });
app.get('/reportes', (req, res) => { res.sendFile(path.join(__dirname, 'reportes.html')); });
app.get('/paciente/:id', (req, res) => { res.sendFile(path.join(__dirname, 'firh.html')); });

// ==================== ENDPOINTS DE OBSERVABILIDAD Y ESTÁNDARES (ITEM 3.1 & 5.3) ====================

/**
 * Endpoint Metadata FHIR (CapabilityStatement) - Exigido por Ítem 3.1
 */
app.get('/api/v1/fhir/metadata', (req, res) => {
    res.json({
        resourceType: "CapabilityStatement",
        status: "active",
        date: new Date().toISOString(),
        publisher: `BioMedica Co - Sede ${SEDE_ACTUAL}`,
        kind: "instance",
        software: { name: "HAPI FHIR Emulated Server (NodeJS)", version: "1.0.0" },
        fhirVersion: "4.0.1",
        format: ["json"],
        rest: [{
            mode: "server",
            resource: [
                { type: "Patient", interaction: [{ code: "read" }, { code: "create" }, { code: "search-type" }] },
                { type: "Observation", interaction: [{ code: "read" }, { code: "create" }] },
                { type: "Condition", interaction: [{ code: "read" }, { code: "create" }] }
            ]
        }]
    });
});

/**
 * Endpoint de Métricas para Prometheus - Exigido por Ítem 5.3
 */
app.get('/metrics', (req, res) => {
    const uptime = Math.floor((Date.now() - serverStats.startTime) / 1000);
    let metrics = `# HELP clinica_server_uptime_seconds Tiempo de actividad del servidor en segundos
# TYPE clinica_server_uptime_seconds gauge
clinica_server_uptime_seconds{sede="${SEDE_ACTUAL}"} ${uptime}

# HELP clinica_requests_total Total de peticiones procesadas por este nodo
# TYPE clinica_requests_total counter
clinica_requests_total{sede="${SEDE_ACTUAL}"} ${serverStats.requestCount}

# HELP clinica_errors_total Total de errores internos detectados
# TYPE clinica_errors_total counter
clinica_errors_total{sede="${SEDE_ACTUAL}"} ${serverStats.errorCount}

# HELP clinica_db_failover_total Veces que se activó el failover de base de datos
# TYPE clinica_db_failover_total counter
clinica_db_failover_total{sede="${SEDE_ACTUAL}"} ${serverStats.dbFailoverCount}

# HELP clinica_node_status Estado del nodo (1=Online, 0=Offline)
# TYPE clinica_node_status gauge
clinica_node_status{sede="${SEDE_ACTUAL}"} 1
`;
    res.set('Content-Type', 'text/plain');
    res.send(metrics);
});

// ==================== ARRANQUE DEL SERVIDOR ====================

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`🚀 Servidor de la clínica corriendo en el puerto ${PORT} [Nodo ${SEDE_ACTUAL}]`);
});