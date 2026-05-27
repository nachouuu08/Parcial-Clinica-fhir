const express = require('express');
const cors = require('cors');
const { Kafka } = require('kafkajs');
const crypto = require('crypto');
const path = require('path');
const fs = require('fs');
const { Pool } = require('pg');

const app = express();
app.use(cors());
app.use(express.json());
app.use('/static', express.static('static'));

const JWT_SECRET = process.env.JWT_SECRET || 'clave_secreta_smart_on_fhir_2026';
const PENDING_FILE = path.join(process.cwd(), 'pendientes_gateway.json');
const SYNC_QUEUE_FILE = path.join(process.cwd(), 'sync_queue_gateway.json');

const globalPool = new Pool({
    user: 'admin_clinica',
    database: 'historia_clinica_global',
    password: 'password_seguro',
    port: 5432,
    host: process.env.GLOBAL_DB_HOST || 'db_global'
});

const SEDE_HOSTS = { Sincelejo: 'app_sincelejo', Bogota: 'app_bogota', Medellin: 'app_medellin' };
const SEDE_PORTS = { Sincelejo: 3001, Bogota: 3002, Medellin: 3003 };
const HOST_IP = process.env.HOST_IP || '192.168.101.14';

async function replicarASede(datos) {
    const sede = datos.sede_destino;
    const port = SEDE_PORTS[sede];
    if (!port) return;
    try {
        const response = await fetch(`http://${HOST_IP}:${port}/api/registro/paciente`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(datos)
        });
        console.log(`[GATEWAY] Replicado a ${sede}:${port}`, response.ok ? 'OK' : 'ERROR');
    } catch (err) {
        console.warn(`[GATEWAY] No se pudo replicar a ${sede}:`, err.message);
    }
}

function guardarPendiente(evento) {
    let pendientes = [];
    if (fs.existsSync(PENDING_FILE)) {
        try { pendientes = JSON.parse(fs.readFileSync(PENDING_FILE, 'utf8')); } catch {}
    }
    pendientes.push({...evento, timestamp: Date.now()});
    fs.writeFileSync(PENDING_FILE, JSON.stringify(pendientes, null, 2));
}

function guardarEnCola(sedeDestino, datos, tipo) {
    let cola = [];
    if (fs.existsSync(SYNC_QUEUE_FILE)) {
        try { cola = JSON.parse(fs.readFileSync(SYNC_QUEUE_FILE, 'utf8')); } catch {}
    }
    cola.push({ sede: sedeDestino, tipo, datos, timestamp: Date.now(), intentos: 0 });
    fs.writeFileSync(SYNC_QUEUE_FILE, JSON.stringify(cola, null, 2));
}

// Procesar cola de sincronización cada 5 segundos
async function procesarCola() {
    if (!fs.existsSync(SYNC_QUEUE_FILE)) return;
    let cola = [];
    try { cola = JSON.parse(fs.readFileSync(SYNC_QUEUE_FILE, 'utf8')); } catch { return; }
    const pendientes = [];
    for (const item of cola) {
        const host = SEDE_HOSTS[item.sede];
        if (!host) continue;
        try {
            const endpoint = item.tipo === 'triage' ? '/api/triage' : '/api/registro/paciente';
            const response = await fetch(`http://${host}:3000${endpoint}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(item.datos)
            });
            if (response.ok) {
                console.log(`[GATEWAY] Sincronizado ${item.tipo} a ${item.sede}`);
            } else {
                throw new Error(`HTTP ${response.status}`);
            }
        } catch (err) {
            item.intentos = (item.intentos || 0) + 1;
            if (item.intentos < 30) pendientes.push(item);
            else console.error(`[GATEWAY] Falló sincronización a ${item.sede} después de ${item.intentos} intentos`);
        }
    }
    fs.writeFileSync(SYNC_QUEUE_FILE, JSON.stringify(pendientes, null, 2));
}

setInterval(procesarCola, 5000);

let producer = null;
try {
    const kafka = new Kafka({ clientId: 'gateway-registro', brokers: [process.env.KAFKA_BROKER || 'kafka:9092'] });
    producer = kafka.producer();
} catch (e) { console.warn('[GATEWAY] Kafka no disponible'); }

async function initKafka() {
    try { if (producer) { await producer.connect(); console.log('✅ Gateway conectado a Kafka'); } }
    catch (err) { console.warn('[GATEWAY] Kafka no disponible:', err.message); }
}
initKafka();

app.post('/api/v1/auth/token', (req, res) => {
    const header = { alg: 'HS256', typ: 'JWT' };
    const payload = { iss: 'gateway_registro', sub: 'doctor_gateway', scope: 'patient/*.write', exp: Math.floor(Date.now() / 1000) + 3600 };
    const encode = (obj) => Buffer.from(JSON.stringify(obj)).toString('base64url');
    const token = `${encode(header)}.${encode(payload)}.dummy`;
    res.json({ access_token: token, token_type: 'Bearer', expires_in: 3600 });
});

const MEDICOS = {
    'MED-001': { nombre: 'Carlos', apellido: 'Gómez', sede_asignada: 'Sincelejo', especialidad: 'Medicina General' },
    'MED-002': { nombre: 'Ana', apellido: 'Rodríguez', sede_asignada: 'Bogota', especialidad: 'Medicina Interna' },
    'MED-003': { nombre: 'Luis', apellido: 'Martínez', sede_asignada: 'Medellin', especialidad: 'Pediatría' }
};

app.post('/api/v1/auth/medico', async (req, res) => {
    const { medico_id, client_id, client_secret } = req.body;
    if (!medico_id) return res.status(400).json({ ok: false, detail: 'medico_id es obligatorio' });
    if (client_id !== 'clinica_node' || client_secret !== 'admin_prueba') {
        return res.status(401).json({ ok: false, detail: 'Credenciales inválidas' });
    }
    const medico = MEDICOS[medico_id];
    const sede = medico?.sede_asignada || 'Sincelejo';
    const header = { alg: 'HS256', typ: 'JWT' };
    const payload = { iss: 'auth_medico_gateway', sub: medico_id, client_id: 'front_clinico_central', scope: 'patient/*.read patient/*.write', sede, exp: Math.floor(Date.now() / 1000) + 3600 };
    const encode = (obj) => Buffer.from(JSON.stringify(obj)).toString('base64url');
    const tokenParcial = `${encode(header)}.${encode(payload)}`;
    const firma = crypto.createHmac('sha256', JWT_SECRET).update(tokenParcial).digest('base64url');
    const token = `${tokenParcial}.${firma}`;
    res.json({ access_token: token, token_type: 'Bearer', expires_in: 3600, sede, medico: medico || { medico_id, sede_asignada: sede } });
});

app.post('/api/registro/paciente', async (req, res) => {
    const datos = req.body || {};
    const { paciente_id, sede_destino } = datos;
    if (!paciente_id || !sede_destino) {
        return res.status(400).json({ ok: false, detail: 'paciente_id y sede_destino son obligatorios' });
    }
    const evento = {
        event_id: `GATEWAY-${paciente_id}-${Date.now()}`,
        tipo: 'PACIENTE_REGISTRO_SOLICITADO',
        sede_origen: 'GATEWAY',
        sede_destino: sede_destino,
        medico_id: datos.medico_id,
        timestamp: new Date().toISOString(),
        data: datos
    };
    try {
        // Guardar en BD global como backup central
        await globalPool.query(
            `INSERT INTO pacientes (paciente_id, nombre, apellido, fecha_nacimiento, ciudad_registro_origen, firh) 
             VALUES ($1, $2, $3, $4, $5, $6) 
             ON CONFLICT (paciente_id) DO UPDATE 
             SET nombre = EXCLUDED.nombre, apellido = EXCLUDED.apellido, 
                 fecha_nacimiento = EXCLUDED.fecha_nacimiento, 
                 ciudad_registro_origen = EXCLUDED.ciudad_registro_origen,
                 firh = EXCLUDED.firh`,
            [
                paciente_id,
                datos.nombre || 'Sin nombre',
                datos.apellido || 'Sin apellido',
                datos.fecha_nacimiento || new Date().toISOString().split('T')[0],
                sede_destino,
                JSON.stringify(datos.firh || {})
            ]
        );
        
        if (producer) {
            await producer.send({ topic: 'eventos-clinicos', messages: [{ key: String(paciente_id), value: JSON.stringify(evento) }] });
        }
        // Replicar al nodo sede destino vía HTTP directo
        replicarASede(datos).catch(() => {});
        return res.status(201).json({ ok: true, mensaje: 'Paciente procesado y guardado en BD global', sede_destino });
    } catch (err) {
        guardarPendiente(evento);
        return res.status(202).json({ ok: true, mensaje: 'Guardado para sincronización offline', offline: true });
    }
});

app.post('/api/firh/cargar', async (req, res) => {
    const { tabla, datos } = req.body;
    if (!datos?.paciente_id) {
        return res.status(400).json({ ok: false, detail: 'paciente_id es obligatorio' });
    }
    const pacienteId = datos.paciente_id;
    const firhCompleto = {
        identificacion_usuario: datos.identificacion_usuario || {},
        atencion: datos.atencion || {},
        tecnologias_salud: datos.tecnologias_salud || {},
        diagnosticos: datos.diagnosticos || {},
        egreso: datos.egreso || {}
    };
    
    try {
        // Guardar en BD global (backup central)
        await globalPool.query(
            'UPDATE pacientes SET firh = $1 WHERE paciente_id = $2',
            [JSON.stringify(firhCompleto), pacienteId]
        );
        res.json({ ok: true, mensaje: 'FIRH guardado en BD global', firh: firhCompleto });
    } catch (err) {
        guardarPendiente({ tipo: 'FIRH_GUARDADO', paciente_id: pacienteId, ...firhCompleto });
        res.status(202).json({ ok: true, mensaje: 'Guardado offline para sincronizar después', offline: true, firh: firhCompleto });
    }
});

app.get('/api/firh/historias', async (req, res) => {
    const pacienteId = req.query.paciente_id;
    if (!pacienteId) return res.status(400).json({ detail: 'paciente_id es obligatorio' });
    try {
        const result = await globalPool.query(
            'SELECT firh FROM pacientes WHERE paciente_id = $1',
            [pacienteId]
        );
        const historias = [];
        if (result.rows[0] && result.rows[0].firh) {
            historias.push({ version: 1, sede_actualizacion: 'Global', ...(result.rows[0].firh) });
        }
        res.json({ historias });
    } catch (err) {
        res.status(500).json({ detail: err.message });
    }
});

app.get('/api/paciente/:id', async (req, res) => {
    const pacienteId = decodeURIComponent(req.params.id).trim();
    try {
        const result = await globalPool.query('SELECT * FROM pacientes WHERE paciente_id = $1', [pacienteId]);
        if (!result.rows[0]) {
            return res.status(404).json({ detail: `Paciente "${pacienteId}" no encontrado` });
        }
        const row = result.rows[0];
        const firhData = row.firh || {};
        
        // Obtener triage records
        const triageResult = await globalPool.query("SELECT * FROM triage_records WHERE paciente_id = $1 ORDER BY fecha_registro DESC LIMIT 50", [pacienteId]);
        const triage = triageResult.rows.map(r => ({
            nivel_triage: r.nivel_triage,
            motivo_consulta: r.motivo_consulta,
            presion_arterial: r.presion_arterial,
            frecuencia_cardiaca: r.frecuencia_cardiaca,
            temperatura: r.temperatura,
            saturacion_oxigeno: r.saturacion_oxigeno,
            sede: r.sede,
            fecha_registro: r.fecha_registro
        }));
        
        const historias = firhData ? [{ 
            version: 1, 
            sede_actualizacion: row.ciudad_registro_origen, 
            ...firhData 
        }] : [];
        
        res.json({
            paciente: {
                paciente_id: row.paciente_id,
                nombre: row.nombre,
                apellido: row.apellido,
                fecha_nacimiento: row.fecha_nacimiento,
                tipo_documento: row.tipo_documento || 'CC'
            },
            triage,
            historias,
            cuestionario: null,
            firh_guardado: firhData
        });
    } catch (err) {
        res.status(500).json({ detail: err.message });
    }
});

app.post('/api/triage', async (req, res) => {
    const { paciente_id, presion_arterial, frecuencia_cardiaca, temperatura, saturacion_oxigeno, nivel_triage, motivo_consulta } = req.body;
    if (!paciente_id) return res.status(400).json({ ok: false, detail: 'paciente_id es obligatorio' });
    if (!nivel_triage) return res.status(400).json({ ok: false, detail: 'nivel_triage es obligatorio' });
    try {
        await globalPool.query(
            `INSERT INTO triage_records (paciente_id, presion_arterial, frecuencia_cardiaca, temperatura, saturacion_oxigeno, nivel_triage, motivo_consulta, sede) 
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
            [paciente_id, presion_arterial, parseInt(frecuencia_cardiaca) || null, parseFloat(temperatura) || null, parseInt(saturacion_oxigeno) || null, parseInt(nivel_triage), motivo_consulta, 'GATEWAY']
        );
        // Replicar a TODAS las sedes, encolar si falla
        for (const sede of Object.keys(SEDE_HOSTS)) {
            try {
                const response = await fetch(`http://${SEDE_HOSTS[sede]}:3000/api/triage`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ ...req.body, esReplicado: true })
                });
                if (!response.ok) throw new Error(`HTTP ${response.status}`);
            } catch (e) {
                guardarEnCola(sede, { ...req.body, esReplicado: true }, 'triage');
            }
        }
        res.json({ ok: true, mensaje: 'Triage guardado en BD global y todas las sedes' });
    } catch (err) {
        guardarPendiente({ tipo: 'TRIAGE_GUARDADO', ...req.body });
        res.status(202).json({ ok: true, mensaje: 'Guardado offline para sincronizar después', offline: true });
    }
});

app.get('/triage', (req, res) => res.sendFile(path.join(__dirname, 'triage.html')));

const PORT = process.env.PORT || 3000;

app.get('/api/firh/campos', (req, res) => {
    res.json({
        identificacion_usuario: { campos: {
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
        }},
        atencion: { campos: {
            entidad_salud: { etiqueta: "Entidad de salud", tipo: "text", requerido: true },
            fecha_ingreso: { etiqueta: "Fecha ingreso", tipo: "datetime", requerido: true },
            modalidad_servicio: { etiqueta: "Modalidad servicio", tipo: "select", opciones: ["Intramural", "Extramural", "Telemedicina"], requerido: true },
            entorno_atencion: { etiqueta: "Entorno atención", tipo: "select", opciones: ["Urgencias", "Consulta Externa", "Hospitalización"], requerido: true },
            via_ingreso: { etiqueta: "Vía ingreso", tipo: "select", opciones: ["Espontánea", "Remitido", "Contraremitido"], requerido: true },
            causa_atencion: { etiqueta: "Causa atención", tipo: "textarea", requerido: true },
            fecha_triaje: { etiqueta: "Fecha triaje", tipo: "datetime" },
            clasificacion_triaje: { etiqueta: "Clasificación triage", tipo: "select", opciones: ["I", "II", "III", "IV", "V"] },
            comunidad_etnica: { etiqueta: "Comunidad étnica", tipo: "text" }
        }},
        tecnologias_salud: { campos: {
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
        }},
        diagnosticos: { campos: {
            diagnostico_egreso: { etiqueta: "Diagnóstico egreso", tipo: "text", requerido: true },
            dx_rel_1: { etiqueta: "Dx relacionado 1", tipo: "text" },
            dx_rel_2: { etiqueta: "Dx relacionado 2", tipo: "text" },
            dx_rel_3: { etiqueta: "Dx relacionado 3", tipo: "text" }
        }},
        egreso: { campos: {
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
        }}
    });
});

app.get('/api/v1/fhir/patients', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const size = parseInt(req.query.size) || 10;
        const offset = (page - 1) * size;
        const countRes = await globalPool.query('SELECT COUNT(*) FROM pacientes');
        const total = parseInt(countRes.rows[0].count);
        const res2 = await globalPool.query(`
            SELECT p.paciente_id, p.nombre, p.apellido, p.fecha_nacimiento, p.ciudad_registro_origen, p.registrado_por,
                   m.nombre as medico_nombre, m.apellido as medico_apellido, m.especialidad
            FROM pacientes p 
            LEFT JOIN medicos m ON p.registrado_por = m.medico_id
            ORDER BY p.paciente_id DESC LIMIT $1 OFFSET $2`, [size, offset]);
        const fhirPatients = res2.rows.map(p => ({
            id: p.paciente_id,
            identifier: [{ value: p.paciente_id }],
            name: [{ text: `${p.nombre} ${p.apellido}` }],
            gender: 'unknown',
            birthDate: p.fecha_nacimiento ? p.fecha_nacimiento.toISOString().split('T')[0] : 'N/A',
            address: [{ city: p.ciudad_registro_origen || 'Global' }],
            registrado_por: p.registrado_por ? {
                id: p.registrado_por,
                nombre: `${p.medico_nombre || ''} ${p.medico_apellido || ''}`.trim(),
                especialidad: p.especialidad
            } : null
        }));
        res.json({ patients: fhirPatients, total, total_pages: Math.ceil(total / size) || 1 });
    } catch (err) {
        res.status(500).json({ detail: err.message });
    }
});

app.get('/login-medico', (req, res) => res.sendFile(path.join(__dirname, 'login-medico.html')));
app.get('/registro-paciente', (req, res) => res.sendFile(path.join(__dirname, 'registro-paciente.html')));
app.get('/triage', (req, res) => res.sendFile(path.join(__dirname, 'triage.html')));
app.get('/paciente/:id', (req, res) => res.sendFile(path.join(__dirname, 'firh.html')));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'registro-paciente.html')));

const server = app.listen(PORT, () => {
    console.log(`🚀 Gateway de registro escuchando en puerto ${PORT}`);
}).on('error', (err) => {
    if (err.code === 'EADDRINUSE') {
        console.log(`⚠️ Puerto ${PORT} ya está en uso. Usando puerto alternativo ${PORT + 1000}...`);
        app.listen(PORT + 1000, () => console.log(`🚀 Gateway escuchando en puerto alternativo ${PORT + 1000}`));
    } else {
        console.error('Error al iniciar gateway:', err);
    }
});