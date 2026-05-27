const express = require('express');
const cors = require('cors');
const { Kafka } = require('kafkajs');
const path = require('path');
const fs = require('fs');
const { Pool } = require('pg');

const app = express();
app.use(cors());
app.use(express.json());
app.use('/static', express.static('static'));

const JWT_SECRET = process.env.JWT_SECRET || 'clave_secreta_smart_on_fhir_2026';
const PENDING_FILE = path.join(process.cwd(), 'pendientes_registro.json');
const SYNC_QUEUE_FILE = path.join(process.cwd(), 'sync_queue_registro.json');

const globalPool = new Pool({
    user: 'admin_clinica',
    database: 'historia_clinica_global',
    password: 'password_seguro',
    port: 5432,
    host: process.env.GLOBAL_DB_HOST || 'db_global'
});

const SEDE_HOSTS = { Sincelejo: 'app_sincelejo', Bogota: 'app_bogota', Medellin: 'app_medellin' };
const SEDE_PORTS = { Sincelejo: 3001, Bogota: 3002, Medellin: 3003 };

async function replicarATodasSedes(datos) {
    for (const sede of Object.keys(SEDE_HOSTS)) {
        try {
            const response = await fetch(`http://${SEDE_HOSTS[sede]}:3000/api/registro/paciente`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(datos)
            });
            console.log(`[REGISTRO] Replicado a ${sede}`, response.ok ? 'OK' : `ERROR ${response.status}`);
        } catch (err) {
            console.error(`[REGISTRO] No se pudo replicar a ${sede}:`, err.message);
            guardarEnCola(sede, datos, 'registro');
        }
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

let producer = null;
try {
    const kafka = new Kafka({ clientId: 'registro-service', brokers: [process.env.KAFKA_BROKER || 'kafka:9092'] });
    producer = kafka.producer();
} catch (e) { console.warn('[REGISTRO] Kafka no disponible'); }

async function initKafka() {
    try { if (producer) { await producer.connect(); console.log('✅ Registro conectado a Kafka'); } }
    catch (err) { console.warn('[REGISTRO] Kafka no disponible:', err.message); }
}
initKafka();

// Sincronización pendiente para nodos caídos
function guardarEnCola(sedeDestino, datos, tipo) {
    let cola = [];
    if (fs.existsSync(SYNC_QUEUE_FILE)) {
        try { cola = JSON.parse(fs.readFileSync(SYNC_QUEUE_FILE, 'utf8')); } catch {}
    }
    cola.push({ sede: sedeDestino, tipo, datos, timestamp: Date.now(), intentos: 0 });
    fs.writeFileSync(SYNC_QUEUE_FILE, JSON.stringify(cola, null, 2));
}

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
                console.log(`[REGISTRO] Sincronizado ${item.tipo} a ${item.sede}`);
            } else {
                throw new Error(`HTTP ${response.status}`);
            }
        } catch (err) {
            item.intentos = (item.intentos || 0) + 1;
            if (item.intentos < 30) pendientes.push(item);
            else console.error(`[REGISTRO] Falló sincronización a ${item.sede} después de ${item.intentos} intentos`);
        }
    }
    fs.writeFileSync(SYNC_QUEUE_FILE, JSON.stringify(pendientes, null, 2));
}

// Procesar cola cada 5 segundos
setInterval(procesarCola, 5000);

app.post('/api/v1/auth/token', (req, res) => {
    const header = { alg: 'HS256', typ: 'JWT' };
    const payload = { iss: 'auth_registro', sede: 'Global', role: 'professional', exp: Math.floor(Date.now() / 1000) + 3600 };
    const encode = (obj) => Buffer.from(JSON.stringify(obj)).toString('base64url');
    const token = `${encode(JSON.stringify(header))}.${encode(JSON.stringify(payload))}.direct`;
    res.json({ access_token: token, token_type: 'Bearer', expires_in: 3600 });
});

app.post('/api/registro/paciente', async (req, res) => {
    const datos = req.body || {};
    const { paciente_id, sede_destino } = datos;
    if (!paciente_id) {
        return res.status(400).json({ ok: false, detail: 'paciente_id es obligatorio' });
    }
    const evento = {
        event_id: `REGISTRO-${paciente_id}-${Date.now()}`,
        tipo: 'PACIENTE_REGISTRO_SOLICITADO',
        sede_origen: 'REGISTRO_SERVICE',
        sede_destino: sede_destino || 'Sincelejo',
        medico_id: datos.medico_id,
        timestamp: new Date().toISOString(),
        data: datos
    };
    try {
        await globalPool.query(
            `INSERT INTO pacientes (paciente_id, nombre, apellido, tipo_documento, fecha_nacimiento, ciudad_registro_origen, firh) 
             VALUES ($1, $2, $3, $4, $5, $6, $7) 
             ON CONFLICT (paciente_id) DO UPDATE 
             SET nombre = EXCLUDED.nombre, apellido = EXCLUDED.apellido, 
                 tipo_documento = EXCLUDED.tipo_documento,
                 fecha_nacimiento = EXCLUDED.fecha_nacimiento, 
                 ciudad_registro_origen = EXCLUDED.ciudad_registro_origen,
                 firh = EXCLUDED.firh`,
            [
                paciente_id,
                datos.nombre || 'Sin nombre',
                datos.apellido || 'Sin apellido',
                datos.tipo_documento || 'CC',
                datos.fecha_nacimiento || new Date().toISOString().split('T')[0],
                sede_destino || 'Sincelejo',
                JSON.stringify(datos.firh || {})
            ]
        );
        if (producer) {
            await producer.send({ topic: 'eventos-clinicos', messages: [{ key: String(paciente_id), value: JSON.stringify(evento) }] });
        }
        // Replicar a TODAS las sedes
        replicarATodasSedes(datos).catch(() => {});
        return res.status(201).json({ ok: true, mensaje: 'Paciente guardado en BD global y todas las sedes', sede_destino });
    } catch (err) {
        guardarPendiente(evento);
        return res.status(202).json({ ok: true, mensaje: 'Guardado para sincronización offline', offline: true });
    }
});

app.get('/api/firh/campos', (req, res) => {
    res.json({
        identificacion_usuario: { campos: {
            tipo_documento: { etiqueta: "Tipo de documento", tipo: "select", requerido: true, opciones: ["CC", "TI", "CE", "PA", "RC", "MS", "AS"] },
            numero_documento: { etiqueta: "Número de documento", tipo: "text", requerido: true },
            pais_nacionalidad: { etiqueta: "País de nacionalidad", tipo: "text", requerido: true },
            nombre_completo: { etiqueta: "Nombre completo", tipo: "text", requerido: true },
            fecha_nacimiento: { etiqueta: "Fecha de nacimiento", tipo: "date", requerido: true },
            edad: { etiqueta: "Edad", tipo: "number", requerido: true },
            unidad_edad: { etiqueta: "Unidad de edad", tipo: "select", requerido: true, opciones: ["Años", "Meses", "Días"] },
            sexo: { etiqueta: "Sexo", tipo: "select", requerido: true, opciones: ["M", "F", "I"] },
            genero: { etiqueta: "Género", tipo: "text", requerido: true },
            ocupacion: { etiqueta: "Ocupación", tipo: "text", requerido: true },
            voluntad_anticipada: { etiqueta: "Voluntad anticipada", tipo: "text", requerido: true },
            categoria_discapacidad: { etiqueta: "Categoría discapacidad", tipo: "text", requerido: true },
            pais_residencia: { etiqueta: "País residencia", tipo: "text", requerido: true },
            municipio_residencia: { etiqueta: "Municipio residencia", tipo: "text", requerido: true },
            etnia: { etiqueta: "Etnia", tipo: "text", requerido: true }
        }},
        atencion: { campos: {
            entidad_salud: { etiqueta: "Entidad de salud", tipo: "text", requerido: true },
            fecha_ingreso: { etiqueta: "Fecha ingreso", tipo: "datetime", requerido: true },
            modalidad_servicio: { etiqueta: "Modalidad servicio", tipo: "select", requerido: true, opciones: ["Intramural", "Extramural", "Telemedicina"] },
            entorno_atencion: { etiqueta: "Entorno atención", tipo: "select", requerido: true, opciones: ["Urgencias", "Consulta Externa", "Hospitalización"] },
            via_ingreso: { etiqueta: "Vía ingreso", tipo: "select", requerido: true, opciones: ["Espontánea", "Remitido", "Contraremitido"] },
            causa_atencion: { etiqueta: "Causa atención", tipo: "textarea", requerido: true },
            fecha_triaje: { etiqueta: "Fecha triaje", tipo: "datetime", requerido: true },
            clasificacion_triaje: { etiqueta: "Clasificación triage", tipo: "select", requerido: true, opciones: ["I", "II", "III", "IV", "V"] },
            comunidad_etnica: { etiqueta: "Comunidad étnica", tipo: "text", requerido: true }
        }},
        tecnologias_salud: { campos: {
            descripcion_medicamento: { etiqueta: "Medicamento", tipo: "text", requerido: true },
            dosis: { etiqueta: "Dosis", tipo: "text", requerido: true },
            via_administracion: { etiqueta: "Vía administración", tipo: "select", requerido: true, opciones: ["Oral", "IV", "IM", "SC", "Tópica"] },
            frecuencia: { etiqueta: "Frecuencia", tipo: "text", requerido: true },
            dias_tratamiento: { etiqueta: "Días tratamiento", tipo: "number", requerido: true },
            unidades_aplicadas: { etiqueta: "Unidades aplicadas", tipo: "number", requerido: true },
            identificacion_personal_salud: { etiqueta: "ID profesional salud", tipo: "text", requerido: true },
            finalidad_tecnologia: { etiqueta: "Finalidad", tipo: "select", requerido: true, opciones: ["Terapéutica", "Diagnóstica", "Paliativa", "Preventiva", "Rehabilitación"] },
            tipo_diagnostico_ingreso: { etiqueta: "Tipo diagnóstico ingreso", tipo: "select", requerido: true, opciones: ["Principal", "Relacionado", "Complicación"] },
            diagnostico_ingreso: { etiqueta: "Diagnóstico ingreso", tipo: "text", requerido: true },
            tipo_diagnostico_egreso: { etiqueta: "Tipo diagnóstico egreso", tipo: "select", requerido: true, opciones: ["Principal", "Relacionado"] }
        }},
        diagnosticos: { campos: {
            diagnostico_egreso: { etiqueta: "Diagnóstico egreso", tipo: "text", requerido: true },
            dx_rel_1: { etiqueta: "Dx relacionado 1", tipo: "text", requerido: true },
            dx_rel_2: { etiqueta: "Dx relacionado 2", tipo: "text", requerido: true },
            dx_rel_3: { etiqueta: "Dx relacionado 3", tipo: "text", requerido: true }
        }},
        egreso: { campos: {
            fecha_egreso: { etiqueta: "Fecha egreso", tipo: "datetime", requerido: true },
            condicion_salida: { etiqueta: "Condición salida", tipo: "select", requerido: true, opciones: ["Vivo", "Muerto"] },
            diagnostico_muerte: { etiqueta: "Diagnóstico muerte", tipo: "text", requerido: true },
            codigo_prestador: { etiqueta: "Código prestador", tipo: "text", requerido: true },
            tipo_incapacidad: { etiqueta: "Tipo incapacidad", tipo: "select", requerido: true, opciones: ["Temporal", "Permanente Parcial", "Permanente Total"] },
            dias_incapacidad: { etiqueta: "Días incapacidad", tipo: "number", requerido: true },
            dias_licencia_maternidad: { etiqueta: "Días licencia maternidad", tipo: "number", requerido: true },
            alergias: { etiqueta: "Alergias", tipo: "text", requerido: true },
            antecedentes_familiares: { etiqueta: "Antecedentes familiares", tipo: "text", requerido: true },
            riesgos_ocupacionales: { etiqueta: "Riesgos ocupacionales", tipo: "text", requerido: true },
            responsable_egreso: { etiqueta: "Responsable egreso", tipo: "text", requerido: true },
            zona_residencia: { etiqueta: "Zona residencia", tipo: "select", requerido: true, opciones: ["Urbana", "Rural"] },
            direccion_residencia: { etiqueta: "Dirección", tipo: "text", requerido: true },
            telefono: { etiqueta: "Teléfono", tipo: "text", requerido: true },
            correo_electronico: { etiqueta: "Correo", tipo: "email", requerido: true },
            nombre_responsable: { etiqueta: "Nombre responsable", tipo: "text", requerido: true },
            parentesco_responsable: { etiqueta: "Parentesco", tipo: "select", requerido: true, opciones: ["Padre/Madre", "Hijo", "Esposo", "Otro"] },
            telefono_responsable: { etiqueta: "Teléfono responsable", tipo: "text", requerido: true }
        }}
    });
});

app.post('/api/firh/cargar', async (req, res) => {
    const { tabla, datos } = req.body;
    if (!datos?.paciente_id) return res.status(400).json({ ok: false, detail: 'paciente_id es obligatorio' });
    const pacienteId = datos.paciente_id;
    const firhCompleto = {
        identificacion_usuario: datos.identificacion_usuario || {},
        atencion: datos.atencion || {},
        tecnologias_salud: datos.tecnologias_salud || {},
        diagnosticos: datos.diagnosticos || {},
        egreso: datos.egreso || {}
    };
    try {
        await globalPool.query('UPDATE pacientes SET firh = $1 WHERE paciente_id = $2', [JSON.stringify(firhCompleto), pacienteId]);
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
        const result = await globalPool.query('SELECT firh FROM pacientes WHERE paciente_id = $1', [pacienteId]);
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
        
        // Obtener triage records
        const triageResult = await globalPool.query("SELECT * FROM triage_records WHERE paciente_id = $1 ORDER BY fecha_registro DESC", [pacienteId]);
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
        
        // Obtener historias clínicas
        const historias = row.firh ? [{
            version: 1,
            medico_identification: 'N/A',
            sede_actualizacion: row.ciudad_registro_origen || 'Global',
            fecha_actualizacion: row.fecha_creacion || new Date().toISOString(),
            diagnostico: row.firh?.diagnosticos?.diagnostico_egreso || 'Sin diagnóstico'
        }] : [];
        
        res.json({ 
            paciente: {
                paciente_id: row.paciente_id,
                nombre: row.nombre,
                apellido: row.apellido,
                fecha_nacimiento: row.fecha_nacimiento,
                edad: row.edad,
                tipo_documento: row.tipo_documento || 'CC'
            },
            firh_guardado: row.firh || {},
            triage,
            historias
        });
    } catch (err) {
        console.error('[REGISTRO] Error en /api/paciente/:id:', err.message);
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
             [paciente_id, presion_arterial, parseInt(frecuencia_cardiaca) || null, parseFloat(temperatura) || null, parseInt(saturacion_oxigeno) || null, parseInt(nivel_triage), motivo_consulta, 'REGISTRO_SERVICE']
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

app.get('/api/triage/historias', async (req, res) => {
    const pacienteId = req.query.paciente_id;
    if (!pacienteId) return res.status(400).json({ detail: 'paciente_id es obligatorio' });
    const limit = parseInt(req.query.limit) || 50;
    const offset = parseInt(req.query.offset) || 0;
    try {
        const result = await globalPool.query(
            "SELECT * FROM triage_records WHERE paciente_id = $1 ORDER BY fecha_registro DESC LIMIT $2 OFFSET $3",
            [pacienteId, limit, offset]
        );
        const triages = result.rows.map(r => ({
            id: r.triage_id,
            paciente_id: r.paciente_id,
            presion_arterial: r.presion_arterial,
            frecuencia_cardiaca: r.frecuencia_cardiaca,
            temperatura: r.temperatura,
            saturacion_oxigeno: r.saturacion_oxigeno,
            nivel_triage: r.nivel_triage,
            motivo_consulta: r.motivo_consulta,
            sede_actualizacion: r.sede,
            fecha_actualizacion: r.fecha_registro
        }));
        const countRes = await globalPool.query('SELECT COUNT(*) FROM triage_records WHERE paciente_id = $1', [pacienteId]);
        res.json({ triages, pagination: { total: parseInt(countRes.rows[0].count), limit, offset } });
    } catch (err) {
        res.status(500).json({ detail: err.message });
    }
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

app.get('/registro-paciente', (req, res) => res.sendFile(path.join(__dirname, 'registro-paciente.html')));
app.get('/firh', (req, res) => res.sendFile(path.join(__dirname, 'firh.html')));
app.get('/paciente/:id', (req, res) => res.sendFile(path.join(__dirname, 'firh.html')));
app.get('/triage', (req, res) => res.sendFile(path.join(__dirname, 'triage.html')));
app.get('/reportes', (req, res) => res.sendFile(path.join(__dirname, 'reportes.html')));
app.get('/monitor-nodos', (req, res) => res.sendFile(path.join(__dirname, 'monitor-nodos.html')));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Registro service escuchando en puerto ${PORT}`));