const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const { Pool } = require('pg');

const app = express();
app.use(cors());
app.use(express.json());
app.use('/static', express.static(path.join(__dirname, 'static')));

const SEDE_ACTUAL = process.env.SEDE_ACTUAL || 'Sincelejo';
const PENDING_FILE = path.join(process.cwd(), 'pendientes.json');
const SYNC_QUEUE_FILE = path.join(process.cwd(), 'sync_queue.json');

const pool = new Pool({
    user: 'admin_clinica',
    database: 'historia_clinica_global',
    password: 'password_seguro',
    port: 5432,
    host: process.env.DB_HOST || 'localhost'
});

const globalPool = new Pool({
    user: 'admin_clinica',
    database: 'historia_clinica_global',
    password: 'password_seguro',
    port: 5432,
    host: process.env.GLOBAL_DB_HOST || 'db_global'
});

const SEDE_HOSTS = { Sincelejo: 'app_sincelejo', Bogota: 'app_bogota', Medellin: 'app_medellin' };

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
                console.log(`[SINCELEJO] Sincronizado ${item.tipo} a ${item.sede}`);
            } else {
                throw new Error(`HTTP ${response.status}`);
            }
        } catch (err) {
            item.intentos = (item.intentos || 0) + 1;
            if (item.intentos < 30) pendientes.push(item);
            else console.error(`[SINCELEJO] Falló sincronización a ${item.sede} después de ${item.intentos} intentos`);
        }
    }
    fs.writeFileSync(SYNC_QUEUE_FILE, JSON.stringify(pendientes, null, 2));
}

setInterval(procesarCola, 5000);

app.get('/api/firh/campos', (req, res) => {
    res.json({
        identificacion_usuario: { campos: {
            tipo_documento: { etiqueta: "Tipo de documento", tipo: "select", requerido: true, opciones: ["CC", "TI", "CE", "PA", "RC", "MS", "AS"] },
            numero_documento: { etiqueta: "Número de documento", tipo: "text", requerido: true },
            pais_nacionalidad: { etiqueta: "País de nacionalidad", tipo: "text", requerido: true },
            nombre_completo: { etiqueta: "Nombre completo", tipo: "text", requerido: true },
            fecha_nacimiento: { etiqueta: "Fecha nacimiento", tipo: "date", requerido: true },
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

app.post('/api/firh/cargar', async (req, res) => {
    res.json({ ok: true, mensaje: 'FIRH guardado (modo demo)' });
});

app.get('/api/paciente/:id', async (req, res) => {
    const pacienteId = decodeURIComponent(req.params.id).trim();
    try {
        let result = await pool.query('SELECT * FROM pacientes WHERE paciente_id = $1', [pacienteId]);
        if (!result.rows[0]) {
            result = await globalPool.query('SELECT * FROM pacientes WHERE paciente_id = $1', [pacienteId]);
        }
        if (!result.rows[0]) {
            return res.status(404).json({ detail: `Paciente "${pacienteId}" no encontrado` });
        }
        const row = result.rows[0];
        
        // Obtener triage records
        let triageResult = await pool.query("SELECT * FROM triage_records WHERE paciente_id = $1 ORDER BY fecha_registro DESC LIMIT 50", [pacienteId]);
        if (!triageResult.rows[0]) {
            triageResult = await globalPool.query("SELECT * FROM triage_records WHERE paciente_id = $1 ORDER BY fecha_registro DESC LIMIT 50", [pacienteId]);
        }
        
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
        
        const firhData = row.firh || {};
        const historias = firhData ? [{
            version: 1,
            medico_identification: 'N/A',
            sede_actualizacion: row.ciudad_registro_origen || 'Global',
            fecha_actualizacion: row.fecha_creacion || new Date().toISOString(),
            diagnostico: firhData?.diagnosticos?.diagnostico_egreso || 'Sin diagnóstico'
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
        console.error('[SINCELEJO] Error en /api/paciente/:id:', err.message);
        res.status(500).json({ detail: err.message });
    }
});

app.post('/api/registro/paciente', async (req, res) => {
    const datos = req.body || {};
    const { paciente_id, sede_destino } = datos;
    if (!paciente_id) return res.status(400).json({ ok: false, detail: 'paciente_id es obligatorio' });
    try {
        await pool.query(
            `INSERT INTO pacientes (paciente_id, nombre, apellido, tipo_documento, fecha_nacimiento, ciudad_registro_origen, firh) 
             VALUES ($1, $2, $3, $4, $5, $6, $7) 
             ON CONFLICT (paciente_id) DO UPDATE 
             SET nombre = EXCLUDED.nombre, apellido = EXCLUDED.apellido, 
                 tipo_documento = EXCLUDED.tipo_documento,
                 fecha_nacimiento = EXCLUDED.fecha_nacimiento`,
            [
                paciente_id,
                datos.nombre || 'Sin nombre',
                datos.apellido || 'Sin apellido',
                datos.tipo_documento || 'CC',
                datos.fecha_nacimiento || new Date().toISOString().split('T')[0],
                sede_destino || SEDE_ACTUAL,
                JSON.stringify(datos.firh || {})
            ]
        );
        // Replicar a otras sedes
        for (const sede of Object.keys(SEDE_HOSTS)) {
            if (sede === SEDE_ACTUAL) continue;
            try {
                const response = await fetch(`http://${SEDE_HOSTS[sede]}:3000/api/registro/paciente`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ ...datos, esReplicado: true })
                });
                if (!response.ok) throw new Error(`HTTP ${response.status}`);
            } catch (e) {
                guardarEnCola(sede, { ...datos, esReplicado: true }, 'registro');
            }
        }
        // Guardar también en global como backup
        try {
            await globalPool.query(
                `INSERT INTO pacientes (paciente_id, nombre, apellido, tipo_documento, fecha_nacimiento, ciudad_registro_origen, firh) 
                 VALUES ($1, $2, $3, $4, $5, $6, $7) 
                 ON CONFLICT (paciente_id) DO UPDATE 
                 SET nombre = EXCLUDED.nombre, apellido = EXCLUDED.apellido, 
                     tipo_documento = EXCLUDED.tipo_documento,
                     fecha_nacimiento = EXCLUDED.fecha_nacimiento, 
                     firh = EXCLUDED.firh`,
                [paciente_id, datos.nombre || 'Sin nombre', datos.apellido || 'Sin apellido', datos.tipo_documento || 'CC', datos.fecha_nacimiento || new Date().toISOString().split('T')[0], SEDE_ACTUAL, JSON.stringify(datos.firh || {})]
            );
        } catch (e) {}
        res.json({ ok: true, mensaje: `Paciente guardado en nodo ${SEDE_ACTUAL}` });
    } catch (err) {
        guardarPendiente({ tipo: 'REGISTRO_PACIENTE', ...req.body });
        res.status(202).json({ ok: true, mensaje: 'Guardado offline para sincronizar después', offline: true });
    }
});

app.post('/api/triage', async (req, res) => {
    const { paciente_id, presion_arterial, frecuencia_cardiaca, temperatura, saturacion_oxigeno, nivel_triage, motivo_consulta, esReplicado } = req.body;
    if (!paciente_id) return res.status(400).json({ ok: false, detail: 'paciente_id es obligatorio' });
    if (!nivel_triage) return res.status(400).json({ ok: false, detail: 'nivel_triage es obligatorio' });
    try {
        // Verificar duplicado reciente
        const recentCheck = await pool.query(
            "SELECT COUNT(*) FROM triage_records WHERE paciente_id = $1 AND fecha_registro > NOW() - INTERVAL '5 seconds'", 
            [paciente_id]
        );
        if (parseInt(recentCheck.rows[0].count) > 0 && !esReplicado) {
            return res.json({ ok: true, mensaje: 'Triage ya registrado recientemente (duplicado evitado)' });
        }
        
        await pool.query(
            `INSERT INTO triage_records (paciente_id, presion_arterial, frecuencia_cardiaca, temperatura, saturacion_oxigeno, nivel_triage, motivo_consulta, sede) 
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
            [paciente_id, presion_arterial, parseInt(frecuencia_cardiaca) || null, parseFloat(temperatura) || null, parseInt(saturacion_oxigeno) || null, parseInt(nivel_triage), motivo_consulta, SEDE_ACTUAL]
        );
        
        // Replicar a otras sedes, encolar si falla
        for (const sede of Object.keys(SEDE_HOSTS)) {
            if (sede === SEDE_ACTUAL) continue;
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
        
        // Guardar también en global
        try {
            await globalPool.query(
                `INSERT INTO triage_records (paciente_id, presion_arterial, frecuencia_cardiaca, temperatura, saturacion_oxigeno, nivel_triage, motivo_consulta, sede) 
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
                [paciente_id, presion_arterial, parseInt(frecuencia_cardiaca) || null, parseFloat(temperatura) || null, parseInt(saturacion_oxigeno) || null, parseInt(nivel_triage), motivo_consulta, SEDE_ACTUAL]
            );
        } catch (e) {}
        
        res.json({ ok: true, mensaje: `Triage guardado en nodo ${SEDE_ACTUAL}` });
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
        let result = await pool.query(
            "SELECT * FROM triage_records WHERE paciente_id = $1 ORDER BY fecha_registro DESC LIMIT $2 OFFSET $3", 
            [pacienteId, limit, offset]
        );
        if (!result.rows[0]) {
            result = await globalPool.query(
                "SELECT * FROM triage_records WHERE paciente_id = $1 ORDER BY fecha_registro DESC LIMIT $2 OFFSET $3", 
                [pacienteId, limit, offset]
            );
        }
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
        let countResult = await pool.query('SELECT COUNT(*) FROM triage_records WHERE paciente_id = $1', [pacienteId]);
        let total = parseInt(countResult.rows[0].count);
        if (total === 0) {
            countResult = await globalPool.query('SELECT COUNT(*) FROM triage_records WHERE paciente_id = $1', [pacienteId]);
            total = parseInt(countResult.rows[0].count);
        }
        res.json({ triages, pagination: { total, limit, offset } });
    } catch (err) {
        res.status(500).json({ detail: err.message });
    }
});

app.get('/api/v1/fhir/patients', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const size = parseInt(req.query.size) || 10;
        const offset = (page - 1) * size;
        let countRes = await pool.query('SELECT COUNT(*) FROM pacientes');
        let total = parseInt(countRes.rows[0].count);
        if (total === 0) {
            countRes = await globalPool.query('SELECT COUNT(*) FROM pacientes');
            total = parseInt(countRes.rows[0].count);
        }
        let res2 = await pool.query(`
            SELECT p.paciente_id, p.nombre, p.apellido, p.fecha_nacimiento, p.ciudad_registro_origen, p.registrado_por
            FROM pacientes p 
            ORDER BY p.paciente_id DESC LIMIT $1 OFFSET $2`, [size, offset]);
        if (!res2.rows[0]) {
            res2 = await globalPool.query(`
                SELECT p.paciente_id, p.nombre, p.apellido, p.fecha_nacimiento, p.ciudad_registro_origen, p.registrado_por
                FROM pacientes p 
                ORDER BY p.paciente_id DESC LIMIT $1 OFFSET $2`, [size, offset]);
        }
        const fhirPatients = res2.rows.map(p => ({
            id: p.paciente_id,
            identifier: [{ value: p.paciente_id }],
            name: [{ text: `${p.nombre} ${p.apellido}` }],
            gender: 'unknown',
            birthDate: p.fecha_nacimiento ? p.fecha_nacimiento.toISOString().split('T')[0] : 'N/A',
            address: [{ city: p.ciudad_registro_origen || SEDE_ACTUAL }]
        }));
        res.json({ patients: fhirPatients, total, total_pages: Math.ceil(total / size) || 1 });
    } catch (err) {
        res.status(500).json({ detail: err.message });
    }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Servidor ${SEDE_ACTUAL} en puerto ${PORT}`));