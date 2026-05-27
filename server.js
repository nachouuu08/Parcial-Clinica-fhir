const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const path = require('path');
const fs = require('fs');

const app = express();
app.use(cors());
app.use(express.json());
app.use('/static', express.static(path.join(__dirname, 'static')));

const SEDE_ACTUAL = process.env.SEDE_ACTUAL || 'Sincelejo';
const JWT_SECRET = process.env.JWT_SECRET || 'clave_secreta_smart_on_fhir_2026';
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
        const host = SEDE_HOSTS_REPL[item.sede];
        if (!host) continue;
        try {
            const endpoint = item.tipo === 'triage' ? '/api/triage' : '/api/firh/cargar';
            const response = await fetch(`http://${host}:3000${endpoint}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(item.tipo === 'triage' ? item.datos : { tabla: 'completo', datos: item.datos })
            });
            if (response.ok) console.log(`[SYNC] Aplicado a ${item.sede}`);
            else throw new Error(`HTTP ${response.status}`);
        } catch (err) {
            item.intentos = (item.intentos || 0) + 1;
            if (item.intentos < 30) pendientes.push(item);
            else console.error(`[SYNC] Falló ${item.tipo} en ${item.sede} tras ${item.intentos} intentos`);
        }
    }
    fs.writeFileSync(SYNC_QUEUE_FILE, JSON.stringify(pendientes, null, 2));
}

const SEDE_HOSTS_REPL = { Sincelejo: 'app_sincelejo', Bogota: 'app_bogota', Medellin: 'app_medellin', Registro: 'gateway_registro', FIRH: 'gateway_firh', Triage: 'gateway_triage' };
setInterval(procesarCola, 5000);

async function replicarFhirATodasSedes(fhirData) {
    const sedeHosts = { Sincelejo: 'app_sincelejo', Bogota: 'app_bogota', Medellin: 'app_medellin', Registro: 'gateway_registro', FIRH: 'gateway_firh', Triage: 'gateway_triage' };
    for (const [sede, host] of Object.entries(sedeHosts)) {
        if (sede === SEDE_ACTUAL) {
            continue;
        }
        try {
            const response = await fetch(`http://${host}:3000/api/firh/cargar`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    tabla: 'completo',
                    datos: fhirData
                })
            });
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            console.log(`[FIRH] Replicado a ${sede}`);
        } catch (err) {
            guardarEnCola(sede, fhirData, 'firh');
            console.warn(`[FIRH] No se pudo replicar a ${sede}:`, err.message);
        }
    }
}

function normalizarFirh(firh) {
    if (!firh) return {};
    if (Array.isArray(firh)) return firh.length > 0 && typeof firh[0] === 'object' ? firh[0] : {};
    if (typeof firh === 'string') { try { return normalizarFirh(JSON.parse(firh)); } catch { return {}; } }
    return typeof firh === 'object' ? firh : {};
}

const REGLAS_FIRH = {
    identificacion_usuario: ["tipo_documento", "numero_documento", "pais_nacionalidad", "nombre_completo", "fecha_nacimiento", "edad", "sexo"],
    atencion: ["entidad_salud", "fecha_ingreso", "modalidad_servicio", "entorno_atencion", "via_ingreso", "causa_atencion"],
    diagnosticos: ["diagnostico_egreso"],
    egreso: ["fecha_egreso", "condicion_salida", "codigo_prestador"]
};

function validarCampos(obligatorios, datos) {
    const errores = [];
    for (const campo of obligatorios) {
        if (datos[campo] === undefined || datos[campo] === null || datos[campo] === '') errores.push(`Campo obligatorio faltante: ${campo}`);
    }
    return errores;
}

function generarToken() {
    const header = { alg: "HS256", typ: "JWT" };
    const payload = { iss: "auth_central", aud: "api_clinicas", sede: SEDE_ACTUAL, role: "professional", exp: Math.floor(Date.now() / 1000) + 3600 };
    const headerB64 = Buffer.from(JSON.stringify(header)).toString('base64url');
    const payloadB64 = Buffer.from(JSON.stringify(payload)).toString('base64url');
    const firmaB64 = Buffer.from('dummy').toString('base64url');
    return `${headerB64}.${payloadB64}.${firmaB64}`;
}

app.post('/auth/token', (req, res) => {
    res.json({ access_token: generarToken(), token_type: "Bearer", expires_in: 3600, scope: 'patient/*.read patient/*.write' });
});

app.post('/api/v1/auth/token', (req, res) => {
    res.json({ access_token: generarToken(), token_type: "Bearer", expires_in: 3600, scope: 'patient/*.read patient/*.write' });
});

const PORT = process.env.PORT || 3000;

// Migración para agregar columna registrado_por si no existe
async function migrarBD() {
    try {
        await pool.query('ALTER TABLE pacientes ADD COLUMN IF NOT EXISTS registrado_por VARCHAR(50)');
        await globalPool.query('ALTER TABLE pacientes ADD COLUMN IF NOT EXISTS registrado_por VARCHAR(50)');
    } catch (e) {}
}
migrarBD();

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
    const esReplicado = datos?.esReplicado || false;
    const reglas = REGLAS_FIRH[tabla] || [];
    const errores = validarCampos(reglas, datos);
    if (errores.length > 0) return res.status(400).json({ ok: false, tabla, errores });

    const pacienteId = datos.paciente_id;
    const firhCompleto = {
        identificacion_usuario: datos.identificacion_usuario || {},
        atencion: datos.atencion || {},
        tecnologias_salud: datos.tecnologias_salud || {},
        diagnosticos: datos.diagnosticos || {},
        egreso: datos.egreso || {}
    };

    try {
        // Get the next version number for this patient
        const versionResult = await pool.query(
            "SELECT COALESCE(MAX(version), 0) + 1 as next_version FROM historias_clinicas WHERE paciente_id = $1",
            [pacienteId]
        );
        const nextVersion = versionResult.rows[0].next_version;

        // Insert new clinical history
        await pool.query(
            `INSERT INTO historias_clinicas (paciente_id, version, medico_identification, sede_actualizacion, diagnostico, tratamiento) 
             VALUES ($1, $2, $3, $4, $5, $6)`,
            [
                pacienteId,
                nextVersion,
                'N/A', // medico_identification - could be extracted from auth context
                SEDE_ACTUAL,
                firhCompleto.diagnosticos?.diagnostico_egreso || 'Sin diagnóstico',
                JSON.stringify(firhCompleto) // Store full FIRH as treatment for now
            ]
        );

        // Also update the pacientes table with latest FIRH for backward compatibility
        await pool.query(
            'UPDATE pacientes SET firh = $1 WHERE paciente_id = $2',
            [JSON.stringify(firhCompleto), pacienteId]
        );
        
        // Solo replicar si no es una replicación (evitar bucles de replicación)
        if (!esReplicado) {
            // Replicar a todas las sedes
            await replicarFhirATodasSedes({ 
                paciente_id: pacienteId, 
                ...firhCompleto,
                esReplicado: true
            });
            // Replicar también a BD global como backup
            try {
                await globalPool.query(
                    `INSERT INTO pacientes (paciente_id, nombre, apellido, fecha_nacimiento, ciudad_registro_origen, firh) 
                     VALUES ($1, $2, $3, $4, $5, $6) 
                     ON CONFLICT (paciente_id) DO UPDATE 
                     SET nombre = EXCLUDED.nombre, apellido = EXCLUDED.apellido, 
                         fecha_nacimiento = EXCLUDED.fecha_nacimiento, 
                         firh = EXCLUDED.firh`,
                    [
                        pacienteId,
                        datos.identificacion_usuario?.nombre_completo || 'Sin nombre',
                        datos.identificacion_usuario?.apellido || '',
                        datos.identificacion_usuario?.fecha_nacimiento || new Date().toISOString().split('T')[0],
                        SEDE_ACTUAL,
                        JSON.stringify(firhCompleto)
                    ]
                );
            } catch (e) {
                console.warn('[FIRH] No se pudo replicar a BD global:', e.message);
            }
        }
        res.json({ ok: true, mensaje: 'FIRH guardado', firh: firhCompleto, version: nextVersion });
    } catch (err) {
        // Solo intentar replicar si no es una replicación (evitar bucles de replicación)
        if (!esReplicado) {
            // Replicar a todas las sedes incluso si falla localmente
            await replicarFhirATodasSedes({ paciente_id: pacienteId, ...firhCompleto, esReplicado: true });
            // Intentar guardar en BD global como último recurso
            try {
                await globalPool.query(
                    `INSERT INTO pacientes (paciente_id, nombre, apellido, fecha_nacimiento, ciudad_registro_origen, firh) 
                     VALUES ($1, $2, $3, $4, $5, $6) 
                     ON CONFLICT (paciente_id) DO UPDATE 
                     SET nombre = EXCLUDED.nombre, apellido = EXCLUDED.apellido, 
                         fecha_nacimiento = EXCLUDED.fecha_nacimiento, 
                         firh = EXCLUDED.firh`,
                    [
                        pacienteId,
                        datos.identificacion_usuario?.nombre_completo || 'Sin nombre',
                        datos.identificacion_usuario?.apellido || '',
                        datos.identificacion_usuario?.fecha_nacimiento || new Date().toISOString().split('T')[0],
                        SEDE_ACTUAL,
                        JSON.stringify(firhCompleto)
                    ]
                );
                res.status(202).json({ ok: true, mensaje: 'Guardado en BD global y replicado a sedes (nodo caído)', firh: firhCompleto });
            } catch (e) {
                res.status(500).json({ ok: false, detail: err.message });
            }
        } else {
            // Si es una replicación que falló, solo reportar el error
            res.status(500).json({ ok: false, detail: err.message });
        }
    }
});

app.get('/api/firh/historias', async (req, res) => {
    const pacienteId = req.query.paciente_id;
    if (!pacienteId) return res.status(400).json({ detail: 'paciente_id es obligatorio' });
    try {
        const result = await pool.query(
            "SELECT * FROM historias_clinicas WHERE paciente_id = $1 ORDER BY version DESC", 
            [pacienteId]
        );
        
        // Si no hay en BD local, buscar en global
        if (!result.rows[0]) {
            const globalResult = await globalPool.query(
                "SELECT * FROM historias_clinicas WHERE paciente_id = $1 ORDER BY version DESC", 
                [pacienteId]
            );
            if (globalResult.rows[0]) {
                const historias = globalResult.rows.map(r => {
                    let firhData = {};
                    try { firhData = JSON.parse(r.tratamiento || '{}'); } catch {}
                    return {
                        version: r.version,
                        medico_identification: r.medico_identification,
                        sede_actualizacion: r.sede_actualizacion,
                        diagnostico: r.diagnostico,
                        fecha_actualizacion: r.fecha_actualizacion,
                        ...firhData
                    };
                });
                res.json({ historias });
            } else {
                res.json({ historias: [] });
            }
        } else {
            const historias = result.rows.map(r => {
                let firhData = {};
                try { firhData = JSON.parse(r.tratamiento || '{}'); } catch {}
                return {
                    version: r.version,
                    medico_identification: r.medico_identification,
                    sede_actualizacion: r.sede_actualizacion,
                    diagnostico: r.diagnostico,
                    fecha_actualizacion: r.fecha_actualizacion,
                    ...firhData
                };
            });
            res.json({ historias });
        }
    } catch (err) {
        res.status(500).json({ detail: err.message });
    }
});

app.get('/api/triage/historias', async (req, res) => {
    const pacienteId = req.query.paciente_id;
    if (!pacienteId) return res.status(400).json({ detail: 'paciente_id es obligatorio' });
    
    // Pagination parameters with sensible defaults
    const limit = parseInt(req.query.limit) || 50;  // Default to 50 records
    const offset = parseInt(req.query.offset) || 0; // Default to start from beginning
    
    try {
        // Query local database with pagination
        let result = await pool.query(
            "SELECT * FROM triage_records WHERE paciente_id = $1 ORDER BY fecha_registro DESC LIMIT $2 OFFSET $3", 
            [pacienteId, limit, offset]
        );
        
        // If no results in local DB, try global database
        if (!result.rows[0]) {
            result = await globalPool.query(
                "SELECT * FROM triage_records WHERE paciente_id = $1 ORDER BY fecha_registro DESC LIMIT $2 OFFSET $3", 
                [pacienteId, limit, offset]
            );
        }
        
        // Also get total count for pagination info
        let countResult = await pool.query(
            "SELECT COUNT(*) FROM triage_records WHERE paciente_id = $1", 
            [pacienteId]
        );
        let total = parseInt(countResult.rows[0].count);
        
        // If local count is 0, check global count
        if (total === 0) {
            countResult = await globalPool.query(
                "SELECT COUNT(*) FROM triage_records WHERE paciente_id = $1", 
                [pacienteId]
            );
            total = parseInt(countResult.rows[0].count);
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
        
        res.json({ 
            triages,
            pagination: {
                total,
                limit,
                offset,
                hasMore: (offset + limit) < total
            }
        });
    } catch (err) {
        res.status(500).json({ detail: err.message });
    }
});

app.get('/api/paciente/:id', async (req, res) => {
    const pacienteId = decodeURIComponent(req.params.id).trim();
    try {
        let result = await pool.query('SELECT * FROM pacientes WHERE paciente_id = $1', [pacienteId]);
        
        // Si no está en BD local, buscar en global
        if (!result.rows[0]) {
            result = await globalPool.query('SELECT * FROM pacientes WHERE paciente_id = $1', [pacienteId]);
        }
        
        if (!result.rows[0]) {
            return res.status(404).json({ detail: `Paciente "${pacienteId}" no encontrado` });
        }
        const row = result.rows[0];
        // Extraer tipo_documento del FIRH si no está en columna
        const tipo_doc = row.tipo_documento || row.firh?.identificacion_usuario?.tipo_documento || 'CC';
        
        // Obtener triage records with pagination (limit to most recent 50)
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
                tipo_documento: tipo_doc
            },
            firh_guardado: row.firh || {},
            triage,
            historias
        });
    } catch (err) {
        console.error('Error en /api/paciente/:id:', err);
        res.status(500).json({ detail: err.message });
    }
});

app.get('/api/v1/fhir/patients', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const size = parseInt(req.query.size) || 10;
        const offset = (page - 1) * size;
        const countRes = await pool.query('SELECT COUNT(*) FROM pacientes');
        const total = parseInt(countRes.rows[0].count);
        const res2 = await pool.query(`
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
            address: [{ city: p.ciudad_registro_origen || SEDE_ACTUAL }],
            registrado_por: p.registrado_por ? {
                id: p.registrado_por,
                nombre: `${p.medico_nombre || ''} ${p.medico_apellido || ''}`,
                especialidad: p.especialidad
            } : null
        }));
        res.json({ patients: fhirPatients, total, total_pages: Math.ceil(total / size) || 1 });
    } catch (err) {
        res.status(500).json({ detail: err.message });
    }
});

app.get('/api/v1/fhir/metadata', (req, res) => {
    res.json({ resourceType: "CapabilityStatement", status: "active", date: new Date().toISOString(), publisher: `Sede ${SEDE_ACTUAL}`, fhirVersion: "4.0.1" });
});

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'consulta-hc.html')));
app.get('/registro-paciente', (req, res) => res.sendFile(path.join(__dirname, 'registro-paciente.html')));
app.get('/consulta-hc', (req, res) => res.sendFile(path.join(__dirname, 'consulta-hc.html')));
app.get('/firh', (req, res) => res.sendFile(path.join(__dirname, 'firh.html')));
    app.get('/paciente/:id', (req, res) => res.sendFile(path.join(__dirname, 'firh.html')));
app.get('/triage', (req, res) => res.sendFile(path.join(__dirname, 'triage.html')));
app.get('/reportes', (req, res) => res.sendFile(path.join(__dirname, 'reportes.html')));
app.get('/monitor-nodos', (req, res) => res.sendFile(path.join(__dirname, 'monitor-nodos.html')));

app.post('/api/registro/paciente', async (req, res) => {
    const datos = req.body || {};
    const { paciente_id, nombre, apellido, fecha_nacimiento, tipo_documento, registrado_por } = datos;
    if (!paciente_id) return res.status(400).json({ ok: false, detail: 'paciente_id es obligatorio' });
    try {
        await pool.query(
            `INSERT INTO pacientes (paciente_id, nombre, apellido, tipo_documento, fecha_nacimiento, ciudad_registro_origen, registrado_por) 
             VALUES ($1, $2, $3, $4, $5, $6, $7) 
             ON CONFLICT (paciente_id) DO UPDATE 
             SET nombre = EXCLUDED.nombre, apellido = EXCLUDED.apellido, 
                 tipo_documento = EXCLUDED.tipo_documento,
                 fecha_nacimiento = EXCLUDED.fecha_nacimiento,
                 registrado_por = COALESCE(EXCLUDED.registrado_por, pacientes.registrado_por)`,
            [paciente_id, nombre || 'Sin nombre', apellido || 'Sin apellido', tipo_documento || 'CC', fecha_nacimiento || new Date().toISOString().split('T')[0], SEDE_ACTUAL, registrado_por || null]
        );
        res.json({ ok: true, mensaje: `Paciente guardado en nodo ${SEDE_ACTUAL}` });
    } catch (err) {
        res.status(500).json({ ok: false, detail: err.message });
    }
});

async function replicarTriageATodasSedes(triageData) {
    const sedeHosts = { Sincelejo: 'app_sincelejo', Bogota: 'app_bogota', Medellin: 'app_medellin', Registro: 'gateway_registro', FIRH: 'gateway_firh', Triage: 'gateway_triage' };
    for (const [sede, host] of Object.entries(sedeHosts)) {
        if (sede === SEDE_ACTUAL) {
            continue;
        }
        try {
            const response = await fetch(`http://${host}:3000/api/triage`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(triageData)
            });
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            console.log(`[TRIAGE] Replicado a ${sede}`);
        } catch (err) {
            guardarEnCola(sede, triageData, 'triage');
            console.warn(`[TRIAGE] No se pudo replicar a ${sede}:`, err.message);
        }
    }
}

app.post('/api/triage', async (req, res) => {
    const { paciente_id, presion_arterial, frecuencia_cardiaca, temperatura, saturacion_oxigeno, nivel_triage, motivo_consulta, esReplicado } = req.body;
    if (!paciente_id) return res.status(400).json({ ok: false, detail: 'paciente_id es obligatorio' });
    if (!nivel_triage) return res.status(400).json({ ok: false, detail: 'nivel_triage es obligatorio' });
    try {
        // Verificar si es un duplicado reciente (últimos 5 segundos) para prevenir reintentos rápidos
        const recentCheck = await pool.query(
            "SELECT COUNT(*) FROM triage_records WHERE paciente_id = $1 AND fecha_registro > NOW() - INTERVAL '5 seconds'", 
            [paciente_id]
        );
        
        if (parseInt(recentCheck.rows[0].count) > 0 && !esReplicado) {
            // Si es un duplicado reciente y no es una replicación, probablemente sea un reintentó rápido del cliente
            return res.json({ ok: true, mensaje: 'Triage ya registrado recientemente (duplicado evitado)' });
        }
        
        await pool.query(
            `INSERT INTO triage_records (paciente_id, presion_arterial, frecuencia_cardiaca, temperatura, saturacion_oxigeno, nivel_triage, motivo_consulta, sede) 
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
            [paciente_id, presion_arterial, parseInt(frecuencia_cardiaca) || null, parseFloat(temperatura) || null, parseInt(saturacion_oxigeno) || null, parseInt(nivel_triage), motivo_consulta, SEDE_ACTUAL]
        );
        
        // Solo replicar si no es un triage ya replicado (evitar bucles de replicación)
        if (!esReplicado) {
            replicarTriageATodasSedes({ 
                paciente_id, 
                presion_arterial, 
                frecuencia_cardiaca, 
                temperatura, 
                saturacion_oxigeno, 
                nivel_triage, 
                motivo_consulta,
                esReplicado: true
            });
            // Replicar también a BD global como backup
            try {
                await globalPool.query(
                    `INSERT INTO triage_records (paciente_id, presion_arterial, frecuencia_cardiaca, temperatura, saturacion_oxigeno, nivel_triage, motivo_consulta, sede) 
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
                    [paciente_id, presion_arterial, parseInt(frecuencia_cardiaca) || null, parseFloat(temperatura) || null, parseInt(saturacion_oxigeno) || null, parseInt(nivel_triage), motivo_consulta, SEDE_ACTUAL]
                );
            } catch (e) {
                console.warn('[TRIAGE] No se pudo replicar a BD global:', e.message);
            }
        }
        res.json({ ok: true, mensaje: `Triage guardado en nodo ${SEDE_ACTUAL}` });
    } catch (err) {
        res.status(500).json({ ok: false, detail: err.message });
    }
});

app.get('/api/reportes/estadisticas', async (req, res) => {
    try {
        const sedesLocal = await pool.query('SELECT ciudad_registro_origen as sede, COUNT(*) as total FROM pacientes GROUP BY ciudad_registro_origen');
        const sedesGlobal = await globalPool.query('SELECT ciudad_registro_origen as sede, COUNT(*) as total FROM pacientes GROUP BY ciudad_registro_origen');
        const sedes = {};
        sedesLocal.rows.forEach(r => { sedes[r.sede] = (sedes[r.sede] || 0) + parseInt(r.total); });
        sedesGlobal.rows.forEach(r => { sedes[r.sede] = (sedes[r.sede] || 0) + parseInt(r.total); });
        const pacientes_sede = Object.entries(sedes).map(([sede, total]) => ({ sede, total }));

        const triageLocal = await pool.query('SELECT nivel_triage, COUNT(*) as total FROM triage_records GROUP BY nivel_triage ORDER BY nivel_triage');
        const triageGlobal = await globalPool.query('SELECT nivel_triage, COUNT(*) as total FROM triage_records GROUP BY nivel_triage ORDER BY nivel_triage');
        const triage = {};
        triageLocal.rows.forEach(r => { triage[r.nivel_triage] = (triage[r.nivel_triage] || 0) + parseInt(r.total); });
        triageGlobal.rows.forEach(r => { triage[r.nivel_triage] = (triage[r.nivel_triage] || 0) + parseInt(r.total); });
        const triage_niveles = Object.entries(triage).map(([nivel_triage, total]) => ({ nivel_triage: parseInt(nivel_triage), total }));

        const recientesLocal = await pool.query('SELECT paciente_id, nombre, apellido, ciudad_registro_origen, fecha_creacion FROM pacientes ORDER BY fecha_creacion DESC LIMIT 10');
        const recientesGlobal = await globalPool.query('SELECT paciente_id, nombre, apellido, ciudad_registro_origen, fecha_creacion FROM pacientes ORDER BY fecha_creacion DESC LIMIT 10');
        const recientesMap = new Map();
        [...recientesLocal.rows, ...recientesGlobal.rows].forEach(r => {
            if (!recientesMap.has(r.paciente_id)) recientesMap.set(r.paciente_id, r);
        });
        const recientes = Array.from(recientesMap.values()).sort((a, b) => new Date(b.fecha_creacion) - new Date(a.fecha_creacion)).slice(0, 10);

        res.json({ pacientes_sede, triage_niveles, recientes });
    } catch (err) {
        res.status(500).json({ detail: err.message });
    }
});

app.listen(PORT, () => console.log(`Servidor ${SEDE_ACTUAL} en puerto ${PORT}`));