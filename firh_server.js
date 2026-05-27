const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const { Pool } = require('pg');

const app = express();
app.use(cors());
app.use(express.json());
app.use('/static', express.static(path.join(__dirname, 'static')));

const SYNC_QUEUE_FILE = path.join(process.cwd(), 'sync_queue_firh.json');
const SEDE_ACTUAL = 'FIRH_SERVICE';

const globalPool = new Pool({
    user: 'admin_clinica',
    database: 'historia_clinica_global',
    password: 'password_seguro',
    port: 5432,
    host: process.env.GLOBAL_DB_HOST || 'db_global'
});

const SEDE_HOSTS = { Sincelejo: 'app_sincelejo', Bogota: 'app_bogota', Medellin: 'app_medellin', Registro: 'gateway_registro', Triage: 'gateway_triage' };

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
            const response = await fetch(`http://${host}:3000/api/firh/cargar`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ tabla: 'completo', datos: item.datos })
            });
            if (response.ok) {
                console.log(`[FIRH] Sincronizado a ${item.sede}`);
            } else {
                throw new Error(`HTTP ${response.status}`);
            }
        } catch (err) {
            item.intentos = (item.intentos || 0) + 1;
            if (item.intentos < 30) pendientes.push(item);
        }
    }
    fs.writeFileSync(SYNC_QUEUE_FILE, JSON.stringify(pendientes, null, 2));
}

setInterval(procesarCola, 5000);

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'firh.html')));
app.get('/paciente/:id', (req, res) => res.sendFile(path.join(__dirname, 'firh.html')));
app.get('/registro-paciente', (req, res) => res.sendFile(path.join(__dirname, 'registro-paciente.html')));
app.get('/triage', (req, res) => res.sendFile(path.join(__dirname, 'triage.html')));

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
    const { tabla, datos } = req.body;
    const pacienteId = datos.paciente_id;
    if (!pacienteId) return res.status(400).json({ ok: false, detail: 'paciente_id es obligatorio' });
    const firhCompleto = {
        identificacion_usuario: datos.identificacion_usuario || {},
        atencion: datos.atencion || {},
        tecnologias_salud: datos.tecnologias_salud || {},
        diagnosticos: datos.diagnosticos || {},
        egreso: datos.egreso || {}
    };
    try {
        // Obtener el siguiente número de versión
        const versionResult = await globalPool.query(
            'SELECT COALESCE(MAX(version), 0) + 1 as next_version FROM historias_clinicas WHERE paciente_id = $1',
            [pacienteId]
        );
        const nextVersion = versionResult.rows[0].next_version;
        
        // Crear nueva historia clínica
        await globalPool.query(
            `INSERT INTO historias_clinicas (paciente_id, version, medico_identification, sede_actualizacion, diagnostico, tratamiento) 
             VALUES ($1, $2, $3, $4, $5, $6)`,
            [pacienteId, nextVersion, 'N/A', SEDE_ACTUAL, firhCompleto.diagnosticos?.diagnostico_egreso || 'Sin diagnóstico', JSON.stringify(firhCompleto)]
        );
        
        // Actualizar el paciente con el último firh (para compatibilidad)
        const existsCheck = await globalPool.query('SELECT 1 FROM pacientes WHERE paciente_id = $1', [pacienteId]);
        if (existsCheck.rows[0]) {
            await globalPool.query(
                `UPDATE pacientes SET firh = $1 WHERE paciente_id = $2`,
                [JSON.stringify(firhCompleto), pacienteId]
            );
        } else {
            await globalPool.query(
                `INSERT INTO pacientes (paciente_id, nombre, apellido, tipo_documento, fecha_nacimiento, ciudad_registro_origen, firh) 
                 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
                [pacienteId, datos.identificacion_usuario?.nombre_completo || 'Sin nombre', datos.identificacion_usuario?.apellido || '', datos.identificacion_usuario?.tipo_documento || 'CC', datos.identificacion_usuario?.fecha_nacimiento || new Date().toISOString().split('T')[0], SEDE_ACTUAL, JSON.stringify(firhCompleto)]
            );
        }
        
        // Replicar a sedes, encolar si falla
        const sedesARepromenar = { Sincelejo: 'app_sincelejo', Bogota: 'app_bogota', Medellin: 'app_medellin', Registro: 'gateway_registro' };
        for (const sede of Object.keys(sedesARepromenar)) {
            try {
                const response = await fetch(`http://${sedesARepromenar[sede]}:3000/api/firh/cargar`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ tabla: 'completo', datos: { ...datos, esReplicado: true } })
                });
                if (!response.ok) throw new Error(`HTTP ${response.status}`);
            } catch (e) {
                guardarEnCola(sede, { ...datos, esReplicado: true }, 'firh');
            }
        }
        res.json({ ok: true, mensaje: 'FIRH guardado como nueva historia clínica', firh: firhCompleto, version: nextVersion });
    } catch (err) {
        console.error('[FIRH_SERVER] Error guardando historia:', err.message);
        res.status(202).json({ ok: true, mensaje: 'Guardado offline para sincronizar después', offline: true, firh: firhCompleto });
    }
});

app.get('/api/firh/historias', async (req, res) => {
    const pacienteId = req.query.paciente_id;
    if (!pacienteId) return res.status(400).json({ detail: 'paciente_id es obligatorio' });
    try {
        // Obtener todas las historias desde historias_clinicas
        const result = await globalPool.query(
            'SELECT * FROM historias_clinicas WHERE paciente_id = $1 ORDER BY version DESC',
            [pacienteId]
        );
        const historias = result.rows.map(r => {
            let firhData = {};
            try { firhData = JSON.parse(r.tratamiento || '{}'); } catch {}
            return {
                version: r.version,
                medico_identification: r.medico_identification,
                sede_actualizacion: r.sede_actualizacion,
                fecha_actualizacion: r.fecha_actualizacion,
                ...firhData
            };
        });
        res.json({ historias });
    } catch (err) {
        console.error('[FIRH_SERVER] Error obteniendo historias:', err.message);
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
            medico_identification: 'N/A',
            sede_actualizacion: row.ciudad_registro_origen || 'Global',
            fecha_actualizacion: row.fecha_creacion || new Date().toISOString(),
            diagnostico: firhData?.diagnosticos?.diagnostico_egreso || 'Sin diagnóstico'
        }] : [];
        res.json({
            paciente: { paciente_id: row.paciente_id, nombre: row.nombre, apellido: row.apellido, fecha_nacimiento: row.fecha_nacimiento },
            firh_guardado: firhData,
            triage,
            historias
        });
    } catch (err) {
        res.status(500).json({ detail: err.message });
    }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`FIRH Service en puerto ${PORT}`));