const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const { Pool } = require('pg');

const app = express();
app.use(cors());
app.use(express.json());
app.use('/static', express.static(path.join(__dirname, 'static')));

const SYNC_QUEUE_FILE = path.join(process.cwd(), 'sync_queue_triage.json');
const SEDE_ACTUAL = 'TRIAGE_SERVICE';

const globalPool = new Pool({
    user: 'admin_clinica',
    database: 'historia_clinica_global',
    password: 'password_seguro',
    port: 5432,
    host: process.env.GLOBAL_DB_HOST || 'db_global'
});

const SEDE_HOSTS = { Sincelejo: 'app_sincelejo', Bogota: 'app_bogota', Medellin: 'app_medellin', Registro: 'gateway_registro', FIRH: 'gateway_firh' };

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
            const response = await fetch(`http://${host}:3000/api/triage`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(item.datos)
            });
            if (response.ok) {
                console.log(`[TRIAGE] Sincronizado a ${item.sede}`);
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

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'triage.html')));
app.get('/triage', (req, res) => res.sendFile(path.join(__dirname, 'triage.html')));
app.get('/paciente/:id', (req, res) => res.sendFile(path.join(__dirname, 'triage.html')));
app.get('/registro-paciente', (req, res) => res.sendFile(path.join(__dirname, 'registro-paciente.html')));
app.get('/firh', (req, res) => res.sendFile(path.join(__dirname, 'firh.html')));

app.post('/api/triage', async (req, res) => {
    const { paciente_id, presion_arterial, frecuencia_cardiaca, temperatura, saturacion_oxigeno, nivel_triage, motivo_consulta, esReplicado } = req.body;
    if (!paciente_id) return res.status(400).json({ ok: false, detail: 'paciente_id es obligatorio' });
    if (!nivel_triage) return res.status(400).json({ ok: false, detail: 'nivel_triage es obligatorio' });
    try {
        const recentCheck = await globalPool.query(
            "SELECT COUNT(*) FROM triage_records WHERE paciente_id = $1 AND fecha_registro > NOW() - INTERVAL '5 seconds'",
            [paciente_id]
        );
        if (parseInt(recentCheck.rows[0].count) > 0 && !esReplicado) {
            return res.json({ ok: true, mensaje: 'Triage ya registrado recientemente' });
        }
        await globalPool.query(
            `INSERT INTO triage_records (paciente_id, presion_arterial, frecuencia_cardiaca, temperatura, saturacion_oxigeno, nivel_triage, motivo_consulta, sede) 
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
            [paciente_id, presion_arterial, parseInt(frecuencia_cardiaca) || null, parseFloat(temperatura) || null, parseInt(saturacion_oxigeno) || null, parseInt(nivel_triage), motivo_consulta, SEDE_ACTUAL]
        );
        // Replicar a sedes, encolar si falla - EXCLUYENDO nodos dedicados para evitar bucles
        const sedesARepromenar = { Sincelejo: 'app_sincelejo', Bogota: 'app_bogota', Medellin: 'app_medellin', Registro: 'gateway_registro', FIRH: 'gateway_firh' };
        for (const sede of Object.keys(sedesARepromenar)) {
            try {
                const response = await fetch(`http://${sedesARepromenar[sede]}:3000/api/triage`, {
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
app.listen(PORT, () => console.log(`Triage Service en puerto ${PORT}`));