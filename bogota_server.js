const express = require('express');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());
app.use('/static', express.static(path.join(__dirname, 'static')));

const SEDE_ACTUAL = process.env.SEDE_ACTUAL || 'Sincelejo';

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

app.post('/auth/token', (req, res) => {
    const header = { alg: "HS256", typ: "JWT" };
    const payload = { iss: "auth_central", aud: "api_clinicas", sede: SEDE_ACTUAL, role: "professional", exp: Math.floor(Date.now() / 1000) + 3600 };
    const headerB64 = Buffer.from(JSON.stringify(header)).toString('base64url');
    const payloadB64 = Buffer.from(JSON.stringify(payload)).toString('base64url');
    const firmaB64 = Buffer.from('dummy').toString('base64url');
    res.json({ access_token: `${headerB64}.${payloadB64}.${firmaB64}`, token_type: "Bearer", expires_in: 3600, scope: 'patient/*.read patient/*.write' });
});

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

app.post('/api/firh/cargar', async (req, res) => {
    const { tabla, datos } = req.body;
    const reglas = REGLAS_FIRH[tabla] || [];
    const errores = validarCampos(reglas, datos);
    if (errores.length > 0) return res.status(400).json({ ok: false, tabla, errores });
    res.json({ ok: true, mensaje: 'FIRH guardado (modo demo sin BD)' });
});

app.get('/api/paciente/:id', async (req, res) => {
    const pacienteId = decodeURIComponent(req.params.id).trim();
    res.status(404).json({ detail: `Paciente "${pacienteId}" no encontrado` });
});

app.get('/api/v1/fhir/patients', async (req, res) => { res.json({ patients: [], total: 0, total_pages: 1 }); });
app.get('/api/v1/fhir/patients/search', async (req, res) => { res.json({ patients: [], total: 0 }); });
app.get('/api/v1/fhir/metadata', (req, res) => { res.json({ resourceType: "CapabilityStatement", status: "active", date: new Date().toISOString(), publisher: `Sede ${SEDE_ACTUAL}`, fhirVersion: "4.0.1" }); });

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'consulta-hc.html')));
app.get('/registro-paciente', (req, res) => res.sendFile(path.join(__dirname, 'registro-paciente.html')));
app.get('/consulta-hc', (req, res) => res.sendFile(path.join(__dirname, 'consulta-hc.html')));
app.get('/firh', (req, res) => res.sendFile(path.join(__dirname, 'firh.html')));
app.get('/paciente/:id', (req, res) => res.sendFile(path.join(__dirname, 'firh.html')));
app.get('/triage', (req, res) => res.sendFile(path.join(__dirname, 'triage.html')));
app.get('/reportes', (req, res) => res.sendFile(path.join(__dirname, 'reportes.html')));
app.get('/monitor-nodos', (req, res) => res.sendFile(path.join(__dirname, 'monitor-nodos.html')));

app.listen(PORT, () => console.log(`Servidor ${SEDE_ACTUAL} en puerto ${PORT}`));