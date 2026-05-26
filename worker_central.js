const { Pool } = require('pg');
const { Kafka } = require('kafkajs');

// 1. Conexión a la Base de Datos Global (Central)
const pool = new Pool({
    user: 'admin_clinica',
    host: 'localhost',
    database: 'historia_clinica_global',
    password: 'password_seguro',
    port: 5432,
});

// 2. Conexión a Kafka
const kafka = new Kafka({
    clientId: 'worker-central',
    brokers: ['localhost:9092']
});
const consumer = kafka.consumer({ groupId: 'clúster-central-sincronizador' });

const iniciarConsumidor = async () => {
    await consumer.connect();

    // --- PARCHE DE COMPATIBILIDAD ---
    try {
        await pool.query('ALTER TABLE pacientes ADD COLUMN IF NOT EXISTS fecha_creacion TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP');
    } catch (err) {}

    // Nos suscribimos al canal de eventos clínicos desde el principio de los tiempos (fromBeginning)
    await consumer.subscribe({ topic: 'eventos-clinicos', fromBeginning: true });
    console.log('>>> Sincronizador Central escuchando eventos de la WAN...');

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const evento = JSON.parse(message.value.toString());
            const {
                paciente_id, nombre, apellido, fecha_nacimiento, version, medico_id, diagnostico, tratamiento,
                preguntas // Extraemos el bloque de preguntas obligatorias enviado por la sede
            } = evento.data;

            console.log(`[EVENTO RECIBIDO] Sincronizando registro completo del paciente ${paciente_id} desde: ${evento.sede}`);

            try {
                await pool.query('BEGIN');

                // 1. Insertar o verificar los datos demográficos base en el maestro central
                await pool.query(`
                    INSERT INTO pacientes (paciente_id, nombre, apellido, fecha_nacimiento, ciudad_registro_origen)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (paciente_id) DO NOTHING`,
                    [paciente_id, nombre, apellido, fecha_nacimiento, evento.sede]
                );

                // 2. Insertar la evolución clínica en el maestro central
                await pool.query(`
                    INSERT INTO historias_clinicas (paciente_id, version, medico_identification, sede_actualizacion, diagnostico, tratamiento)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT ON CONSTRAINT unique_paciente_version DO NOTHING`,
                    [paciente_id, version, medico_id, evento.sede, diagnostico, tratamiento]
                );

                // 3. Insertar las preguntas críticas de control en el maestro central
                if (preguntas) {
                    await pool.query(`
                        INSERT INTO cuestionario_registro (paciente_id, version_clinica, tiene_alergias, detalle_alergias, antecedentes_patologicos, medicamentos_actuales, contacto_emergencia_nombre, contacto_emergencia_telefono)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
                        [
                            paciente_id,
                            version,
                            preguntas.tiene_alergias,
                            preguntas.detalle_alergias,
                            preguntas.antecedentes_patologicos,
                            preguntas.medicamentos_actuales,
                            preguntas.contacto_nombre,
                            preguntas.contacto_telefono
                        ]
                    );
                }

                await pool.query('COMMIT');
                console.log(`[ÉXITO CENTRAL] Datos demográficos, clínicos y cuestionario sincronizados para el paciente: ${paciente_id}`);

            } catch (error) {
                await pool.query('ROLLBACK');
                console.error(`[ERROR DE SINCRONIZACIÓN] No se pudo procesar el evento en la Central:`, error.message);
            }
        },
    });
};

iniciarConsumidor().catch(console.error);