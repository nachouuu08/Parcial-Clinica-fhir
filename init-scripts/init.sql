-- init-scripts/init.sql

CREATE TABLE IF NOT EXISTS pacientes (
    paciente_id VARCHAR(50) PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    apellido VARCHAR(100) NOT NULL,
    fecha_nacimiento DATE NOT NULL,
    ciudad_registro_origen VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS historias_clinicas (
    id SERIAL PRIMARY KEY,
    paciente_id VARCHAR(50) REFERENCES pacientes(paciente_id) ON DELETE CASCADE,
    version INT NOT NULL,
    medico_identification VARCHAR(50) NOT NULL,
    sede_actualizacion VARCHAR(100) NOT NULL,
    diagnostico TEXT NOT NULL,
    tratamiento TEXT NOT NULL,
    fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cuestionario_registro (
    id SERIAL PRIMARY KEY,
    paciente_id VARCHAR(50) REFERENCES pacientes(paciente_id) ON DELETE CASCADE,
    version_clinica INT NOT NULL,
    tiene_alergias VARCHAR(10) NOT NULL,
    detalle_alergias TEXT,
    antecedentes_patologicos TEXT,
    medicamentos_actuales TEXT,
    contacto_emergencia_nombre VARCHAR(150),
    contacto_emergencia_telefono VARCHAR(50)
);