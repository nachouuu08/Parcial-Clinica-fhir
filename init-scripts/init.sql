-- Esquema inicial alineado con schema.sql y server.js

CREATE TABLE IF NOT EXISTS pacientes (
    paciente_id VARCHAR(50) PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    apellido VARCHAR(100) NOT NULL,
    fecha_nacimiento DATE NOT NULL,
    ciudad_registro_origen VARCHAR(100) NOT NULL,
    firh JSONB,
    fecha_creacion TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS historias_clinicas (
    historia_id SERIAL PRIMARY KEY,
    paciente_id VARCHAR(50) REFERENCES pacientes(paciente_id) ON DELETE CASCADE,
    version INT NOT NULL,
    medico_identification VARCHAR(50) NOT NULL,
    sede_actualizacion VARCHAR(100) NOT NULL,
    diagnostico TEXT NOT NULL,
    tratamiento TEXT NOT NULL,
    fecha_actualizacion TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_paciente_version UNIQUE (paciente_id, version)
);

CREATE TABLE IF NOT EXISTS cuestionario_registro (
    cuestionario_id SERIAL PRIMARY KEY,
    paciente_id VARCHAR(50) REFERENCES pacientes(paciente_id) ON DELETE CASCADE,
    version_clinica INT NOT NULL,
    tiene_alergias BOOLEAN NOT NULL DEFAULT false,
    detalle_alergias TEXT,
    antecedentes_patologicos TEXT,
    medicamentos_actuales TEXT,
    contacto_emergencia_nombre VARCHAR(150) NOT NULL DEFAULT 'N/A',
    contacto_emergencia_telefono VARCHAR(50) NOT NULL DEFAULT 'N/A',
    fecha_registro TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS triage_records (
    triage_id SERIAL PRIMARY KEY,
    paciente_id VARCHAR(50) REFERENCES pacientes(paciente_id) ON DELETE CASCADE,
    presion_arterial VARCHAR(20),
    frecuencia_cardiaca INT,
    temperatura DECIMAL(4,1),
    saturacion_oxigeno INT,
    nivel_triage INT NOT NULL CHECK (nivel_triage BETWEEN 1 AND 5),
    motivo_consulta TEXT,
    sede VARCHAR(50) NOT NULL,
    fecha_registro TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS diagnosticos_fhir (
    diag_id SERIAL PRIMARY KEY,
    paciente_id VARCHAR(50) REFERENCES pacientes(paciente_id) ON DELETE CASCADE,
    codigo_cie10 VARCHAR(10),
    descripcion TEXT NOT NULL,
    estado_clinico VARCHAR(20) DEFAULT 'active',
    severidad VARCHAR(20),
    sede VARCHAR(50) NOT NULL,
    fecha_registro TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
