-- Tabla principal de Pacientes (Datos demográficos base)
CREATE TABLE IF NOT EXISTS pacientes (
    paciente_id VARCHAR(50) PRIMARY KEY, -- Identificación única (CC, TI, etc.)
    nombre VARCHAR(100) NOT NULL,
    apellido VARCHAR(100) NOT NULL,
    fecha_nacimiento DATE NOT NULL,
    ciudad_registro_origen VARCHAR(50) NOT NULL, -- Sede donde se creó (ej: 'Sincelejo')
    firh JSONB, -- Datos adicionales en formato JSON para FIRH
    fecha_creacion TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabla de Evolución Clínica (Historial médico versionado)
CREATE TABLE IF NOT EXISTS historias_clinicas (
    historia_id SERIAL PRIMARY KEY,
    paciente_id VARCHAR(50) REFERENCES pacientes(paciente_id) ON DELETE CASCADE,
    version INT NOT NULL, -- Incrementa con cada actualización (1, 2, 3...)
    medico_identificacion VARCHAR(50) NOT NULL,
    sede_actualizacion VARCHAR(50) NOT NULL, -- Ciudad que genera el cambio (ej: 'Bogota')
    diagnostico TEXT NOT NULL,
    tratamiento TEXT NOT NULL,
    fecha_actualizacion TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    -- Clave única para evitar que una misma ciudad pise la misma versión
    CONSTRAINT unique_paciente_version UNIQUE (paciente_id, version)
);

-- Tabla para las preguntas obligatorias del registro del paciente
CREATE TABLE IF NOT EXISTS cuestionario_registro (
    cuestionario_id SERIAL PRIMARY KEY,
    paciente_id VARCHAR(50) REFERENCES pacientes(paciente_id) ON DELETE CASCADE,
    version_clinica INT NOT NULL,
    
    -- Preguntas clave estructuradas
    tiene_alergias BOOLEAN NOT NULL,
    detalle_alergias TEXT,
    antecedentes_patologicos TEXT, -- Enfermedades previas
    medicamentos_actuales TEXT,
    contacto_emergencia_nombre VARCHAR(100) NOT NULL,
    contacto_emergencia_telefono VARCHAR(20) NOT NULL,
    
    fecha_registro TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabla de Triage (Signos vitales y clasificación) - ITEM 4.2
CREATE TABLE IF NOT EXISTS triage_records (
    triage_id SERIAL PRIMARY KEY,
    paciente_id VARCHAR(50) REFERENCES pacientes(paciente_id) ON DELETE CASCADE,
    presion_arterial VARCHAR(20),
    frecuencia_cardiaca INT,
    temperatura DECIMAL(4,1),
    saturacion_oxigeno INT,
    nivel_triage INT NOT NULL CHECK (nivel_triage BETWEEN 1 AND 5), -- Clasificación Manchester/ESI
    motivo_consulta TEXT,
    sede VARCHAR(50) NOT NULL,
    fecha_registro TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabla para mayor granularidad en diagnósticos (Condition FHIR) - ITEM 4.3
CREATE TABLE IF NOT EXISTS diagnosticos_fhir (
    diag_id SERIAL PRIMARY KEY,
    paciente_id VARCHAR(50) REFERENCES pacientes(paciente_id) ON DELETE CASCADE,
    codigo_cie10 VARCHAR(10),
    descripcion TEXT NOT NULL,
    estado_clinico VARCHAR(20) DEFAULT 'active', -- active, recurrence, relapse, inactive, remission, resolved
    severidad VARCHAR(20), -- severe, moderate, mild
    sede VARCHAR(50) NOT NULL,
    fecha_registro TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);