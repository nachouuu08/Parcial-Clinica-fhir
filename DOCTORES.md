# Médicos de Prueba - Sistema Distribuido

## Credenciales para Login

| ID Médico | Nombre | Apellido | Sede | Especialidad | Puerta del Nodo |
|-----------|--------|----------|------|--------------|
| MED-001 | Carlos | Gómez | **Sincelejo** | Medicina General | localhost:3001 |
| MED-002 | Ana | Rodríguez | **Bogota** | Medicina Interna | localhost:3002 |
| MED-003 | Luis | Martínez | **Medellin** | Pediatría | localhost:3003 |

## Flujo del Sistema

### 1. Login del Médico
- URL: `http://localhost:3004/login-medico`
- Ingresar el **ID del médico** (ej: MED-001)
- El sistema redirige automáticamente al nodo de tu sede

### 2. Registro de Paciente
Al crear un paciente desde "Nuevo Paciente":
- El paciente se guarda en la **sede del doctor** que inició sesión
- Se replica automáticamente a **todas las otras sedes** vía Kafka

### 3. Logout
- Desde `consulta-hc.html`, usar el botón "Cerrar Sesión" (rojo)
- Esto borra las credenciales y regresa al login

### 4. URLs de Acceso

| Servicio | URL |
|----------|-----|
| Login Médico | http://localhost:3004/login-medico |
| Sincelejo | http://localhost:3001/consulta-hc |
| Bogotá | http://localhost:3002/consulta-hc |
| Medellín | http://localhost:3003/consulta-hc |
| Grafana | http://localhost:3005 |
| Prometheus | http://localhost:9090 |
1. Login: MED-001 → sede: Sincelejo
2. Registrar paciente → se guarda en Sincelejo
3. Paciente aparece automáticamente en Bogotá y Medellín
```

### Doctor de Bogotá (MED-002)
```
1. Login: MED-002 → sede: Bogota
2. Registrar paciente → se guarda en Bogotá
3. Paciente aparece automáticamente en Sincelejo y Medellín
```

### Doctor de Medellín (MED-003)
```
1. Login: MED-003 → sede: Medellin
2. Registrar paciente → se guarda en Medellín
3. Paciente aparece automáticamente en Sincelejo y Bogotá
```