/**
 * Ayudante de autenticación para SMART on FHIR / OAuth2
 */
const AuthHelper = {
    // Intentar obtener el token del almacenamiento local
    getToken() {
        return localStorage.getItem('fhir_access_token');
    },

    // Guardar el token
    setToken(token) {
        localStorage.setItem('fhir_access_token', token);
    },

    // Obtener un nuevo token del servidor
    async refreshToken(port = null) {
        try {
            const authUrl = port ? `http://${window.location.hostname}:${port}/api/v1/auth/token` : `/api/v1/auth/token`;
            const response = await fetch(authUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    client_id: 'clinica_node',
                    client_secret: 'admin_prueba'
                })
            });

            if (!response.ok) throw new Error('Error al autenticar');

            const data = await response.json();
            const tokenKey = port ? `fhir_token_${port}` : 'fhir_access_token';
            localStorage.setItem(tokenKey, data.access_token);
            return data.access_token;
        } catch (error) {
            console.error('Error de autenticación:', error);
            return null;
        }
    },

    // Wrapper para fetch que incluye el token
    async authenticatedFetch(url, options = {}, port = null) {
        const tokenKey = port ? `fhir_token_${port}` : 'fhir_access_token';
        let token = localStorage.getItem(tokenKey);

        // Si no hay token, intentar obtener uno
        if (!token) {
            token = await this.refreshToken(port);
        }

        if (!options.headers) options.headers = {};
        options.headers['Authorization'] = `Bearer ${token}`;

        let response = await fetch(url, options);

        // Si el token expiró (401 o 403), intentar refrescar una vez
        if (response.status === 401 || response.status === 403) {
            token = await this.refreshToken(port);
            if (token) {
                options.headers['Authorization'] = `Bearer ${token}`;
                response = await fetch(url, options);
            }
        }

        return response;
    }
};
