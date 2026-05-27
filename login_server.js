const express = require('express');
const path = require('path');
const app = express();
app.use('/static', express.static(path.join(__dirname, 'static')));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'login-medico.html')));
app.get('/login-medico', (req, res) => res.sendFile(path.join(__dirname, 'login-medico.html')));
const PORT = 3004;
app.listen(PORT, () => console.log('Login server en puerto ' + PORT)).on('error', (err) => {
    console.log('Error:', err.message);
});