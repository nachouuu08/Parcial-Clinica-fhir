const http = require('http');
const options = {
  hostname: 'localhost',
  port: 3003,
  path: '/api/firh/campos',
  method: 'GET'
};
const req = http.request(options, (res) => {
  let data = '';
  res.on('data', (chunk) => { data += chunk; });
  res.on('end', () => { console.log(data); });
});
req.on('error', console.error);
req.end();