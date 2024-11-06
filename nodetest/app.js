const http = require('http');
const port = 8000;

const server = http.createServer((req, res) => {
  console.log('Received request:', {
    method: req.method,
    url: req.url,
    headers: req.headers
  });

  // Add CORS headers to allow requests from any origin
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  
  // Handle preflight requests
  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  res.writeHead(200, {'Content-Type': 'text/html'});
  res.end('<h1>Hello there</h1>');
});

// Add error handling
server.on('error', (error) => {
  console.error('Server error:', error);
});

// Listen on all network interfaces
server.listen(port, '0.0.0.0', () => {
  console.log(`Test app listening at http://localhost:${port}`);
  console.log('Server configuration:', {
    port: port,
    address: server.address()
  });
});

process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception:', error);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection:', reason);
});
