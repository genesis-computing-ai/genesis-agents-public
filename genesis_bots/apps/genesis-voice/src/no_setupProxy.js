const { createProxyMiddleware } = require('http-proxy-middleware');

module.exports = function(app) {
  // Keep existing proxy from package.json for /realtime
  // Only add WebSocket proxy for voice
  app.use(
    '/ws',  // New path for WebSocket
    createProxyMiddleware({
      target: 'http://localhost:8081',
      changeOrigin: true,
      ws: true
    })
  );
};