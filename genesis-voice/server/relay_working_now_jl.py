import express from 'express';
import cors from 'cors';
import fetch from 'node-fetch';
import dotenv from 'dotenv';
import { createServer } from 'http';
import { WebSocketServer } from 'ws';
import httpProxy from 'http-proxy';
import https from 'https';
import http from 'http';
import WebSocket from 'ws';

dotenv.config();

const app = express();

const port = process.env.PORT || 8082;
const BACKEND_URL = 'http://127.0.0.1:8080';



// Create agents for both HTTP and HTTPS
const httpsAgent = new https.Agent({
  rejectUnauthorized: false
});

const httpAgent = new http.Agent();

// Helper function to get the appropriate agent based on URL
const getAgent = (url) => {
  return url.startsWith('https') ? httpsAgent : httpAgent;
};

// Enhanced logging
const log = {
  info: (msg, data) => console.log(`[${new Date().toISOString()}] INFO:`, msg, data || ''),
  error: (msg, err) => console.error(`[${new Date().toISOString()}] ERROR:`, msg, err)
};

app.use(cors({
  origin: true,
  credentials: true
}));

app.use(express.json());

// Request logging
app.use((req, res, next) => {
  log.info(`Incoming ${req.method} ${req.url}`, {
    headers: req.headers,
    query: req.query
  });
  next();
});

// Tools endpoint
app.get('/realtime/tools', async (req, res) => {
  try {
    const url = `${BACKEND_URL}/realtime/get_tools?bot_id=Janice`;
    log.info('Fetching tools from backend:', url);
    
    const response = await fetch(url, {
      agent: getAgent(url),
      headers: {
        'Accept': 'application/json'
      }
    });
    
    if (!response.ok) {
      throw new Error(`Backend responded with ${response.status}`);
    }
    
    const data = await response.json();
    log.info('Tools fetched successfully');
    res.json(data);
  } catch (error) {
    log.error('Failed to fetch tools', error);
    res.status(500).json({ 
      error: error.message,
      url: `${BACKEND_URL}/realtime/get_tools?bot_id=Janice`,
      stack: error.stack 
    });
  }
});

// Genesis tool endpoint
app.post('/realtime/genesis_tool', async (req, res) => {
  try {
    const url = `${BACKEND_URL}/realtime/genesis_tool`;
    log.info('Calling genesis_tool:', url);
    
    const response = await fetch(url, {
      method: 'POST',
      agent: getAgent(url),
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      },
      body: JSON.stringify(req.body)
    });
    
    if (!response.ok) {
      throw new Error(`Backend responded with ${response.status}`);
    }
    
    const data = await response.json();
    log.info('Genesis tool called successfully');
    res.json(data);
  } catch (error) {
    log.error('Failed to call genesis_tool', error);
    res.status(500).json({ 
      error: error.message,
      url: `${BACKEND_URL}/realtime/genesis_tool`,
      stack: error.stack 
    });
  }
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ 
    status: 'healthy', 
    timestamp: new Date().toISOString(),
    backend_url: BACKEND_URL
  });
});

// 404 handler
app.use((req, res) => {
  log.info(`404 - Not Found: ${req.method} ${req.url}`);
  res.status(404).json({
    error: 'Not Found',
    message: `Endpoint ${req.method} ${req.url} not found`,
    available_endpoints: [
      'GET /health',
      'GET /realtime/tools',
      'POST /realtime/genesis_tool'
    ]
  });
});

// Error handler
app.use((err, req, res, next) => {
  log.error('Unhandled error', err);
  res.status(500).json({ 
    error: 'Internal Server Error',
    message: err.message,
    stack: err.stack
  });
});

const server = createServer(app);

server.listen(port, '0.0.0.0', () => {
  log.info('Relay server running', {
    port,
    backendUrl: BACKEND_URL,
    environment: process.env.NODE_ENV
  });
});


// Create WebSocket server
const wss = new WebSocketServer({ server });

// Handle WebSocket connections
wss.on('connection', (ws, req) => {
  log.info('WebSocket connection received', {
    url: req.url,
    headers: req.headers
  });

  // Extract the original query parameters
  const originalUrl = req.url;
  
  // Create connection to OpenAI with the same path and query parameters
  const openaiWs = new WebSocket('wss://api.openai.com:443' + originalUrl, {
    headers: {
      'Authorization': `Bearer ${'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'}`,
      'Origin': 'https://api.openai.com',
      'User-Agent': req.headers['user-agent'],
      'Content-Type': 'application/json',
    },
    rejectUnauthorized: false // Only if needed for development
  });

  // Add more detailed logging
  log.info('Connecting to OpenAI WebSocket', {
    url: 'wss://api.openai.com:443' + originalUrl,
    originalUrl: originalUrl
  });

  // Handle OpenAI connection
  openaiWs.on('open', () => {
    log.info('Connected to OpenAI WebSocket');
  });

  openaiWs.on('message', (data) => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(data);
    }
  });

  openaiWs.on('error', (error) => {
    log.error('OpenAI WebSocket error', error);
  });

  openaiWs.on('close', () => {
    log.info('OpenAI WebSocket closed');
    if (ws.readyState === WebSocket.OPEN) {
      ws.close();
    }
  });

  // Handle client messages
  ws.on('message', (data) => {
    if (openaiWs.readyState === WebSocket.OPEN) {
      openaiWs.send(data);
    }
  });

  ws.on('close', () => {
    log.info('Client WebSocket closed');
    if (openaiWs.readyState === WebSocket.OPEN) {
      openaiWs.close();
    }
  });

  ws.on('error', (error) => {
    log.error('Client WebSocket error', error);
  });
});


// Handle process termination
process.on('SIGTERM', () => {
  log.info('Shutting down...');
  server.close(() => process.exit(0));
});

process.on('uncaughtException', (error) => {
  log.error('Uncaught Exception', error);
});

