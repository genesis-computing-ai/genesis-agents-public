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
import { RealtimeClient } from '@openai/realtime-api-beta';

dotenv.config();

const app = express();
const port = process.env.PORT || 8081;
const BACKEND_URL = 'http://127.0.0.1:8080';
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

// Enhanced logging setup
const log = {
  info: (msg, data) => {
    const timestamp = new Date().toISOString();
    console.log(`[${timestamp}] 🟦 INFO:`, msg, data ? JSON.stringify(data, null, 2) : '');
  },
  error: (msg, err) => {
    const timestamp = new Date().toISOString();
    console.error(`[${timestamp}] 🟥 ERROR:`, msg, err);
    if (err?.stack) console.error(err.stack);
  },
  debug: (msg, data) => {
    const timestamp = new Date().toISOString();
    console.log(`[${timestamp}] 🟨 DEBUG:`, msg, data ? JSON.stringify(data, null, 2) : '');
  }
};

// Request logging middleware
app.use((req, res, next) => {
 // log.info('Incoming HTTP Request', {
 //   method: req.method,
 //   url: req.url,
 //   headers: req.headers,
 //   query: req.query,
 //   body: req.body
 // });
  next();
});

// Agents setup
const httpsAgent = new https.Agent({
  rejectUnauthorized: false
});

const httpAgent = new http.Agent();
const getAgent = (url) => url.startsWith('https') ? httpsAgent : httpAgent;

// CORS and JSON middleware
app.use(cors({ 
  origin: true, 
  credentials: true 
}));

app.use(express.json());

// Tools endpoint
app.get('/realtime/tools', async (req, res) => {
  try {
    const url = `${BACKEND_URL}/realtime/get_tools?bot_id=Janice`;
    log.debug('Fetching tools from backend:', { url });
    
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
   // log.debug('Tools fetch response:', data);
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


// Get endpoint URL
app.get('/realtime/get_endpoint', async (req, res) => {
  try {
    const endpoint_name = req.query.endpoint_name || 'udfendpoint';
    const url = `${BACKEND_URL}/realtime/get_endpoint?endpoint_name=${endpoint_name}`;
    log.debug('Fetching endpoint:', { url });
    
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
    log.debug('Endpoint fetch response:', data);
    res.json(data);
  } catch (error) {
    log.error('Failed to fetch endpoint', error);
    res.status(500).json({ 
      error: error.message,
      url: `${BACKEND_URL}/realtime/get_endpoint`,
      stack: error.stack 
    });
  }
});

// Genesis tool endpoint
app.post('/realtime/genesis_tool', async (req, res) => {
  try {
    const url = `${BACKEND_URL}/realtime/genesis_tool`;
  //  log.debug('Calling genesis_tool:', {
  //    url,
  //    requestBody: req.body
   // });
    
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
   // log.debug('Genesis tool response:', data);
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

const server = createServer(app);

// Create WebSocket server
const wss = new WebSocketServer({ 
  noServer: true,
  clientTracking: true
});

// Add periodic logging of connection count
setInterval(() => {
  const clientCount = wss.clients ? wss.clients.size : 0;
  if (clientCount > 0) {
    log.info('Active WebSocket connections:', { count: clientCount });
  }
}, 5000);

// Handle OpenAI realtime connections
async function handleRealtimeConnection(ws, req) {
  log.info('New WebSocket connection attempt', {
    headers: req.headers,
    url: req.url
  });

  if (!OPENAI_API_KEY) {
    log.error('No OpenAI API key found');
    ws.close();
    return;
  }

  try {
    // Create OpenAI client
    log.debug('Creating OpenAI client');
    const client = new RealtimeClient({ 
      apiKey: OPENAI_API_KEY,
      dangerouslyAllowAPIKeyInBrowser: true 
    });

    // Relay: OpenAI -> Browser
    client.realtime.on('server.*', (event) => {
      log.debug('Relaying from OpenAI to client:', { eventType: event.type });
      ws.send(JSON.stringify(event));
    });

    client.realtime.on('close', () => {
      log.info('OpenAI connection closed');
      ws.close();
    });

    // Relay: Browser -> OpenAI
    const messageQueue = [];
    const messageHandler = (data) => {
      try {
        const event = JSON.parse(data);
        log.debug('Relaying from client to OpenAI:', { eventType: event.type });
        client.realtime.send(event.type, event);
      } catch (e) {
        log.error('Error parsing event from client:', e);
      }
    };

    ws.on('message', (data) => {
      if (!client.isConnected()) {
        log.debug('Queueing message (client not connected yet)');
        messageQueue.push(data);
      } else {
        messageHandler(data);
      }
    });

    ws.on('close', () => {
      log.info('WebSocket connection closed by client');
      client.disconnect();
    });

    ws.on('error', (error) => {
      log.error('WebSocket error:', error);
    });

    // Connect to OpenAI
    log.debug('Attempting to connect to OpenAI...');
    await client.connect();
    log.info('Connected to OpenAI successfully');
    
    // Process queued messages
    if (messageQueue.length) {
      log.debug(`Processing ${messageQueue.length} queued messages`);
      while (messageQueue.length) {
        messageHandler(messageQueue.shift());
      }
    }
  } catch (e) {
    log.error('Error in handleRealtimeConnection:', e);
    ws.close();
  }
}

// Handle upgrade requests
server.on('upgrade', (request, socket, head) => {
  log.info('Received upgrade request', {
    url: request.url,
    headers: request.headers
  });

  try {
    const pathname = new URL(request.url, `http://${request.headers.host}`).pathname;
    log.debug(`Processing upgrade for pathname: ${pathname}`);

    if (pathname === '/voice') {
      log.info('Handling voice WebSocket upgrade');
      wss.handleUpgrade(request, socket, head, (ws) => {
        ws.isAlive = true;
        ws.on('pong', () => {
          ws.isAlive = true;
        });
        log.info('Voice WebSocket connection established');
        wss.emit('connection', ws, request);
      });
    } else {
      log.error(`Invalid WebSocket path: ${pathname}`);
      socket.destroy();
    }
  } catch (error) {
    log.error('Error in upgrade handler:', error);
    socket.destroy();
  }
});

// Add ping/pong to keep connections alive
const interval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      log.info('Terminating inactive WebSocket connection');
      return ws.terminate();
    }
    ws.isAlive = false;
    ws.ping(() => {});
  });
}, 30000);

wss.on('close', () => {
  clearInterval(interval);
});

// Handle WebSocket connections
wss.on('connection', handleRealtimeConnection);

// Start server
server.listen(port, '0.0.0.0', () => {
  log.info('Combined relay server running', {
    port,
    backendUrl: BACKEND_URL,
    environment: process.env.NODE_ENV
  });
});

// Error handling
process.on('SIGTERM', () => {
  log.info('Shutting down...');
  server.close(() => process.exit(0));
});

process.on('uncaughtException', (error) => {
  log.error('Uncaught Exception', error);
});