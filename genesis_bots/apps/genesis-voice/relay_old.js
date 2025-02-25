// npm install express cors node-fetch

const express = require('express');
const cors = require('cors');
const fetch = require('node-fetch');  // You might need to install this

const app = express();
const port = process.env.PORT || 8080;

// Enable CORS
app.use(cors());
app.use(express.json());

// Proxy endpoint for tools
app.get('/realtime/tools', async (req, res) => {
  try {
    const response = await fetch(`http://localhost:8080/realtime/tools?bot_id=Janice`);
    const data = await response.json();
    res.json(data);
  } catch (error) {
    console.error('Error fetching tools:', error);
    res.status(500).json({ 
      success: false, 
      message: 'Failed to fetch tools',
      error: error.message 
    });
  }
});

// Proxy endpoint for tool execution
app.post('/realtime/genesis_tool', async (req, res) => {
  try {
    const response = await fetch('http://localhost:8080/realtime/genesis_tool', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(req.body)
    });
    const data = await response.json();
    res.json(data);
  } catch (error) {
    console.error('Error executing tool:', error);
    res.status(500).json({ 
      success: false, 
      message: 'Failed to execute tool',
      error: error.message 
    });
  }
});

// Add other proxy endpoints as needed...

app.listen(port, () => {
  console.log(`Relay server listening at http://localhost:${port}`);
});