/**
 * Running a local relay server will allow you to hide your API key
 * and run custom logic on the server
 *
 * Set the local relay server address to:
 * REACT_APP_LOCAL_RELAY_SERVER_URL=http://localhost:8081
 *
 * This will also require you to set OPENAI_API_KEY= in a `.env` file
 * You can run it with `npm run relay`, in parallel with `npm start`
 */

import React, { useState, useEffect, useRef, useCallback } from 'react';
//import { Helmet } from 'react-helmet'; // Ensure react-helmet is imported

import { RealtimeClient } from '@openai/realtime-api-beta';
import { ItemType } from '@openai/realtime-api-beta/dist/lib/client.js';
import { WavRecorder, WavStreamPlayer } from '../lib/wavtools/index.js';
import { instructions } from '../utils/conversation_config.js';
import { WavRenderer } from '../utils/wav_renderer';

import { X, Edit, Zap, ArrowUp, ArrowDown } from 'react-feather';
import { Button } from '../components/button/Button';
import { Toggle } from '../components/toggle/Toggle';
import { Map } from '../components/Map';
import { Table } from '../components/table/Table';

import './ConsolePage.scss';
import { isJsxOpeningLikeElement } from 'typescript';

// const LOCAL_RELAY_SERVER_URL_TOOLS: string = '/realtime';
// const LOCAL_RELAY_SERVER_URL: string = 'http://localhost:8081';


const LOCAL_RELAY_SERVER_URL_TOOLS: string = '/realtime';
//const LOCAL_RELAY_SERVER_URL: string = 'http://localhost:8081/voice';  
//const LOCAL_RELAY_SERVER_URL: string = '/voice';  
const LOCAL_RELAY_SERVER_URL: string = '';

/**
 * Type for result from get_weather() function call
 */
interface Coordinates {
  lat: number;
  lng: number;
  location?: string;
  temperature?: {
    value: number;
    units: string;
  };
  wind_speed?: {
    value: number;
    units: string;
  };
}

/**
 * Type for all event logs
 */
interface RealtimeEvent {
  time: string;
  source: 'client' | 'server';
  count?: number;
  event: { [key: string]: any };
}

// Add this interface for tool call results
interface ToolCallResult {
  toolName: string;
  result: any;
  base64Image?: {
    filename: string;
    content: string;
  };
}

export function ConsolePage() {
  /**
   * Ask user for API Key
   * If we're using the local relay server, we don't need this
   */
  const apiKey = LOCAL_RELAY_SERVER_URL
    ? ''
    : localStorage.getItem('tmp::voice_api_key') ||
      prompt('OpenAI API Key') ||
      '';
  if (apiKey !== '') {
    localStorage.setItem('tmp::voice_api_key', apiKey);
  }

  /**
   * Instantiate:
   * - WavRecorder (speech input)
   * - WavStreamPlayer (speech output)
   * - RealtimeClient (API client)
   */
  const wavRecorderRef = useRef<WavRecorder>(
    new WavRecorder({ sampleRate: 24000 })
  );
  const wavStreamPlayerRef = useRef<WavStreamPlayer>(
    new WavStreamPlayer({ sampleRate: 24000 })
  );
  const clientRef = useRef<RealtimeClient>(
    new RealtimeClient(
      LOCAL_RELAY_SERVER_URL
        ? { url: LOCAL_RELAY_SERVER_URL }
        : {
            apiKey: apiKey,
            dangerouslyAllowAPIKeyInBrowser: true,
          }
    )
  );

  /**
   * References for
   * - Rendering audio visualization (canvas)
   * - Autoscrolling event logs
   * - Timing delta for event log displays
   */
  const clientCanvasRef = useRef<HTMLCanvasElement>(null);
  const serverCanvasRef = useRef<HTMLCanvasElement>(null);
  const eventsScrollHeightRef = useRef(0);
  const eventsScrollRef = useRef<HTMLDivElement>(null);
  const startTimeRef = useRef<string>(new Date().toISOString());

  /**
   * All of our variables for displaying application state
   * - items are all conversation items (dialog)
   * - realtimeEvents are event logs, which can be expanded
   * - memoryKv is for set_memory() function
   * - coords, marker are for get_weather() function
   */
  const [items, setItems] = useState<ItemType[]>([]);
  const [realtimeEvents, setRealtimeEvents] = useState<RealtimeEvent[]>([]);
  const [expandedEvents, setExpandedEvents] = useState<{
    [key: string]: boolean;
  }>({});
  const [isConnected, setIsConnected] = useState(false);
  const [canPushToTalk, setCanPushToTalk] = useState(false);
  const [isRecording, setIsRecording] = useState(false);
  const [coords, setCoords] = useState<Coordinates | null>({
    lat: 37.775593,
    lng: -122.418137,
  });
  const [marker, setMarker] = useState<Coordinates | null>(null);

  const [lastQueryResult, setLastQueryResult] = useState<any[] | null>(null);
  const [graphImage, setGraphImage] = useState<string | null>(null);
  const [splitPosition, setSplitPosition] = useState(50);
  const [isResizing, setIsResizing] = useState(false);
  const rightPanelRef = useRef<HTMLDivElement>(null);
  const graphImageRef = useRef<HTMLImageElement>(null);

  /**
   * Utility for formatting the timing of logs
   */
  const formatTime = useCallback((timestamp: string) => {
    const startTime = startTimeRef.current;
    const t0 = new Date(startTime).valueOf();
    const t1 = new Date(timestamp).valueOf();
    const delta = t1 - t0;
    const hs = Math.floor(delta / 10) % 100;
    const s = Math.floor(delta / 1000) % 60;
    const m = Math.floor(delta / 60_000) % 60;
    const pad = (n: number) => {
      let s = n + '';
      while (s.length < 2) {
        s = '0' + s;
      }
      return s;
    };
    return `${pad(m)}:${pad(s)}.${pad(hs)}`;
  }, []);

  /**
   * When you click the API key
   */
  const resetAPIKey = useCallback(() => {
    const apiKey = prompt('OpenAI API Key');
    if (apiKey !== null) {
      localStorage.clear();
      localStorage.setItem('tmp::voice_api_key', apiKey);
      window.location.reload();
    }
  }, []);

  /**
   * Connect to conversation:
   * WavRecorder taks speech input, WavStreamPlayer output, client is API client
   */

    /**
   * Disconnect and reset conversation state
   */
    const disconnectConversation = useCallback(async () => {
      setIsConnected(false);
      setRealtimeEvents([]);
      setItems([]);
      setCoords({
        lat: 37.775593,
        lng: -122.418137,
      });
      setMarker(null);
  
      const client = clientRef.current;
      client.disconnect();
  
      const wavRecorder = wavRecorderRef.current;
      await wavRecorder.end();
  
      const wavStreamPlayer = wavStreamPlayerRef.current;
      await wavStreamPlayer.interrupt();
    }, []);

  const connectConversation = useCallback(async () => {
    const client = clientRef.current;
    const wavRecorder = wavRecorderRef.current;
    const wavStreamPlayer = wavStreamPlayerRef.current;
  
    try {
      // Set state variables
      startTimeRef.current = new Date().toISOString();
      setIsConnected(false); // Start as false until we confirm connection
      setRealtimeEvents([]);
      setItems(client.conversation.getItems());
  
      // Connect to microphone
      await wavRecorder.begin();
  
      // Connect to audio output
      await wavStreamPlayer.connect();
  
      // Connect to realtime API and wait for connection
      await client.connect();
      
      // Wait a moment to ensure connection is established
      await new Promise(resolve => setTimeout(resolve, 1000));
      
      // Verify connection before proceeding
      if (!client.isConnected()) {
        throw new Error('Failed to establish connection');
      }
  
      // Now that we're connected, update state and send initial message
      setIsConnected(true);
      
      await client.sendUserMessageContent([
        {
          type: `input_text`,
          text: `Hi, I'm Justin Langseth.`,
        },
      ]);
  
      if (client.getTurnDetectionType() === 'server_vad') {
        await wavRecorder.record((data) => {
          if (client.isConnected()) {
            client.appendInputAudio(data.mono);
          }
        });
      }
    } catch (error) {
      console.error('Connection error:', error);
      await disconnectConversation();
      // Optionally show error to user
      alert('Failed to connect. Please try again.');
    }
  }, [disconnectConversation]);




  const deleteConversationItem = useCallback(async (id: string) => {
    const client = clientRef.current;
    client.deleteItem(id);
  }, []);

  /**
   * In push-to-talk mode, start recording
   * .appendInputAudio() for each sample
   */
const startRecording = async () => {
  try {
    const client = clientRef.current;
    if (!client.isConnected()) {
      throw new Error('Client not connected');
    }

    setIsRecording(true);
    const wavRecorder = wavRecorderRef.current;
    const wavStreamPlayer = wavStreamPlayerRef.current;
    
    const trackSampleOffset = await wavStreamPlayer.interrupt();
    if (trackSampleOffset?.trackId) {
      const { trackId, offset } = trackSampleOffset;
      await client.cancelResponse(trackId, offset);
    }
    
    await wavRecorder.record((data) => {
      if (client.isConnected()) {
        client.appendInputAudio(data.mono);
      }
    });
  } catch (error) {
    console.error('Recording error:', error);
    setIsRecording(false);
    // Optionally show error to user
    alert('Failed to start recording. Please try reconnecting.');
  }
};

  /**
   * In push-to-talk mode, stop recording
   */
  const stopRecording = async () => {
    setIsRecording(false);
    const client = clientRef.current;
    const wavRecorder = wavRecorderRef.current;
    await wavRecorder.pause();
    client.createResponse();
  };

  /**
   * Switch between Manual <> VAD mode for communication
   */
  const changeTurnEndType = async (value: string) => {
    const client = clientRef.current;
    const wavRecorder = wavRecorderRef.current;
    if (value === 'none' && wavRecorder.getStatus() === 'recording') {
      await wavRecorder.pause();
    }
    client.updateSession({
      turn_detection: value === 'none' ? null : { type: 'server_vad' },
    });
    if (value === 'server_vad' && client.isConnected()) {
      await wavRecorder.record((data) => client.appendInputAudio(data.mono));
    }
    setCanPushToTalk(value === 'none');
  };

  /**
   * Auto-scroll the event logs
   */
  useEffect(() => {
    if (eventsScrollRef.current) {
      const eventsEl = eventsScrollRef.current;
      const scrollHeight = eventsEl.scrollHeight;
      // Only scroll if height has just changed
      if (scrollHeight !== eventsScrollHeightRef.current) {
        eventsEl.scrollTop = scrollHeight;
        eventsScrollHeightRef.current = scrollHeight;
      }
    }
  }, [realtimeEvents]);

  /**
   * Auto-scroll the conversation logs
   */
  useEffect(() => {
    const conversationEls = [].slice.call(
      document.body.querySelectorAll('[data-conversation-content]')
    );
    for (const el of conversationEls) {
      const conversationEl = el as HTMLDivElement;
      conversationEl.scrollTop = conversationEl.scrollHeight;
    }
  }, [items]);

  /**
   * Set up render loops for the visualization canvas
   */
  useEffect(() => {
    let isLoaded = true;

    const wavRecorder = wavRecorderRef.current;
    const clientCanvas = clientCanvasRef.current;
    let clientCtx: CanvasRenderingContext2D | null = null;

    const wavStreamPlayer = wavStreamPlayerRef.current;
    const serverCanvas = serverCanvasRef.current;
    let serverCtx: CanvasRenderingContext2D | null = null;

    const render = () => {
      if (isLoaded) {
        if (clientCanvas) {
          if (!clientCanvas.width || !clientCanvas.height) {
            clientCanvas.width = clientCanvas.offsetWidth;
            clientCanvas.height = clientCanvas.offsetHeight;
          }
          clientCtx = clientCtx || clientCanvas.getContext('2d');
          if (clientCtx) {
            clientCtx.clearRect(0, 0, clientCanvas.width, clientCanvas.height);
            const result = wavRecorder.recording
              ? wavRecorder.getFrequencies('voice')
              : { values: new Float32Array([0]) };
            WavRenderer.drawBars(
              clientCanvas,
              clientCtx,
              result.values,
              '#0099ff',
              10,
              0,
              8
            );
          }
        }
        if (serverCanvas) {
          if (!serverCanvas.width || !serverCanvas.height) {
            serverCanvas.width = serverCanvas.offsetWidth;
            serverCanvas.height = serverCanvas.offsetHeight;
          }
          serverCtx = serverCtx || serverCanvas.getContext('2d');
          if (serverCtx) {
            serverCtx.clearRect(0, 0, serverCanvas.width, serverCanvas.height);
            const result = wavStreamPlayer.analyser
              ? wavStreamPlayer.getFrequencies('voice')
              : { values: new Float32Array([0]) };
            WavRenderer.drawBars(
              serverCanvas,
              serverCtx,
              result.values,
              '#009900',
              10,
              0,
              8
            );
          }
        }
        window.requestAnimationFrame(render);
      }
    };
    render();

    return () => {
      isLoaded = false;
    };
  }, []);

  /**
   * Core RealtimeClient and audio capture setup
   * Set all of our instructions, tools, events and more
   */
  useEffect(() => {
    // Get refs
    const wavStreamPlayer = wavStreamPlayerRef.current;
    const client = clientRef.current;

    // Set instructions
    client.updateSession({ instructions: instructions });
    // Set transcription, otherwise we don't get user transcriptions back
    client.updateSession({ input_audio_transcription: { model: 'whisper-1' } });

    // Add tools
    client.addTool(
      {
        name: 'set_memory',
        description: 'Saves important data about the user into memory.',
        parameters: {
          type: 'object',
          properties: {
            key: {
              type: 'string',
              description:
                'The key of the memory value. Always use lowercase and underscores, no other characters.',
            },
            value: {
              type: 'string',
              description: 'Value can be anything represented as a string',
            },
          },
          required: ['key', 'value'],
        },
      },
      async ({ key, value }: { [key: string]: any }) => {
        return { ok: true };
      }
    );
    client.addTool(
      {
        name: 'get_weather',
        description:
          'Retrieves the weather for a given lat, lng coordinate pair. Specify a label for the location.',
        parameters: {
          type: 'object',
          properties: {
            lat: {
              type: 'number',
              description: 'Latitude',
            },
            lng: {
              type: 'number',
              description: 'Longitude',
            },
            location: {
              type: 'string',
              description: 'Name of the location',
            },
          },
          required: ['lat', 'lng', 'location'],
        },
      },
      async ({ lat, lng, location }: { [key: string]: any }) => {
        setMarker({ lat, lng, location });
        setCoords({ lat, lng, location });
        const result = await fetch(
          `https://api.open-meteo.com/v1/forecast?latitude=${lat}&longitude=${lng}&current=temperature_2m,wind_speed_10m`
        );
        const json = await result.json();
        const temperature = {
          value: json.current.temperature_2m as number,
          units: json.current_units.temperature_2m as string,
        };
        const wind_speed = {
          value: json.current.wind_speed_10m as number,
          units: json.current_units.wind_speed_10m as string,
        };
        setMarker({ lat, lng, location, temperature, wind_speed });
        return json;
      }
    );


 
    // Add more tools here following the same pattern


    
    const callRemoteTool = async (toolName: string, params: any) => {
      try {
        console.log(`Attempting to run ${toolName}:`, params);
        const response = await fetch('/realtime/genesis_tool', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            tool_name: toolName,
            bot_id: 'Janice',
            params: JSON.stringify(params)
          }),
        });

        if (!response.ok) {
          console.error('Server responded with an error:', response.status, response.statusText);
          throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();
        console.log('Tool response:', data);

        if (data.success) {
          if (toolName === 'run_query' && Array.isArray(data.results)) {
            setLastQueryResult(data.results);
          }

          let base64Image;
          if (data.results.base64_object) {
            base64Image = {
              filename: data.results.base64_object.filename,
              content: data.results.base64_object.content
            };
            // Remove the base64_object from the results
            delete data.results.base64_object;
          }

          // Set the graphImage if there's a base64Image
          if (base64Image) {
            setGraphImage(base64Image.content);
          }

          // Return the results without the base64_object
          return data.results;
        } else {
          console.error('Tool execution failed:', data.message);
          throw new Error(data.message || 'Tool execution failed');
        }
      } catch (error) {
        console.error('Error running tool:', error);
        if (error instanceof Error) {
          return `Error: ${error.message}`;
        } else {
          return `Error: An unknown error occurred`;
        }
      }
    };

    const fetchTools = async (): Promise<any[]> => {
      try {
        const response = await fetch(`/realtime/tools?bot_id=Janice`, {
          method: 'GET',
          headers: {
            'Content-Type': 'application/json',
          }
        });

        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        if (data.success && Array.isArray(data.tools)) {
          return data.tools.map((toolFunc: any) => {
            let name = toolFunc.function?.name || '';
            if (name.startsWith('_')) {
              name = name.slice(1);
            }
            return {
              name,
              description: toolFunc.function?.description || '',
              parameters: toolFunc.function?.parameters || { type: 'object', properties: {} }
            };
          });
        } else {
          console.error('Failed to fetch tools:', data.message || 'Unexpected response format');
          return [];
        }
      } catch (error) {
        console.error('Error fetching tools:', error);
        return [];
      }
    };

    fetchTools().then(tools => {
      tools.forEach((tool: any, index: number) => {
        if (!tool.name) {
          console.error(`Tool at index ${index} is missing a name:`, tool);
          return; // Skip this tool
        }
        try {
          client.addTool(
            {
              name: tool.name,
              description: tool.description || '',
              parameters: tool.parameters || { type: 'object', properties: {} }
            },
            async (params: { [key: string]: any }) => {
              return await callRemoteTool(tool.name, params);
            }
          );
          console.log(`Successfully added tool: ${tool.name}`);
        } catch (error) {
          console.error(`Error adding tool "${tool.name}":`, error);
        }
      });
    }).catch(error => {
      console.error('Error fetching tools:', error);
    });

    // handle realtime events from client + server for event logging
    client.on('realtime.event', (realtimeEvent: RealtimeEvent) => {
      setRealtimeEvents((realtimeEvents) => {
        const lastEvent = realtimeEvents[realtimeEvents.length - 1];
        if (lastEvent?.event.type === realtimeEvent.event.type) {
          // if we receive multiple events in a row, aggregate them for display purposes
          lastEvent.count = (lastEvent.count || 0) + 1;
          return realtimeEvents.slice(0, -1).concat(lastEvent);
        } else {
          return realtimeEvents.concat(realtimeEvent);
        }
      });
    });
    client.on('error', (event: any) => console.error(event));
    client.on('conversation.interrupted', async () => {
      const trackSampleOffset = await wavStreamPlayer.interrupt();
      if (trackSampleOffset?.trackId) {
        const { trackId, offset } = trackSampleOffset;
        await client.cancelResponse(trackId, offset);
      }
    });
    client.on('conversation.updated', async ({ item, delta }: any) => {
      const items = client.conversation.getItems();
      if (delta?.audio) {
        wavStreamPlayer.add16BitPCM(delta.audio, item.id);
      }
      if (item.status === 'completed' && item.formatted.audio?.length) {
        const wavFile = await WavRecorder.decode(
          item.formatted.audio,
          24000,
          24000
        );
        item.formatted.file = wavFile;
      }
      setItems(items);
    });

    setItems(client.conversation.getItems());

    // Set VAD as the default turn detection type
    client.updateSession({ turn_detection: { type: 'server_vad' } });
    client.updateSession({ voice: 'alloy' });
  

    return () => {
      // cleanup; resets to defaults
      client.reset();
    };
  }, []);

  // Use useCallback to memoize the function
  const renderToolCallResult = useCallback((result: any, toolName: string) => {
    console.log("Tool call result:", result);

    if (result && result.base64Image) {
      // If there's a base64 image, set it to the graphImage state
      setGraphImage(result.base64Image);
      return null; // Don't render anything here, it will be shown in the bottom panel
    }

    if (toolName === 'run_query' && Array.isArray(result)) {
      return <Table data={result} />;
    } else if (typeof result === 'object' && result !== null) {
      return <pre>{JSON.stringify(result, null, 2)}</pre>;
    } else {
      return <p>{String(result)}</p>;
    }
  }, [setGraphImage]); // Include setGraphImage in the dependency array

  const handleMouseDown = (e: React.MouseEvent) => {
    setIsResizing(true);
  };

  const handleMouseUp = () => {
    setIsResizing(false);
  };

  const handleMouseMove = useCallback(
    (e: MouseEvent) => {
      if (!isResizing || !rightPanelRef.current) return;
      const containerRect = rightPanelRef.current.getBoundingClientRect();
      const newSplitPosition = ((e.clientY - containerRect.top) / containerRect.height) * 100;
      setSplitPosition(Math.min(Math.max(newSplitPosition, 10), 90)); // Limit between 10% and 90%
    },
    [isResizing]
  );

  useEffect(() => {
    window.addEventListener('mousemove', handleMouseMove);
    window.addEventListener('mouseup', handleMouseUp);
    return () => {
      window.removeEventListener('mousemove', handleMouseMove);
      window.removeEventListener('mouseup', handleMouseUp);
    };
  }, [handleMouseMove]);

  useEffect(() => {
    if (graphImageRef.current && rightPanelRef.current) {
      const containerWidth = rightPanelRef.current.clientWidth;
      const containerHeight = rightPanelRef.current.clientHeight * (1 - splitPosition / 100) - 10; // Subtract handle height
      const img = graphImageRef.current;
      
      img.onload = () => {
        const aspectRatio = img.naturalWidth / img.naturalHeight;
        if (containerWidth / containerHeight > aspectRatio) {
          img.style.height = '100%';
          img.style.width = 'auto';
        } else {
          img.style.width = '100%';
          img.style.height = 'auto';
        }
      };
    }
  }, [graphImage, splitPosition]);

  /**
   * Render the application
   */
  return (
    <div data-component="ConsolePage">
      <div className="content-top">
        <div className="content-title">
          <img 
            src="https://i0.wp.com/genesiscomputing.ai/wp-content/uploads/2024/04/cropped-GenisGg.png?fit=676%2C676&ssl=1" 
            alt="Genesis Computing Logo"
            className="genesis-logo"
          />
          <span>Genesis Computing</span>
        </div>
        <div className="content-api-key">
          {!LOCAL_RELAY_SERVER_URL && (
            <Button
              icon={Edit}
              iconPosition="end"
              buttonStyle="flush"
              label={`api key: ${apiKey.slice(0, 3)}...`}
              onClick={() => resetAPIKey()}
            />
          )}
        </div>
      </div>
      <div className="content-main">
        <div className="content-logs">
          <div className="content-block events">
            <div className="visualization">
              <div className="visualization-entry client">
                <canvas ref={clientCanvasRef} />
              </div>
              <div className="visualization-entry server">
                <canvas ref={serverCanvasRef} />
              </div>
            </div>
            <div className="content-block-title">events</div>
            <div className="content-block-body" ref={eventsScrollRef}>
              {!realtimeEvents.length && `awaiting connection...`}
              {realtimeEvents.map((realtimeEvent, i) => {
                const count = realtimeEvent.count;
                const event = { ...realtimeEvent.event };
                if (event.type === 'input_audio_buffer.append') {
                  event.audio = `[trimmed: ${event.audio.length} bytes]`;
                } else if (event.type === 'response.audio.delta') {
                  event.delta = `[trimmed: ${event.delta.length} bytes]`;
                }
                return (
                  <div className="event" key={event.event_id}>
                    <div className="event-timestamp">
                      {formatTime(realtimeEvent.time)}
                    </div>
                    <div className="event-details">
                      <div
                        className="event-summary"
                        onClick={() => {
                          // toggle event details
                          const id = event.event_id;
                          const expanded = { ...expandedEvents };
                          if (expanded[id]) {
                            delete expanded[id];
                          } else {
                            expanded[id] = true;
                          }
                          setExpandedEvents(expanded);
                        }}
                      >
                        <div
                          className={`event-source ${
                            event.type === 'error'
                              ? 'error'
                              : realtimeEvent.source
                          }`}
                        >
                          {realtimeEvent.source === 'client' ? (
                            <ArrowUp />
                          ) : (
                            <ArrowDown />
                          )}
                          <span>
                            {event.type === 'error'
                              ? 'error!'
                              : realtimeEvent.source}
                          </span>
                        </div>
                        <div className="event-type">
                          {event.type}
                          {count && ` (${count})`}
                        </div>
                      </div>
                      {!!expandedEvents[event.event_id] && (
                        <div className="event-payload">
                          {JSON.stringify(event, null, 2)}
                        </div>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
          <div className="content-block conversation">
            <div className="content-block-title">conversation</div>
            <div className="content-block-body" data-conversation-content>
              {!items.length && `awaiting connection...`}
              {items.map((conversationItem, i) => {
                return (
                  <div className="conversation-item" key={conversationItem.id}>
                    <div className={`speaker ${conversationItem.role || ''}`}>
                      <div>
                        {(
                          conversationItem.role || conversationItem.type
                        ).replaceAll('_', ' ')}
                      </div>
                      <div
                        className="close"
                        onClick={() =>
                          deleteConversationItem(conversationItem.id)
                        }
                      >
                        <X />
                      </div>
                    </div>
                    <div className={`speaker-content`}>
                      {/* tool response */}
                      {conversationItem.type === 'function_call_output' && (
                        <div>{conversationItem.formatted.output}</div>
                      )}
                      {/* tool call */}
                      {!!conversationItem.formatted.tool && (
                        <div>
                          {conversationItem.formatted.tool.name}(
                          {conversationItem.formatted.tool.arguments})
                        </div>
                      )}
                      {!conversationItem.formatted.tool &&
                        conversationItem.role === 'user' && (
                          <div>
                            {conversationItem.formatted.transcript ||
                              (conversationItem.formatted.audio?.length
                                ? '(awaiting transcript)'
                                : conversationItem.formatted.text ||
                                  '(item sent)')}
                          </div>
                        )}
                      {!conversationItem.formatted.tool &&
                        conversationItem.role === 'assistant' && (
                          <div>
                            {conversationItem.formatted.transcript ||
                              conversationItem.formatted.text ||
                              '(truncated)'}
                          </div>
                        )}
                      {conversationItem.formatted.file && (
                        <audio
                          src={conversationItem.formatted.file.url}
                          controls
                        />
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
          <div className="content-actions">
            <Toggle
              defaultValue={true}
              labels={['manual', 'vad']}
              values={['none', 'server_vad']}
              onChange={(_, value) => changeTurnEndType(value)}
            />
            <div className="spacer" />
            {isConnected && canPushToTalk && (
              <Button
                label={isRecording ? 'release to send' : 'push to talk'}
                buttonStyle={isRecording ? 'alert' : 'regular'}
                disabled={!isConnected || !canPushToTalk}
                onMouseDown={startRecording}
                onMouseUp={stopRecording}
              />
            )}
            <div className="spacer" />
            <Button
              label={isConnected ? 'disconnect' : 'connect'}
              iconPosition={isConnected ? 'end' : 'start'}
              icon={isConnected ? X : Zap}
              buttonStyle={isConnected ? 'regular' : 'action'}
              onClick={
                isConnected ? disconnectConversation : connectConversation
              }
            />
          </div>
        </div>
        <div className="content-right" ref={rightPanelRef}>
          <div 
            className="content-block query-result"
            style={{ height: `${splitPosition}%`, overflow: 'auto' }}
          >

            <div className="content-block-body">
              {lastQueryResult ? (
                <Table data={lastQueryResult} />
              ) : (
                <p>No query results yet</p>
              )}
            </div>
          </div>
          <div 
            className="resize-handle"
            onMouseDown={handleMouseDown}
            style={{
              height: '10px',
              cursor: 'ns-resize',
              backgroundColor: '#ddd',
              borderTop: '1px solid #ccc',
              borderBottom: '1px solid #ccc',
            }}
          />
          <div 
            className="content-block graph-result"
            style={{ height: `calc(${100 - splitPosition}% - 10px)`, overflow: 'hidden' }}
          >
           
            <div className="content-block-body">
              {graphImage ? (
                <div style={{ width: '100%', height: '100%', display: 'flex', justifyContent: 'center', alignItems: 'center' }}>
                  <img 
                    ref={graphImageRef}
                    src={`data:image/png;base64,${graphImage}`} 
                    alt="Graph Output"
                    style={{ maxWidth: '100%', maxHeight: '100%', objectFit: 'contain' }}
                  />
                </div>
              ) : (
                <p>No graph output yet</p>
              )}
            </div>
          </div>
        </div>
      </div>
      {/* Add Helmet to define or update CSP */}

    </div>
  );
}