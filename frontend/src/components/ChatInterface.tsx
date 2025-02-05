import React, { useState, useRef, useEffect } from 'react';
import { Send, User, Bot, Plus, MessageSquare, X, Menu, ChevronLeft } from 'lucide-react';

interface Message {
  role: 'user' | 'assistant';
  content: string;
}

interface Thread {
  id: string;
  title: string;
  messages: Message[];
  createdAt: string;
}

const API_BASE_URL = 'http://localhost:8000';

const ChatInterface: React.FC = () => {
  const [threads, setThreads] = useState<Thread[]>([
    {
      id: '1',
      title: 'New chat',
      messages: [
        { role: 'assistant', content: 'Hello! How can I help you today?' }
      ],
      createdAt: new Date().toISOString()
    }
  ]);
  
  const [currentThreadId, setCurrentThreadId] = useState<string>('1');
  const [input, setInput] = useState<string>('');
  const [selectedBot, setSelectedBot] = useState<string>('');
  const [isSidebarOpen, setIsSidebarOpen] = useState<boolean>(true);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const [availableBots, setAvailableBots] = useState<string[]>([]);

  const currentThread = threads.find(t => t.id === currentThreadId);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [currentThread?.messages]);

  useEffect(() => {
    fetchAvailableBots();
  }, []);

  const fetchAvailableBots = async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/bots`);
      const bots = await response.json();
      setAvailableBots(bots);
    } catch (error) {
      console.error('Error fetching available bots:', error);
    }
  };

  const createNewThread = () => {
    const newThread: Thread = {
      id: Date.now().toString(),
      title: 'New chat',
      messages: [
        { role: 'assistant', content: 'Hello! How can I help you today?' }
      ],
      createdAt: new Date().toISOString()
    };
    setThreads(prev => [newThread, ...prev]);
    setCurrentThreadId(newThread.id);
    setInput('');
  };

  const deleteThread = (threadId: string) => {
    setThreads(prev => prev.filter(t => t.id !== threadId));
    if (currentThreadId === threadId) {
      const remainingThreads = threads.filter(t => t.id !== threadId);
      if (remainingThreads.length > 0) {
        setCurrentThreadId(remainingThreads[0].id);
      } else {
        createNewThread();
      }
    }
  };

  const updateThreadTitle = (threadId: string, firstUserMessage: string) => {
    setThreads(prev => prev.map(thread => {
      if (thread.id === threadId) {
        const title = firstUserMessage.split(' ').slice(0, 4).join(' ') + '...';
        return { ...thread, title };
      }
      return thread;
    }));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim() || !selectedBot) return;

    const userMessage: Message = { role: 'user', content: input };
    setThreads(prev =>
      prev.map(thread => {
        if (thread.id === currentThreadId) {
          return { ...thread, messages: [...thread.messages, userMessage] };
        }
        return thread;
      })
    );

    try {
      const threadId = currentThreadId || crypto.randomUUID();

      const response = await fetch(`${API_BASE_URL}/chat`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          bot_id: selectedBot,
          message: input,s
          thread_id: threadId
        })
      });

      if (response.ok) {
        const data = await response.json();
        const assistantMsg: Message = { role: 'assistant', content: data.response };
        setThreads(prev =>
          prev.map(thread => {
            if (thread.id === currentThreadId) {
              return { ...thread, messages: [...thread.messages, assistantMsg] };
            }
            return thread;
          })
        );
      } else {
        console.error('Failed to submit message');
      }
    } catch (error) {
      console.error('Error submitting message:', error);
    }

    setInput('');
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSubmit(e as unknown as React.FormEvent);
    }
  };

  const adjustTextareaHeight = () => {
    const textarea = textareaRef.current;
    if (textarea) {
      textarea.style.height = 'auto';
      textarea.style.height = `${Math.min(textarea.scrollHeight, 200)}px`;
    }
  };

  const formatDate = (dateString: string) => {
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', { 
      month: 'short', 
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  };

  return (
    <div className="flex h-screen bg-gray-50">
      {/* Mobile sidebar backdrop */}
      {isSidebarOpen && (
        <div 
          className="fixed inset-0 bg-black bg-opacity-50 z-20 md:hidden"
          onClick={() => setIsSidebarOpen(false)}
        />
      )}

      {/* Sidebar */}
      <aside 
        className={`fixed md:static inset-y-0 left-0 z-30 w-64 bg-gray-900 transform ${
          isSidebarOpen ? 'translate-x-0' : '-translate-x-full'
        } transition-transform duration-200 ease-in-out md:translate-x-0`}
      >
        <div className="flex flex-col h-full">
          {/* New Chat Button */}
          <div className="p-4">
            <button
              onClick={createNewThread}
              className="w-full flex items-center justify-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
            >
              <Plus size={20} />
              New Chat
            </button>
          </div>

          {/* Thread List */}
          <div className="flex-1 overflow-y-auto">
            {threads.map(thread => (
              <div
                key={thread.id}
                className={`group flex items-center gap-3 px-4 py-3 cursor-pointer hover:bg-gray-800 ${
                  currentThreadId === thread.id ? 'bg-gray-800' : ''
                }`}
                onClick={() => {
                  setCurrentThreadId(thread.id);
                  setIsSidebarOpen(false);
                }}
              >
                <MessageSquare size={18} className="text-gray-400" />
                <div className="flex-1 min-w-0">
                  <div className="text-sm text-gray-200 truncate">{thread.title}</div>
                  <div className="text-xs text-gray-400">{formatDate(thread.createdAt)}</div>
                </div>
                <button
                  onClick={(e: React.MouseEvent) => {
                    e.stopPropagation();
                    deleteThread(thread.id);
                  }}
                  className="opacity-0 group-hover:opacity-100 p-1 hover:bg-gray-700 rounded"
                >
                  <X size={16} className="text-gray-400" />
                </button>
              </div>
            ))}
          </div>

          {/* Bot Selector */}
          <div className="p-4">
            <label htmlFor="bot-select" className="block text-sm font-medium text-gray-700">
              Select a bot:
            </label>
            <select
              id="bot-select"
              value={selectedBot}
              onChange={(e) => setSelectedBot(e.target.value)}
              className="mt-1 block w-full pl-3 pr-10 py-2 text-base border-gray-300 focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm rounded-md"
            >
              <option value="">Select a bot</option>
              {availableBots.map((bot) => (
                <option key={bot} value={bot}>
                  {bot}
                </option>
              ))}
            </select>
          </div>
        </div>
      </aside>

      {/* Main Chat Area */}
      <main className="flex-1 flex flex-col w-0 md:w-auto">
        {/* Chat Header */}
        <header className="flex items-center px-4 py-3 border-b bg-white">
          <button
            onClick={() => setIsSidebarOpen(!isSidebarOpen)}
            className="md:hidden mr-4"
          >
            {isSidebarOpen ? <ChevronLeft size={24} /> : <Menu size={24} />}
          </button>
          <h1 className="text-lg font-semibold truncate">{currentThread?.title}</h1>
        </header>

        {/* Messages Container */}
        <div className="flex-1 overflow-y-auto p-4">
          <div className="max-w-3xl mx-auto space-y-4">
            {currentThread?.messages.map((message, index) => (
              <div
                key={index}
                className={`flex items-start gap-3 ${
                  message.role === 'user' ? 'justify-end' : ''
                }`}
              >
                {message.role === 'assistant' && (
                  <div className="w-8 h-8 rounded-full bg-blue-100 flex items-center justify-center">
                    <Bot size={20} className="text-blue-600" />
                  </div>
                )}
                
                <div
                  className={`max-w-[80%] rounded-lg p-3 ${
                    message.role === 'user'
                      ? 'bg-blue-600 text-white'
                      : 'bg-white border'
                  }`}
                >
                  {message.content}
                </div>

                {message.role === 'user' && (
                  <div className="w-8 h-8 rounded-full bg-gray-100 flex items-center justify-center">
                    <User size={20} className="text-gray-600" />
                  </div>
                )}
              </div>
            ))}
            <div ref={messagesEndRef} />
          </div>
        </div>

        {/* Input Area */}
        <div className="border-t bg-white p-4">
          <div className="max-w-3xl mx-auto">
            <form onSubmit={handleSubmit} className="flex items-end gap-2">
              <div className="flex-1 relative">
                <textarea
                  ref={textareaRef}
                  value={input}
                  onChange={(e) => {
                    setInput(e.target.value);
                    adjustTextareaHeight();
                  }}
                  onKeyDown={handleKeyDown}
                  placeholder="Type your message..."
                  className="w-full resize-none rounded-lg border p-3 pr-12 focus:outline-none focus:border-blue-500"
                  rows={1}
                />
                <button
                  type="submit"
                  className="absolute right-2 bottom-2 p-2 text-blue-600 hover:bg-blue-50 rounded-lg disabled:opacity-50"
                  disabled={!input.trim()}
                >
                  <Send size={20} />
                </button>
              </div>
            </form>
          </div>
        </div>
      </main>
    </div>
  );
};

export default ChatInterface;