import React, { useState, useRef, useEffect } from 'react';
import ReactMarkdown from 'react-markdown';
import { sendMessage, ChatResponse } from '../../api/client';
import './ChatInterface.css';

interface Message {
  id: string;
  text: string;
  sender: 'user' | 'system';
  timestamp: Date;
  chartSpec?: any;
  stats?: any;
  error?: string;
}

const ChatInterface: React.FC = () => {
  const [messages, setMessages] = useState<Message[]>([
    {
      id: '1',
      text: 'Hello! I can help you analyze Columbia University CS papers. Try asking: "show me the number of papers by field" or "show me the number of papers by year".',
      sender: 'system',
      timestamp: new Date(),
    },
  ]);
  const [inputValue, setInputValue] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const handleSend = async () => {
    if (!inputValue.trim() || isLoading) return;

    const userMessage: Message = {
      id: Date.now().toString(),
      text: inputValue,
      sender: 'user',
      timestamp: new Date(),
    };

    setMessages((prev) => [...prev, userMessage]);
    setInputValue('');
    setIsLoading(true);

    try {
      const response: ChatResponse = await sendMessage(inputValue);

      const systemMessage: Message = {
        id: (Date.now() + 1).toString(),
        text: response.message || (response.success ? 'Analysis complete' : 'Error occurred'),
        sender: 'system',
        timestamp: new Date(),
        chartSpec: response.chart_spec,
        stats: response.stats,
        error: response.error,
      };

      setMessages((prev) => [...prev, systemMessage]);

      if (response.success && response.chart_spec) {
        const event = new CustomEvent('newChart', { detail: response.chart_spec });
        window.dispatchEvent(event);
      }
    } catch (error: any) {
      const errorMessage: Message = {
        id: (Date.now() + 1).toString(),
        text: `Error: ${error.message || 'Failed to process request'}`,
        sender: 'system',
        timestamp: new Date(),
        error: error.message,
      };
      setMessages((prev) => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  return (
    <div className="chat-interface">
      <div className="chat-messages">
        {messages.map((message) => (
          <div key={message.id} className={`message ${message.sender}`}>
            <div className="message-content">
              <div className="message-text">
                <ReactMarkdown>{message.text}</ReactMarkdown>
              </div>
              {message.error && (
                <div className="message-error">Error: {message.error}</div>
              )}
              {message.stats && Object.keys(message.stats).length > 0 && (
                <div className="message-stats">
                  <strong>Statistics:</strong>
                  <pre>{JSON.stringify(message.stats, null, 2)}</pre>
                </div>
              )}
            </div>
            <div className="message-time">
              {message.timestamp.toLocaleTimeString()}
            </div>
          </div>
        ))}
        {isLoading && (
          <div className="message system">
            <div className="message-content">
              <div className="loading">Processing...</div>
            </div>
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>
      <div className="chat-input">
        <input
          type="text"
          value={inputValue}
          onChange={(e) => setInputValue(e.target.value)}
          onKeyPress={handleKeyPress}
          placeholder="Ask a question about the data..."
          disabled={isLoading}
        />
        <button onClick={handleSend} disabled={isLoading || !inputValue.trim()}>
          Send
        </button>
      </div>
    </div>
  );
};

export default ChatInterface;

