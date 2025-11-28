import axios from 'axios';

const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:5000';

const client = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

export interface ChatMessage {
  message: string;
}

export interface ChatResponse {
  success: boolean;
  message?: string;
  chart_spec?: any;
  stats?: any;
  error?: string;
  query_type?: string;
}

export const sendMessage = async (message: string): Promise<ChatResponse> => {
  const response = await client.post<ChatResponse>('/api/chat/message', {
    message,
  });
  return response.data;
};

export default client;

