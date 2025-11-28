import React from 'react';
import './App.css';
import ChatInterface from './components/ChatInterface/ChatInterface';
import Dashboard from './components/Dashboard/Dashboard';

function App() {
  return (
    <div className="App">
      <header className="App-header">
        <h1>SciSciNet Data Analysis Chat</h1>
        <p className="author">Work by Wangshu Zhu</p>
      </header>
      <div className="App-content">
        <ChatInterface />
        <Dashboard />
      </div>
    </div>
  );
}

export default App;
