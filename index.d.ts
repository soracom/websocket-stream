// Type definitions for websocket-stream 5.3
// Project: https://github.com/maxogden/websocket-stream#readme
// Original definitions by: Ben Burns <https://github.com/benjamincburns>

import WebSocket = require('ws');
import { Duplex, DuplexOptions } from 'stream';
import * as http from 'http';

declare namespace WebSocketStream {
  interface ReceiveLimits {
    maxFragments?: number;
    maxBufferedChunks?: number;
  }
  type ClientOptions = WebSocket.ClientOptions & DuplexOptions & ReceiveLimits & {
    binary?: boolean;
    browserBufferSize?: number;
    browserBufferTimeout?: number;
  };
  type ServerOptions = WebSocket.ServerOptions & DuplexOptions & ReceiveLimits;
  type WebSocketDuplex = Duplex & { socket: WebSocket };

  class Server extends WebSocket.Server {
    constructor(opts?: ServerOptions, callback?: (stream: WebSocketDuplex, request: http.IncomingMessage) => void);
    on(event: 'connection', cb: (this: WebSocket.Server, socket: WebSocket, request: http.IncomingMessage) => void): this;
    on(event: 'error', cb: (this: WebSocket.Server, error: Error) => void): this;
    on(event: 'headers', cb: (this: WebSocket.Server, headers: string[], request: http.IncomingMessage) => void): this;
    on(event: 'listening', cb: (this: WebSocket.Server) => void): this;
    on(event: 'stream', cb: (this: WebSocket.Server, stream: WebSocketDuplex, request: http.IncomingMessage) => void): this;
    on(event: string | symbol, listener: (this: WebSocket.Server, ...args: any[]) => void): this;
  }

  function createServer(opts?: ServerOptions, callback?: (stream: WebSocketDuplex, request: http.IncomingMessage) => void): Server;
}

declare function WebSocketStream(target: string | WebSocket, options?: WebSocketStream.ClientOptions): WebSocketStream.WebSocketDuplex;
declare function WebSocketStream(target: string | WebSocket, protocols?: string | string[] | null, options?: WebSocketStream.ClientOptions): WebSocketStream.WebSocketDuplex;

export = WebSocketStream;
