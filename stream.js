'use strict'

var Transform = require('readable-stream').Transform
var duplexify = require('duplexify')
var WS = require('ws')
var Buffer = require('safe-buffer').Buffer

module.exports = WebSocketStream

function buildProxy (options, socketWrite, socketEnd) {
  var proxy = new Transform({
    objectMode: options.objectMode,
    highWaterMark: options.highWaterMark
  })

  proxy._write = socketWrite
  proxy._flush = socketEnd

  return proxy
}

function WebSocketStream(target, protocols, options) {
  var stream, socket
  var readableEnded = false

  var isBrowser = process.title === 'browser'
  var isNative = !!global.WebSocket
  var socketWrite = isBrowser ? socketWriteBrowser : socketWriteNode

  if (protocols && !Array.isArray(protocols) && 'object' === typeof protocols) {
    // accept the "options" Object as the 2nd argument
    options = protocols
    protocols = undefined

    if (typeof options.protocol === 'string' || Array.isArray(options.protocol)) {
      protocols = options.protocol;
    }
  }

  if (protocols === null) protocols = undefined

  if (!options) options = {}

  if (options.objectMode === undefined) {
    options.objectMode = !(options.binary === true || options.binary === undefined)
  }

  var proxy = buildProxy(options, socketWrite, socketEnd)

  if (!options.objectMode) {
    proxy._writev = writev
  }

  // browser only: sets the maximum socket buffer size before throttling
  var bufferSize = options.browserBufferSize || 1024 * 512

  // browser only: how long to wait when throttling
  var bufferTimeout = options.browserBufferTimeout || 1000

  // use existing WebSocket object that was passed in
  if (typeof target === 'object') {
    socket = target
  // otherwise make a new one
  } else {
    // special constructor treatment for native websockets in browsers, see
    // https://github.com/maxogden/websocket-stream/issues/82
    if (isNative && isBrowser) {
      socket = new WS(target, protocols)
    } else {
      socket = new WS(target, protocols, options)
    }

    socket.binaryType = 'arraybuffer'
  }
  
  // EventTarget listeners preserve handlers installed by the caller and ws.
  var eventListenerSupport = typeof socket.addEventListener === 'function'

  // was already open when passed in
  if (socket.readyState === socket.OPEN) {
    stream = proxy
  } else {
    stream = duplexify(undefined, undefined, options)
    if (!options.objectMode) {
      stream._writev = writev
    }
    
    if (eventListenerSupport) {
       socket.addEventListener('open', onopen)
    } else {
       socket.onopen = onopen
    }
  }

  stream.socket = socket

  if (eventListenerSupport) {
     socket.addEventListener('close', onclose)
     socket.addEventListener('error', onerror)
     socket.addEventListener('message', onmessage)
  } else {
     socket.onclose = onclose
     socket.onerror = onerror
     socket.onmessage = onmessage
  }

  // Preserve Transform's write/read coordination while propagating demand to ws.
  var read = proxy._read
  proxy._read = function (size) {
    read.call(this, size)
    // A direct socket.close() still needs reads to receive the peer's close frame.
    if (!this.destroyed && (socket.readyState === socket.OPEN || socket.readyState === socket.CLOSING) && typeof socket.resume === 'function') {
      socket.resume()
    }
  }

  proxy.on('close', destroy)

  var coerceToBuffer = !options.objectMode

  function socketWriteNode(chunk, enc, next) {
    // avoid errors, this never happens unless
    // destroy() is called
    if (socket.readyState !== socket.OPEN) {
      next()
      return
    }

    if (coerceToBuffer && typeof chunk === 'string') {
      chunk = Buffer.from(chunk, 'utf8')
    }
    socket.send(chunk, next)
  }

  function socketWriteBrowser(chunk, enc, next) {
    if (socket.bufferedAmount > bufferSize) {
      setTimeout(socketWriteBrowser, bufferTimeout, chunk, enc, next)
      return
    }

    if (coerceToBuffer && typeof chunk === 'string') {
      chunk = Buffer.from(chunk, 'utf8')
    }

    try {
      socket.send(chunk)
    } catch(err) {
      return next(err)
    }

    next()
  }

  function socketEnd(done) {
    // Completing _flush pushes EOF. Stop forwarding messages before resuming
    // the transport, which may still deliver data ahead of the close frame.
    readableEnded = true
    // Closing must make progress even when the consumer has stopped reading.
    if (typeof socket.resume === 'function') socket.resume()
    socket.close()
    done()
  }

  function onopen() {
    if (stream.destroyed) return destroy()
    stream.setReadable(proxy)
    stream.setWritable(proxy)
    stream.emit('connect')
  }

  function onclose() {
    stream.end()
    stream.destroy()
  }

  function onerror(event) {
    var err = event && event.error ? event.error : event
    if (!(err instanceof Error)) {
      var normalized = new Error(err && err.message ? err.message : String(err))
      if (err && err.code !== undefined) normalized.code = err.code
      err = normalized
    }
    stream.destroy(err)
  }

  function onmessage(event) {
    if (readableEnded || stream.destroyed) return
    var data = event.data
    if (!Buffer.isBuffer(data)) {
      if (data instanceof ArrayBuffer) data = Buffer.from(data)
      else data = Buffer.from(data, 'utf8')
    }
    // push() may synchronously call a data handler which ends the stream.
    // Do not undo socketEnd's resume once local EOF has been committed.
    if (!proxy.push(data) && !readableEnded && !stream.destroyed && (socket.readyState === socket.OPEN || socket.readyState === socket.CLOSING) && typeof socket.pause === 'function') socket.pause()
  }

  function destroy() {
    if (typeof socket.resume === 'function') socket.resume()
    socket.close()
  }

  // this is to be enabled only if objectMode is false
  function writev (chunks, cb) {
    var buffers = new Array(chunks.length)
    for (var i = 0; i < chunks.length; i++) {
      if (typeof chunks[i].chunk === 'string') {
        buffers[i] = Buffer.from(chunks[i], 'utf8')
      } else {
        buffers[i] = chunks[i].chunk
      }
    }

    this._write(Buffer.concat(buffers), 'binary', cb)
  }

  return stream
}
