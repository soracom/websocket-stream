'use strict'

const assert = require('node:assert/strict')
const { once } = require('node:events')
const websocket = require('../')

async function main() {
  const [side, mode = 'drain'] = process.argv.slice(2)
  const options = { highWaterMark: 16384, perMessageDeflate: false }
  const server = websocket.createServer({ ...options, host: '127.0.0.1', port: 0 })
  await once(server, 'listening')
  const accepted = once(server, 'stream')
  const client = websocket('ws://127.0.0.1:' + server.address().port, options)
  const connected = once(client, 'connect')
  const [peer] = await accepted
  await connected
  const receiver = side === 'client' ? client : peer
  const sender = receiver === client ? peer : client
  const ws = receiver.socket
  let firstPause, closingPause
  const paused = new Promise(resolve => { firstPause = resolve })
  const pausedWhileClosing = new Promise(resolve => { closingPause = resolve })
  const pause = ws.pause
  let closingPauses = 0
  ws.pause = function () {
    pause.call(this)
    firstPause()
    if (this.readyState === this.CLOSING) { closingPauses++; closingPause() }
  }
  let maxBuffered = 0
  let inMessage = false
  ws.prependListener('message', () => { inMessage = true })
  ws.on('message', () => {
    const proxy = receiver._readable && receiver._readable._readableState ? receiver._readable : null
    const buffered = receiver._readableState.length + (proxy ? proxy._readableState.length : 0)
    maxBuffered = Math.max(maxBuffered, buffered)
    inMessage = false
  })
  const bytes = 8 * 1024 * 1024
  const chunk = Buffer.alloc(16384, 0x5a)
  for (let sent = 0; sent < bytes; sent += chunk.length) sender.socket.send(chunk)
  await paused
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(ws.isPaused, true)

  const closed = Promise.all([once(ws, 'close'), once(sender.socket, 'close')])
  ws.close(1000, 'closing backpressure')
  let received = 0
  let stopped = false
  let endedInMessage = false
  receiver.on('data', data => {
    assert(data.equals(Buffer.alloc(data.length, 0x5a)))
    received += data.length
    // Buffered wrapper data may not resume the socket; wait for fresh wire data.
    if (mode === 'end' && inMessage && !endedInMessage) {
      endedInMessage = true
      receiver.pause()
      receiver.end()
    } else if (mode === 'drain' && inMessage && !stopped) {
      stopped = true
      receiver.pause()
    }
  })
  receiver.resume()
  if (mode === 'drain') {
    await Promise.race([
      pausedWhileClosing,
      closed.then(() => { throw new Error('Closed without restoring backpressure; buffered=' + maxBuffered) })
    ])
    await new Promise(resolve => setImmediate(resolve))
    assert.equal(ws.readyState, ws.CLOSING)
    assert.equal(ws.isPaused, true)
    assert(stopped, 'consumer must stop after reception resumes')
    assert(closingPauses > 0)
    assert(received < bytes)
    assert(maxBuffered <= 4 * 1024 * 1024, 'stream buffer budget exceeded')
    receiver.resume()
  }
  const results = await closed
  for (const [code, reason] of results) {
    assert.equal(code, 1000)
    assert.equal(reason.toString(), 'closing backpressure')
  }
  if (mode === 'drain') assert.equal(received, bytes)
  else assert(endedInMessage, 'end must run synchronously inside message delivery')
  await new Promise(resolve => server.close(resolve))
  console.log(JSON.stringify({ side, mode, received, maxBuffered, closingPauses, endedInMessage }))
}

// An error or stuck handshake must fail the isolated child, not be swallowed.
main().catch(error => { console.error(error); process.exitCode = 1 })
