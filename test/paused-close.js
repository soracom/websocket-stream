'use strict'

const assert = require('node:assert/strict')
const { once } = require('node:events')
const websocket = require('../')

async function main() {
  const options = { highWaterMark: 16384, perMessageDeflate: false }
  const server = websocket.createServer({ ...options, host: '127.0.0.1', port: 0 })
  await once(server, 'listening')
  const accepted = once(server, 'stream')
  const client = websocket('ws://127.0.0.1:' + server.address().port, options)
  const connected = once(client, 'connect')
  const [peer] = await accepted
  await connected
  const receiver = process.argv[2] === 'client' ? client : peer
  const sender = receiver === client ? peer : client
  const paused = new Promise(resolve => {
    const pause = receiver.socket.pause
    receiver.socket.pause = function () { pause.call(this); resolve() }
  })
  const bytes = 2 * 1024 * 1024
  const chunk = Buffer.alloc(16384, 0x5a)
  for (let sent = 0; sent < bytes; sent += chunk.length) sender.socket.send(chunk)
  await paused
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(receiver.socket.isPaused, true)

  let resumesWhileClosing = 0
  const resume = receiver.socket.resume
  receiver.socket.resume = function () {
    if (this.readyState === this.CLOSING) resumesWhileClosing++
    return resume.call(this)
  }
  const closed = Promise.all([once(receiver.socket, 'close'), once(sender.socket, 'close')])
  const started = Date.now()
  // Bypass stream.end()/destroy(): only a subsequent read can resume reception.
  receiver.socket.close(1000, 'closing regression')
  assert.equal(receiver.socket.readyState, receiver.socket.CLOSING)
  let received = 0
  receiver.on('data', data => {
    assert(data.equals(Buffer.alloc(data.length, 0x5a)))
    received += data.length
  })
  receiver.resume()
  const results = await closed
  for (const [code, reason] of results) {
    assert.equal(code, 1000)
    assert.equal(reason.toString(), 'closing regression')
  }
  assert(resumesWhileClosing > 0)
  assert.equal(received, bytes)
  await new Promise(resolve => server.close(resolve))
  console.log(JSON.stringify({ side: process.argv[2], received, resumesWhileClosing, elapsedMs: Date.now() - started }))
}

// No forced exit or stream error handlers: the parent requires natural exit.
main().catch(error => { console.error(error); process.exitCode = 1 })
