'use strict'

// No stream error handlers or forced process.exit(): an error after EOF must
// fail the child, and a stuck close handshake must fail the parent's timeout.
const assert = require('node:assert/strict')
const { once } = require('node:events')
const websocket = require('../')

async function main() {
  const server = websocket.createServer({ host: '127.0.0.1', port: 0, highWaterMark: 16384, perMessageDeflate: false })
  await once(server, 'listening')
  const accepted = new Promise(resolve => server.once('stream', resolve))
  const client = websocket('ws://127.0.0.1:' + server.address().port, { highWaterMark: 16384, perMessageDeflate: false })
  const peer = await accepted
  await new Promise(resolve => client.once('connect', resolve))
  const receiver = process.argv[2] === 'client' ? client : peer
  const sender = receiver === peer ? client : peer
  const paused = new Promise(resolve => {
    const pause = receiver.socket.pause
    receiver.socket.pause = function () {
      pause.call(this)
      resolve()
    }
  })
  const closed = Promise.all([client, peer].map(stream => new Promise(resolve => stream.once('close', resolve))))
  let finished = false
  let messagesAfterFinish = 0
  receiver.once('finish', () => { finished = true })
  receiver.socket.on('message', () => { if (finished) messagesAfterFinish++ })
  const chunk = Buffer.alloc(16384, 0x5a)
  for (let sent = 0; sent < 2 * 1024 * 1024; sent += chunk.length) sender.socket.send(chunk)
  await paused
  assert.equal(receiver.socket.isPaused, true)
  assert.equal(receiver._readableState.highWaterMark, 16384)
  assert(receiver._readableState.length < 2 * 1024 * 1024)
  assert.equal(receiver.listenerCount('error'), 0)
  receiver.end()
  receiver.end() // Repeated local end must remain harmless.
  await closed
  assert(finished)
  assert(messagesAfterFinish > 0, 'exercise inbound messages after the local EOF')
  assert.equal(client.socket.readyState, client.socket.CLOSED)
  assert.equal(peer.socket.readyState, peer.socket.CLOSED)
  await new Promise(resolve => server.close(resolve))
  console.log(JSON.stringify({ side: process.argv[2], messagesAfterFinish }))
}

main().catch(error => { console.error(error); process.exitCode = 1 })
