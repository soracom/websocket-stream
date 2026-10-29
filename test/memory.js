'use strict'

const { fork } = require('node:child_process')
const { once } = require('node:events')
const assert = require('node:assert/strict')
const { createHash } = require('node:crypto')
const websocket = require('../')
const maxPayload = 100 * 1024 * 1024
const connections = Number(process.argv[2] || 4)

async function main() {
  if (process.argv[3]) {
    const clients = []
    for (let i = 0; i < connections; i++) {
      const client = websocket(process.argv[3], { perMessageDeflate: false, highWaterMark: 16384 })
      await once(client, 'connect')
      clients.push(client)
    }
    process.on('message', message => {
      if (message === 'start') for (const client of clients) client.write(Buffer.alloc(maxPayload, 0x5a))
      if (message === 'stop') {
        for (const client of clients) { client.destroy(); client.socket.terminate() }
        process.disconnect()
      }
    })
    process.send('ready')
    return
  }
  const server = websocket.createServer({ host: '127.0.0.1', port: 0, maxPayload, highWaterMark: 16384, perMessageDeflate: false })
  const peers = []
  const complete = []
  server.on('stream', stream => { peers.push(stream); complete.push(once(stream.socket, 'message')) })
  await once(server, 'listening')
  const child = fork(__filename, [String(connections), 'ws://127.0.0.1:' + server.address().port])
  const exit = once(child, 'exit')
  await once(child, 'message')
  if (global.gc) global.gc()
  const baseline = process.memoryUsage().rss
  let peak = baseline
  const sample = setInterval(() => { peak = Math.max(peak, process.memoryUsage().rss) }, 10)
  child.send('start')
  await Promise.all(complete)
  peak = Math.max(peak, process.memoryUsage().rss)
  for (const peer of peers) {
    assert(peer.socket.isPaused)
    assert.equal(peer._readableState.length, maxPayload)
    const hash = createHash('sha256')
    const expected = createHash('sha256')
    for (let i = 0; i < maxPayload / 16384; i++) expected.update(Buffer.alloc(16384, 0x5a))
    hash.update(peer.read())
    assert.equal(hash.digest('hex'), expected.digest('hex'))
  }
  clearInterval(sample)
  child.send('stop')
  for (const peer of peers) { peer.destroy(); peer.socket.terminate() }
  await new Promise(resolve => server.close(resolve))
  assert.deepEqual(await exit, [0, null])
  // Payload fragments + contiguous reconstruction, per-connection stream/socket
  // allowance, and a fixed allocator allowance; this is not a deployment limit.
  const budget = connections * (2 * maxPayload + 4 * 1024 * 1024) + 64 * 1024 * 1024
  console.log(JSON.stringify({ connections, maxPayload, baseline, peak, increase: peak - baseline, budget }))
  assert(peak - baseline <= budget)
}
main().catch(error => { console.error(error); process.exitCode = 1 })
