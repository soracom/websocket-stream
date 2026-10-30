'use strict'

// Run each direction/size/rate in an isolated receiver process. The sender is
// always another process and produces one chunk at a time, respecting drain.
const { fork } = require('node:child_process')
const { once } = require('node:events')
const assert = require('node:assert/strict')
const { createHash } = require('node:crypto')
const fs = require('node:fs')
const websocket = require(process.env.STREAM_MODULE || '../')
const HWM = 16384

async function send(stream, bytes) {
  const hash = createHash('sha256')
  for (let offset = 0; offset < bytes; offset += HWM) {
    const data = Buffer.alloc(Math.min(HWM, bytes - offset), (offset / HWM) % 251)
    hash.update(data)
    if (!stream.write(data)) await once(stream, 'drain')
  }
  return hash.digest('hex')
}

async function main() {
  const [role, direction, size = '64', rate = '0', address] = process.argv.slice(2)
  const bytes = Number(size) * 1024 * 1024
  const options = { highWaterMark: HWM, perMessageDeflate: false }
  if (role === 'sender') {
    let stream, server
    if (direction === 'upload') {
      stream = websocket(address, options)
      await once(stream, 'connect')
    } else {
      server = websocket.createServer({ ...options, host: '127.0.0.1', port: 0 })
      const accepted = once(server, 'stream')
      await once(server, 'listening')
      process.send({ port: (server.address ? server.address() : server._server.address()).port })
      ;[stream] = await accepted
    }
    process.on('message', async message => {
      if (message === 'start') process.send({ hash: await send(stream, bytes) })
      if (message === 'stop') {
        stream.destroy()
        stream.socket.terminate()
        if (server) await new Promise(resolve => server.close(resolve))
        process.disconnect()
      }
    })
    process.send({ ready: true })
    return
  }
  let stream, server
  let url
  if (direction === 'upload') {
    server = websocket.createServer({ ...options, host: '127.0.0.1', port: 0 })
    await once(server, 'listening')
    url = 'ws://127.0.0.1:' + (server.address ? server.address() : server._server.address()).port
  }
  const accepted = server && once(server, 'stream')
  const child = fork(__filename, ['sender', direction, size, rate, url || ''], { stdio: ['ignore', 'inherit', 'inherit', 'ipc'] })
  let expectedHash
  const ready = new Promise(resolve => child.on('message', async message => {
    if (message.port) {
      stream = websocket('ws://127.0.0.1:' + message.port, options)
    }
    if (message.ready) resolve()
    if (message.hash) expectedHash = message.hash
  }))
  const exit = once(child, 'exit')
  await ready
  if (accepted) [stream] = await accepted
  const ws = stream.socket
  assert.equal(stream._readableState.highWaterMark, HWM)
  const startRSS = process.memoryUsage().rss
  const started = Date.now()
  const samples = []
  const sample = () => {
    const proxy = stream._readable && stream._readable._readableState ? stream._readable : null
    const receiver = ws._receiver
    const layers = {
      streamReadable: stream._readableState.length,
      streamWritable: stream._writableState.length,
      proxyReadable: proxy ? proxy._readableState.length : 0,
      proxyWritable: proxy ? proxy._writableState.length : 0,
      wsBufferedAmount: ws.bufferedAmount,
      receiverBuffered: receiver ? receiver._bufferedBytes : 0,
      receiverMessage: receiver ? receiver._totalPayloadLength : 0,
      socketReadable: ws._socket.readableLength || 0,
      socketWritable: ws._socket.writableLength || 0
    }
    samples.push({ ms: Date.now() - started, rss: process.memoryUsage().rss, paused: ws._socket.isPaused(), layers,
      total: Object.values(layers).reduce((a, b) => a + b, 0) })
  }
  const interval = setInterval(sample, 100)
  const hash = createHash('sha256')
  let received = 0
  const consume = data => { received += data.length; hash.update(data) }
  const slow = Number(rate) && setInterval(() => { const data = stream.read(65536); if (data) consume(data) }, 1000)
  child.send('start')
  await new Promise(resolve => setTimeout(resolve, size === '4' ? 1000 : 30000))
  if (slow) clearInterval(slow)
  const released = Date.now()
  await new Promise(resolve => {
    stream.on('data', data => { consume(data); if (received === bytes) resolve() })
    if (received === bytes) resolve()
  })
  while (!expectedHash) await new Promise(resolve => setImmediate(resolve))
  clearInterval(interval)
  sample()
  assert.equal(hash.digest('hex'), expectedHash)
  child.send('stop')
  stream.destroy()
  ws.terminate()
  if (server) await new Promise(resolve => server.close(resolve))
  const [code, signal] = await exit
  assert.equal(code, 0)
  assert.equal(signal, null)
  const stalled = samples.filter(s => s.ms <= (size === '4' ? 1000 : 30000))
  const at10 = stalled.find(s => s.ms >= 10000)
  const maxBuffer = Math.max(...stalled.map(s => s.total))
  const rssIncrease = Math.max(...samples.map(s => s.rss)) - startRSS
  const lateRSSIncrease = at10 ? Math.max(...stalled.filter(s => s.ms >= 10000).map(s => s.rss)) - at10.rss : 0
  const result = { direction, sizeMiB: Number(size), rateKiB: Number(rate), bytes: received, maxBuffer, rssIncrease, lateRSSIncrease,
    paused: stalled.some(s => s.paused), resumeMs: Date.now() - released, highWaterMark: HWM, samples }
  if (process.env.RESULT_FILE) fs.writeFileSync(process.env.RESULT_FILE, JSON.stringify(result))
  console.log(JSON.stringify({ ...result, samples: samples.length }))
  assert(result.paused, 'receive stop must propagate')
  assert(maxBuffer <= 4 * 1024 * 1024, 'buffer budget exceeded')
  assert(rssIncrease <= 64 * 1024 * 1024, 'RSS budget exceeded')
  assert(lateRSSIncrease <= 8 * 1024 * 1024, 'RSS did not plateau')
  assert(result.resumeMs <= 60000)
}

main().catch(error => { console.error(error); process.exitCode = 1 })
