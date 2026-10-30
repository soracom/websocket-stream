'use strict'

const test = require('node:test')
const assert = require('node:assert/strict')
const { once } = require('node:events')
const { WebSocket, Receiver } = require('ws')
const websocket = require('../')
const { readFileSync } = require('node:fs')
const vm = require('node:vm')

async function pair(t, options = {}) {
  const server = websocket.createServer({ port: 0, host: '127.0.0.1', perMessageDeflate: false, ...options })
  await once(server, 'listening')
  const accepted = once(server, 'stream')
  const client = websocket('ws://127.0.0.1:' + server.address().port, { highWaterMark: 16384, perMessageDeflate: false })
  const [stream] = await accepted
  await once(client, 'connect')
  t.after(async () => {
    client.destroy()
    stream.destroy()
    client.socket.terminate()
    stream.socket.terminate()
    await new Promise(resolve => server.close(resolve))
  })
  return { client, stream, server }
}

test('text, Buffer and ArrayBuffer preserve bytes and existing listeners', { timeout: 5000 }, async t => {
  const { client, stream } = await pair(t)
  for (const data of ['text \u2603', Buffer.from([0, 255, 128, 1])]) {
    const received = once(stream, 'data')
    client.socket.send(data)
    assert.deepEqual((await received)[0], Buffer.from(data))
    const reply = once(client, 'data')
    stream.socket.send(data)
    assert.deepEqual((await reply)[0], Buffer.from(data))
  }
  let messages = 0
  const listener = () => messages++
  stream.socket.onmessage = listener
  const received = once(stream, 'data')
  client.write('still registered')
  assert.equal((await received)[0].toString(), 'still registered')
  assert.equal(messages, 1)
  assert.equal(stream.socket.onmessage, listener)
})

test('ErrorEvent forwards the original Error and code', { timeout: 5000 }, async t => {
  const { stream } = await pair(t)
  const error = Object.assign(new Error('test failure'), { code: 'TEST_ERROR' })
  const received = once(stream, 'error')
  stream.socket.emit('error', error)
  assert.equal((await received)[0], error)
})

test('4 MiB unread stream pauses and resumes without losing bytes', { timeout: 10000 }, async t => {
  const { stream, client } = await pair(t, { highWaterMark: 16384 })
  assert.equal(stream._readableState.highWaterMark, 16384)
  const bytes = 4 * 1024 * 1024
  let sent = 0
  const write = () => {
    while (sent < bytes) {
      sent += 16384
      if (!client.write(Buffer.alloc(16384, 0x5a))) {
        client.once('drain', write)
        return
      }
    }
  }
  write()
  await new Promise(resolve => setTimeout(resolve, 200))
  assert.equal(stream.socket.isPaused, true)
  assert(stream._readableState.length < bytes)
  assert(stream._readableState.length <= 128 * 1024)
  let received = 0
  await new Promise((resolve, reject) => {
    stream.on('data', data => {
      try {
        assert(data.equals(Buffer.alloc(data.length, 0x5a)))
        received += data.length
        if (received === bytes) resolve()
      } catch (error) { reject(error) }
    })
  })
  assert.equal(received, bytes)
})

test('closing an unread paused stream releases both peers', { timeout: 5000 }, async t => {
  const { stream, client } = await pair(t, { highWaterMark: 16 })
  client.write(Buffer.alloc(1024))
  await new Promise(resolve => setTimeout(resolve, 50))
  assert.equal(stream.socket.isPaused, true)
  const closed = once(client, 'close')
  stream.destroy()
  await closed
})

function receiver(options) {
  const r = new Receiver({ isServer: false, maxPayload: 1024 * 1024, ...options })
  const messages = []
  r.on('message', data => messages.push(data))
  // Each write is controlled here, independently of TCP packet coalescing.
  const write = data => new Promise(resolve => r.write(data, error => resolve(error)))
  r.on('error', () => {})
  return { r, write, messages }
}

for (const limit of [4, 16384]) {
  for (const count of [limit - 1, limit, limit + 1]) {
    test('fragment boundary ' + count + '/' + limit, async () => {
      const { r, write, messages } = receiver({ maxFragments: limit })
      let error
      for (let i = 0; i < count; i++) {
        // Alternate empty and non-empty fragments, including final continuation.
        const empty = i % 2 === 0
        error = await write(Buffer.from([(i === count - 1 ? 0x80 : 0) | (i === 0 ? 2 : 0), empty ? 0 : 1, ...empty ? [] : [i % 251]]))
        if (error) break
      }
      if (count > limit) {
        assert.equal(error.code, 'WS_ERR_TOO_MANY_BUFFERED_PARTS')
        assert.equal(messages.length, 0)
      } else {
        assert.equal(error == null, true)
        assert.deepEqual(messages, [Buffer.from(Array.from({ length: count }, (_, i) => i).filter(i => i % 2).map(i => i % 251))])
      }
      r.destroy()
    })
  }
}

for (const limit of [4, 262144]) {
  for (const count of [limit - 1, limit, limit + 1]) {
    test('buffered chunk boundary ' + count + '/' + limit, async () => {
      const { r, write, messages } = receiver({ maxBufferedChunks: limit })
      const header = Buffer.alloc(10)
      header[0] = 0x82
      header[1] = 127
      header.writeBigUInt64BE(BigInt(count), 2)
      await write(header)
      let error
      for (let i = 0; i < count; i++) {
        error = await write(Buffer.from([i % 251]))
        if (error) break
      }
      if (count > limit) {
        assert.equal(error.code, 'WS_ERR_TOO_MANY_BUFFERED_PARTS')
        assert.equal(messages.length, 0)
      } else {
        assert.equal(error == null, true)
        assert.deepEqual(messages, [Buffer.from(Array.from({ length: count }, (_, i) => i % 251))])
      }
      r.destroy()
    })
  }
}

test('browser EventTarget listeners coexist and normalize errors', async () => {
  const context = {
    require, module: { exports: {} }, process: { title: 'browser' }, global: {},
    setTimeout, ArrayBuffer
  }
  vm.runInNewContext(readFileSync(require.resolve('../stream'), 'utf8'), context)
  const listeners = new Map()
  const socket = {
    OPEN: 1, readyState: 1, bufferedAmount: 0,
    onmessage: () => {},
    addEventListener: (name, cb) => listeners.set(name, cb),
    send: () => {}, close: () => {}
  }
  const prior = socket.onmessage
  const stream = context.module.exports(socket)
  assert.equal(socket.onmessage, prior)
  const data = once(stream, 'data')
  listeners.get('message')({ data: 'browser text' })
  assert.equal((await data)[0].toString(), 'browser text')
  const original = Buffer.from([0, 255, 1])
  const binary = once(stream, 'data')
  listeners.get('message')({ data: original })
  assert.equal((await binary)[0], original)
  const error = once(stream, 'error')
  listeners.get('error')({ error: { message: 'browser failure', code: 'TEST' } })
  assert.equal((await error)[0].code, 'TEST')
})

test('fragment rejection sends 1008 and leaves the server usable', { timeout: 5000 }, async t => {
  const { client, stream, server } = await pair(t, { maxFragments: 4 })
  let forwarded = 0
  stream.on('data', data => { forwarded += data.length })
  const error = once(stream, 'error')
  const closed = once(client.socket, 'close')
  for (let i = 0; i < 5; i++) client.socket.send(Buffer.from([i]), { fin: i === 4 })
  assert.equal((await error)[0].code, 'WS_ERR_TOO_MANY_BUFFERED_PARTS')
  assert.equal((await closed)[0], 1008)
  assert.equal(forwarded, 0)
  const accepted = once(server, 'stream')
  const next = websocket('ws://127.0.0.1:' + server.address().port)
  const [peer] = await accepted
  const received = once(peer, 'data')
  next.write('after rejection')
  assert.equal((await received)[0].toString(), 'after rejection')
  next.destroy()
  peer.destroy()
})

test('truncated upgrade headers reject only the offending connection', { timeout: 5000 }, async () => {
  const http = require('node:http')
  const net = require('node:net')
  const server = http.createServer()
  server.maxHeadersCount = 10
  const wss = websocket.createServer({ server })
  wss.on('stream', stream => stream.pipe(stream))
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  const socket = net.connect(server.address().port, '127.0.0.1')
  const response = once(socket, 'data')
  socket.write('GET / HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\n' +
    Array.from({ length: 20 }, (_, i) => 'X-' + i + ': x\r\n').join('') +
    'Upgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n')
  assert.match((await response)[0].toString(), /^HTTP\/1.1 431/)
  socket.destroy()
  const client = websocket('ws://127.0.0.1:' + server.address().port)
  const echoed = once(client, 'data')
  client.write('healthy')
  assert.equal((await echoed)[0].toString(), 'healthy')
  client.destroy()
  await new Promise(resolve => wss.close(resolve))
  await new Promise(resolve => server.close(resolve))
})

for (const side of ['server', 'client']) {
  for (const mode of ['drain', 'end']) {
    test('closing backpressure on the ' + side + ' stream: ' + mode, { timeout: 10000 }, async () => {
      const { execFile } = require('node:child_process')
      const { promisify } = require('node:util')
      const { stdout } = await promisify(execFile)(process.execPath, [require.resolve('./closing-backpressure'), side, mode], { timeout: 7000 })
      const result = JSON.parse(stdout)
      assert.equal(result.side, side)
      assert.equal(result.mode, mode)
      if (mode === 'drain') {
        assert.equal(result.received, 8 * 1024 * 1024)
        assert(result.maxBuffered <= 4 * 1024 * 1024)
        assert(result.closingPauses > 0)
      } else assert(result.endedInMessage)
    })
  }

  test('reading a paused ' + side + ' stream after socket.close completes the handshake', { timeout: 10000 }, async () => {
    const { execFile } = require('node:child_process')
    const { promisify } = require('node:util')
    // The default WebSocket close timeout is 30 seconds; it must not be needed.
    const { stdout } = await promisify(execFile)(process.execPath, [require.resolve('./paused-close'), side], { timeout: 5000 })
    const result = JSON.parse(stdout)
    assert.equal(result.side, side)
    assert.equal(result.received, 2 * 1024 * 1024)
    assert(result.resumesWhileClosing > 0)
  })

  test('end on an unread paused ' + side + ' stream closes without an error listener', { timeout: 10000 }, async () => {
    const { execFile } = require('node:child_process')
    const { promisify } = require('node:util')
    const { stdout } = await promisify(execFile)(process.execPath, [require.resolve('./paused-end'), side], { timeout: 7000 })
    const result = JSON.parse(stdout)
    assert.equal(result.side, side)
    assert(result.messagesAfterFinish > 0)
  })
}

test('explicit null protocols preserve third-argument options', { timeout: 5000 }, async t => {
  const server = websocket.createServer({ host: '127.0.0.1', port: 0 })
  t.after(async () => {
    for (const socket of server.clients) socket.terminate()
    await new Promise(resolve => server.close(resolve))
  })
  await once(server, 'listening')
  const accepted = once(server, 'stream')
  const client = websocket('ws://127.0.0.1:' + server.address().port, null, {
    headers: { 'x-test-null-protocol': 'preserved' },
    highWaterMark: 16384
  })
  t.after(() => client.socket.terminate())
  const [peer, request] = await accepted
  assert.equal(request.headers['x-test-null-protocol'], 'preserved')
  assert.equal(peer.socket.protocol, '')
  peer.pipe(peer)
  const response = once(client, 'data')
  client.write('null protocols')
  assert.equal((await response)[0].toString(), 'null protocols')
  client.end()
})
