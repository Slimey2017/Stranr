const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  pingTimeout: 60000,
  pingInterval: 25000
});

app.use(express.static(path.join(__dirname, 'public')));

// ── State ──────────────────────────────────────────
const waiting = [];          // queue of sockets waiting for a partner
const pairs = new Map();     // socketId -> partnerSocketId
const usernames = new Map(); // socketId -> username
const onlineCount = { n: 0 };

function broadcast_count() {
  io.emit('online_count', onlineCount.n);
}

// ── Matchmaking ────────────────────────────────────
function tryMatch(socket) {
  // Remove any stale sockets from queue
  while (waiting.length > 0 && !io.sockets.sockets.get(waiting[0].id)) {
    waiting.shift();
  }

  if (waiting.length === 0) {
    waiting.push(socket);
    socket.emit('status', { state: 'waiting', msg: 'searching for a stranger...' });
    return;
  }

  const partner = waiting.shift();
  if (partner.id === socket.id) {
    waiting.push(socket);
    socket.emit('status', { state: 'waiting', msg: 'searching for a stranger...' });
    return;
  }

  // Pair them
  pairs.set(socket.id, partner.id);
  pairs.set(partner.id, socket.id);

  socket.emit('matched', { partnerName: 'stranger' });
  partner.emit('matched', { partnerName: 'stranger' });
}

// ── Socket events ──────────────────────────────────
io.on('connection', (socket) => {
  onlineCount.n++;
  broadcast_count();

  socket.on('join', ({ username }) => {
    const safe = String(username || 'anon').replace(/[<>]/g, '').slice(0, 20) || 'anon';
    usernames.set(socket.id, safe);
    tryMatch(socket);
  });

  socket.on('message', ({ text }) => {
    const partner = pairs.get(socket.id);
    if (!partner) return;
    const partnerSocket = io.sockets.sockets.get(partner);
    if (!partnerSocket) return;
    const safe = String(text || '').replace(/</g, '&lt;').replace(/>/g, '&gt;').slice(0, 300);
    if (!safe.trim()) return;
    partnerSocket.emit('message', { text: safe });
  });

  socket.on('skip', () => {
    disconnectPair(socket, 'stranger disconnected.');
    tryMatch(socket);
  });

  socket.on('disconnect', () => {
    onlineCount.n = Math.max(0, onlineCount.n - 1);
    broadcast_count();
    usernames.delete(socket.id);
    // Remove from waiting queue
    const idx = waiting.findIndex(s => s.id === socket.id);
    if (idx !== -1) waiting.splice(idx, 1);
    // Notify partner
    disconnectPair(socket, 'stranger disconnected.');
  });

  function disconnectPair(sock, msg) {
    const partnerId = pairs.get(sock.id);
    if (partnerId) {
      pairs.delete(sock.id);
      pairs.delete(partnerId);
      const partnerSocket = io.sockets.sockets.get(partnerId);
      if (partnerSocket) {
        partnerSocket.emit('partner_left', { msg });
      }
    }
  }
});

// ── Health check ───────────────────────────────────
app.get('/health', (_, res) => res.json({ ok: true, online: onlineCount.n }));

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`STRANR running on port ${PORT}`));
