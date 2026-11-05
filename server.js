const express = require('express');
const http = require('http');
const path = require('path');
const { WebSocketServer } = require('ws');

const app = express();
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const MAX_PARTICIPANTS = 10;
// ws -> { id, name }   (id = 0..9, ovoz paketlarida ishlatiladi; faqat xotirada, hech qayerga saqlanmaydi)
const participants = new Map();

const send = (ws, message) => {
  if (ws.readyState === 1) ws.send(JSON.stringify(message));
};

function freeId() {
  const used = new Set(Array.from(participants.values()).map(p => p.id));
  for (let i = 0; i < MAX_PARTICIPANTS; i++) if (!used.has(i)) return i;
  return -1;
}

function broadcast() {
  const members = Array.from(participants.values())
    .sort((a, b) => a.id - b.id)
    .map(p => ({ id: p.id, name: p.name }));
  wss.clients.forEach(ws => {
    const me = participants.get(ws);
    send(ws, {
      type: 'state',
      count: members.length,
      maxParticipants: MAX_PARTICIPANTS,
      members,
      inside: !!me,
      participantId: me ? me.id : null
    });
  });
}

wss.on('connection', ws => {
  ws.isAlive = true;
  ws.on('pong', () => (ws.isAlive = true));
  broadcast();

  ws.on('message', (data, isBinary) => {
    // Ovoz: gaplashayotgan odamdan -> xonadagi boshqa hammaga
    if (isBinary) {
      const me = participants.get(ws);
      if (!me || data.length <= 2 || data.length > 65536) return;
      const packet = Buffer.from(data);
      packet[0] = me.id; // kim gapirayotgani
      packet[1] = 0;
      participants.forEach((info, client) => {
        if (client !== ws && client.readyState === 1 && client.bufferedAmount < 1e6) {
          client.send(packet, { binary: true });
        }
      });
      return;
    }

    let message;
    try { message = JSON.parse(data.toString()); } catch { return; }

    if (message.type === 'join') {
      const name = typeof message.name === 'string'
        ? message.name.replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, 24)
        : '';
      if (participants.has(ws)) {
        send(ws, { type: 'join_result', ok: true });
      } else if (!name) {
        send(ws, { type: 'join_result', ok: false, reason: 'Ismingizni kiriting.' });
      } else if (participants.size >= MAX_PARTICIPANTS) {
        send(ws, { type: 'join_result', ok: false, reason: 'Gaplashish xonasi to\u2018la (10 kishi).' });
      } else {
        participants.set(ws, { id: freeId(), name });
        send(ws, { type: 'join_result', ok: true });
      }
    } else if (message.type === 'leave') {
      participants.delete(ws);
    }

    broadcast();
  });

  ws.on('close', () => {
    participants.delete(ws);
    broadcast();
  });
});

// o'lik ulanishlarni tozalash
setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('TUZAKI ishga tushdi: ' + PORT));
