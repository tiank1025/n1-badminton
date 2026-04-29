const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(path.join(__dirname, 'public')));

const COURT_IDS = [6, 7, 8, 9, 10];
const LEVELS = ['A', 'B', 'C', 'D+', 'D', 'D-', 'E+', 'E', 'E-'];

const state = {
  courts: Object.fromEntries(COURT_IDS.map(id => [id, { players: [], playing: false }])),
  queue: [],    // string[] of names
  players: {},  // { [name]: level } — persistent level registry
};

let connectedCount = 0;

function broadcast() {
  io.emit('state_update', state);
}

function sanitize(name) {
  return String(name || '').trim().replace(/[<>"'&]/g, '').slice(0, 20);
}

function isNameUsed(name) {
  if (state.queue.includes(name)) return true;
  return COURT_IDS.some(id => state.courts[id].players.includes(name));
}

function lvlIdx(name) {
  const i = LEVELS.indexOf(state.players[name]);
  return i >= 0 ? i : 4; // default to D if unknown
}

// Fill court from queue using smart level-matching:
// First player is always taken in queue order.
// Each subsequent slot scans the queue in order and picks the first person
// whose level keeps the court's total spread within 3 tiers.
// If no compatible player is found, falls back to strict queue order.
function smartFill(courtId) {
  const court = state.courts[courtId];
  if (!court) return;
  const needed = 4 - court.players.length;
  if (needed <= 0 || state.queue.length === 0) return;

  const pool = [...state.queue];
  const chosen = [];

  for (let slot = 0; slot < needed && pool.length > 0; slot++) {
    if (chosen.length === 0) {
      chosen.push(pool.shift());
    } else {
      const lvls = chosen.map(lvlIdx);
      const minL = Math.min(...lvls);
      const maxL = Math.max(...lvls);

      let foundIdx = -1;
      for (let i = 0; i < pool.length; i++) {
        const l = lvlIdx(pool[i]);
        if (Math.max(maxL, l) - Math.min(minL, l) <= 3) {
          foundIdx = i;
          break;
        }
      }
      // Fall back to queue order if nobody fits within 3 tiers
      if (foundIdx === -1) foundIdx = 0;
      chosen.push(...pool.splice(foundIdx, 1));
    }
  }

  const chosenSet = new Set(chosen);
  state.queue = state.queue.filter(n => !chosenSet.has(n));
  court.players.push(...chosen);
}

io.on('connection', (socket) => {
  connectedCount++;
  io.emit('connected_count', connectedCount);
  socket.emit('state_update', state);

  socket.on('disconnect', () => {
    connectedCount--;
    io.emit('connected_count', connectedCount);
  });

  socket.on('join_queue', ({ name, level }) => {
    name = sanitize(name);
    if (!name || isNameUsed(name)) return;
    if (LEVELS.includes(level)) state.players[name] = level;
    state.queue.push(name);
    broadcast();
  });

  socket.on('remove_from_queue', (name) => {
    name = sanitize(name);
    state.queue = state.queue.filter(n => n !== name);
    broadcast();
  });

  socket.on('fill_court', (courtId) => {
    courtId = Number(courtId);
    smartFill(courtId);
    broadcast();
  });

  socket.on('manual_assign', ({ courtId, names }) => {
    courtId = Number(courtId);
    const court = state.courts[courtId];
    if (!court) return;
    const spots = 4 - court.players.length;
    const toAdd = (names || [])
      .map(n => sanitize(n))
      .filter(n => n && state.queue.includes(n) && !court.players.includes(n))
      .slice(0, spots);
    const added = new Set(toAdd);
    state.queue = state.queue.filter(n => !added.has(n));
    court.players.push(...toAdd);
    broadcast();
  });

  socket.on('assign_player', ({ courtId, name }) => {
    courtId = Number(courtId);
    name = sanitize(name);
    const court = state.courts[courtId];
    if (!court || court.players.length >= 4 || court.players.includes(name)) return;
    state.queue = state.queue.filter(n => n !== name);
    court.players.push(name);
    broadcast();
  });

  socket.on('remove_from_court', ({ courtId, name }) => {
    courtId = Number(courtId);
    name = sanitize(name);
    const court = state.courts[courtId];
    if (!court) return;
    court.players = court.players.filter(n => n !== name);
    if (court.players.length < 4) court.playing = false;
    broadcast();
  });

  socket.on('start_game', (courtId) => {
    courtId = Number(courtId);
    const court = state.courts[courtId];
    if (court && court.players.length === 4) {
      court.playing = true;
      broadcast();
    }
  });

  socket.on('end_game', ({ courtId, toQueue }) => {
    courtId = Number(courtId);
    const court = state.courts[courtId];
    if (!court) return;
    if (toQueue) state.queue.push(...court.players);
    court.players = [];
    court.playing = false;
    broadcast();
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🏸  N1 Badminton Social 已启动`);
  console.log(`   本机访问: http://localhost:${PORT}`);
  console.log(`   局域网访问: http://<你的IP>:${PORT}\n`);
});
