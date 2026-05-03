const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(path.join(__dirname, 'public')));

const COURT_IDS = [6, 7, 8, 9, 10];
const LEVELS = ['advanced', 'intermediate', 'novice', 'beginner'];

const state = {
  courts: Object.fromEntries(COURT_IDS.map(id => [id, { players: [], playing: false }])),
  next:   Object.fromEntries(COURT_IDS.map(id => [id, { players: [] }])),
  queue:   [],
  players: {},
  pairs:   {}, // pairs[name] = partnerName (mutual)
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
  if (COURT_IDS.some(id => state.courts[id].players.includes(name))) return true;
  if (COURT_IDS.some(id => state.next[id].players.includes(name))) return true;
  return false;
}

function lvlIdx(name) {
  const i = LEVELS.indexOf(state.players[name]);
  return i >= 0 ? i : 2;
}

function shiftNextSlots() {
  const filled = COURT_IDS.map(id => state.next[id].players).filter(p => p.length > 0);
  COURT_IDS.forEach((id, i) => {
    state.next[id].players = filled[i] ? [...filled[i]] : [];
  });
}

function smartFillInto(targetPlayers) {
  const needed = 4 - targetPlayers.length;
  if (needed <= 0 || state.queue.length === 0) return;

  const pool = [...state.queue];
  const chosen = [];

  function tryGrabPair(name) {
    const partner = state.pairs[name];
    if (partner && chosen.length < needed) {
      const pIdx = pool.indexOf(partner);
      if (pIdx !== -1) {
        pool.splice(pIdx, 1);
        chosen.push(partner);
      }
    }
  }

  for (let slot = 0; slot < needed && pool.length > 0; slot++) {
    if (chosen.length === 0) {
      const first = pool.shift();
      chosen.push(first);
      tryGrabPair(first);
      if (chosen.length > 1) slot++;
    } else {
      const lvls = chosen.map(lvlIdx);
      const minL = Math.min(...lvls);
      const maxL = Math.max(...lvls);

      let foundIdx = -1;
      for (let i = 0; i < pool.length; i++) {
        const l = lvlIdx(pool[i]);
        if (Math.max(maxL, l) - Math.min(minL, l) <= 1) { foundIdx = i; break; }
      }
      if (foundIdx === -1) foundIdx = 0;
      const next = pool.splice(foundIdx, 1)[0];
      chosen.push(next);
      tryGrabPair(next);
      if (chosen.length > slot + 1) slot++;
    }
  }

  const chosenSet = new Set(chosen);
  state.queue = state.queue.filter(n => !chosenSet.has(n));
  targetPlayers.push(...chosen);
}

io.on('connection', (socket) => {
  connectedCount++;
  io.emit('connected_count', connectedCount);
  socket.emit('state_update', state);

  socket.on('disconnect', () => {
    connectedCount--;
    io.emit('connected_count', connectedCount);
  });

  socket.on('join_queue', ({ name, level, partner }) => {
    name = sanitize(name);
    partner = sanitize(partner || '');
    if (!name || isNameUsed(name)) return;
    if (LEVELS.includes(level)) state.players[name] = level;
    state.queue.push(name);
    if (partner && partner !== name) {
      state.pairs[name] = partner;
      if (state.pairs[partner] === name) {
        // already mutual — confirmed
      } else {
        state.pairs[partner] = name;
      }
    }
    broadcast();
  });

  socket.on('remove_from_queue', (name) => {
    name = sanitize(name);
    state.queue = state.queue.filter(n => n !== name);
    const partner = state.pairs[name];
    if (partner) {
      delete state.pairs[name];
      if (state.pairs[partner] === name) delete state.pairs[partner];
    }
    broadcast();
  });

  socket.on('unlink_pair', (name) => {
    name = sanitize(name);
    const partner = state.pairs[name];
    if (partner) {
      delete state.pairs[name];
      if (state.pairs[partner] === name) delete state.pairs[partner];
    }
    broadcast();
  });

  socket.on('fill_court', (courtId) => {
    courtId = Number(courtId);
    const court = state.courts[courtId];
    if (court) { smartFillInto(court.players); broadcast(); }
  });

  socket.on('fill_next', (courtId) => {
    courtId = Number(courtId);
    const next = state.next[courtId];
    if (next) { smartFillInto(next.players); broadcast(); }
  });

  socket.on('manual_assign', ({ courtId, names }) => {
    courtId = Number(courtId);
    const court = state.courts[courtId];
    if (!court) return;
    const spots = 4 - court.players.length;
    const toAdd = (names || []).map(n => sanitize(n))
      .filter(n => n && state.queue.includes(n) && !court.players.includes(n))
      .slice(0, spots);
    const added = new Set(toAdd);
    state.queue = state.queue.filter(n => !added.has(n));
    court.players.push(...toAdd);
    broadcast();
  });

  socket.on('manual_assign_next', ({ courtId, names }) => {
    courtId = Number(courtId);
    const next = state.next[courtId];
    if (!next) return;
    const spots = 4 - next.players.length;
    const toAdd = (names || []).map(n => sanitize(n))
      .filter(n => n && state.queue.includes(n) && !next.players.includes(n))
      .slice(0, spots);
    const added = new Set(toAdd);
    state.queue = state.queue.filter(n => !added.has(n));
    next.players.push(...toAdd);
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

  socket.on('remove_from_next', ({ courtId, name }) => {
    courtId = Number(courtId);
    name = sanitize(name);
    const next = state.next[courtId];
    if (!next) return;
    const wasIn = next.players.includes(name);
    next.players = next.players.filter(n => n !== name);
    if (wasIn) state.queue.push(name);
    broadcast();
  });

  socket.on('promote_next', ({ fromCourtId, toCourtId }) => {
    fromCourtId = Number(fromCourtId);
    toCourtId   = Number(toCourtId);
    const next  = state.next[fromCourtId];
    const court = state.courts[toCourtId];
    if (!next || !court || next.players.length === 0) return;
    if (court.players.length > 0 || court.playing) return;
    court.players = [...next.players];
    next.players  = [];
    shiftNextSlots();
    broadcast();
  });

  socket.on('start_game', (courtId) => {
    courtId = Number(courtId);
    const court = state.courts[courtId];
    if (court && court.players.length === 4) { court.playing = true; broadcast(); }
  });

  socket.on('end_game', ({ courtId, toQueue }) => {
    courtId = Number(courtId);
    const court = state.courts[courtId];
    if (!court) return;
    if (toQueue) state.queue.push(...court.players);
    court.players = [];
    court.playing = false;
    // Auto-load staged players if any
    const next = state.next[courtId];
    if (next && next.players.length > 0) {
      court.players = [...next.players];
      next.players = [];
      shiftNextSlots();
    }
    broadcast();
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🏸  N1 Badminton Social 已启动`);
  console.log(`   本机访问: http://localhost:${PORT}`);
  console.log(`   局域网访问: http://<你的IP>:${PORT}\n`);
});
