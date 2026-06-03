const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(path.join(__dirname, 'public')));

const INITIAL_COURT_IDS = [6, 7, 8, 9, 10];
const EXTRA_COURT_IDS   = [1, 2, 3, 4, 5]; // venue courts available as extras
const LEVELS = ['advanced', 'intermediate', 'novice', 'beginner'];
const ADMIN_PIN = String(process.env.ADMIN_PIN || '1234');

const state = {
  courtIds: [...INITIAL_COURT_IDS],
  courts: Object.fromEntries(INITIAL_COURT_IDS.map(id => [id, { players: [], playing: false }])),
  next:    Object.fromEntries(INITIAL_COURT_IDS.map(id => [id, { players: [] }])),
  queue:   [],
  players: {},
  pairs:   {},
  history: [],
};

let connectedCount = 0;
const undoSnapshots = {};
const courtUndoSnapshots = {}; // courtId -> { type: 'end_game'|'promote', players, wasPlaying }

function stateWithUndo() {
  const canUndo = Object.fromEntries(INITIAL_COURT_IDS.map(id => [id, !!undoSnapshots[id]]));
  const canUndoCourt = Object.fromEntries(state.courtIds.map(id => [id, !!courtUndoSnapshots[id]]));
  return { ...state, canUndo, canUndoCourt };
}

function broadcast() {
  io.emit('state_update', stateWithUndo());
}

function sanitize(name) {
  return String(name || '').trim().replace(/[<>"'&]/g, '').slice(0, 20);
}

function isNameUsed(name) {
  if (state.queue.includes(name)) return true;
  if (state.courtIds.some(id => state.courts[id].players.includes(name))) return true;
  if (INITIAL_COURT_IDS.some(id => state.next[id].players.includes(name))) return true;
  return false;
}

function lvlIdx(name) {
  const i = LEVELS.indexOf(state.players[name]);
  return i >= 0 ? i : 2;
}

function shiftNextSlots() {
  const filled = INITIAL_COURT_IDS.map(id => state.next[id].players).filter(p => p.length > 0);
  INITIAL_COURT_IDS.forEach((id, i) => {
    state.next[id].players = filled[i] ? [...filled[i]] : [];
  });
  INITIAL_COURT_IDS.forEach(id => { delete undoSnapshots[id]; });
}

function cleanupPair(name) {
  const partner = state.pairs[name];
  if (partner) {
    delete state.pairs[name];
    if (state.pairs[partner] === name) delete state.pairs[partner];
  }
}

// Locate a player across queue / courts / on-deck. Returns null if not found.
function findPlayer(name) {
  if (state.queue.includes(name)) return { type: 'queue' };
  for (const id of state.courtIds) {
    if (state.courts[id] && state.courts[id].players.includes(name)) return { type: 'court', id };
  }
  for (const id of INITIAL_COURT_IDS) {
    if (state.next[id] && state.next[id].players.includes(name)) return { type: 'next', id };
  }
  return null;
}

function smartFillInto(targetPlayers) {
  const needed = 4 - targetPlayers.length;
  if (needed <= 0 || state.queue.length === 0) return;

  const pool = [...state.queue];
  const chosen = [];

  function getUnit(name) {
    const partner = state.pairs[name];
    if (partner && pool.includes(partner)) return [name, partner];
    return [name];
  }

  function removeFromPool(...names) {
    names.forEach(n => { const i = pool.indexOf(n); if (i !== -1) pool.splice(i, 1); });
  }

  while (chosen.length < needed && pool.length > 0) {
    const lvls = chosen.map(lvlIdx);
    const minL = chosen.length ? Math.min(...lvls) : null;
    const maxL = chosen.length ? Math.max(...lvls) : null;

    let foundUnit = null;

    for (const name of pool) {
      const unit = getUnit(name);
      if (chosen.length + unit.length > needed) continue;
      if (chosen.length === 0) { foundUnit = unit; break; }
      const uLvls = unit.map(lvlIdx);
      if (Math.max(maxL, ...uLvls) - Math.min(minL, ...uLvls) <= 1) { foundUnit = unit; break; }
    }

    if (!foundUnit) {
      for (const name of pool) {
        const unit = getUnit(name);
        if (chosen.length + unit.length <= needed) { foundUnit = unit; break; }
      }
    }

    if (!foundUnit) break;
    removeFromPool(...foundUnit);
    chosen.push(...foundUnit);
  }

  const chosenSet = new Set(chosen);
  state.queue = state.queue.filter(n => !chosenSet.has(n));
  targetPlayers.push(...chosen);
}

io.on('connection', (socket) => {
  connectedCount++;
  io.emit('connected_count', connectedCount);
  socket.isAdmin = false;
  socket.emit('state_update', stateWithUndo());

  socket.on('disconnect', () => {
    connectedCount--;
    io.emit('connected_count', connectedCount);
  });

  socket.on('admin_login', (pin) => {
    if (String(pin) === ADMIN_PIN) {
      socket.isAdmin = true;
      socket.emit('admin_result', { ok: true });
    } else {
      socket.isAdmin = false;
      socket.emit('admin_result', { ok: false });
    }
  });

  socket.on('admin_logout', () => { socket.isAdmin = false; });

  socket.on('join_queue', ({ name, level, partner }) => {
    name = sanitize(name);
    partner = sanitize(partner || '');
    if (!name) return;
    if (isNameUsed(name)) { socket.emit('join_error', { reason: 'name_taken', name }); return; }
    if (!LEVELS.includes(level)) return;
    state.players[name] = level;
    state.queue.push(name);
    if (partner && partner !== name) {
      state.pairs[name] = partner;
      if (state.pairs[partner] === name) {
        // already mutual — confirmed
      } else {
        state.pairs[partner] = name;
      }
    }
    socket.emit('join_success');
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
    if (!socket.isAdmin) return;
    name = sanitize(name);
    const partner = state.pairs[name];
    if (partner) {
      delete state.pairs[name];
      if (state.pairs[partner] === name) delete state.pairs[partner];
    }
    broadcast();
  });

  socket.on('add_court', () => {
    if (!socket.isAdmin) return;
    const allIds = [...EXTRA_COURT_IDS, ...INITIAL_COURT_IDS].sort((a, b) => a - b);
    const nextId = allIds.find(id => !state.courtIds.includes(id));
    if (nextId === undefined) return; // all courts already active
    state.courtIds.push(nextId);
    state.courtIds.sort((a, b) => a - b);
    state.courts[nextId] = { players: [], playing: false };
    broadcast();
  });

  socket.on('remove_court', (courtId) => {
    if (!socket.isAdmin) return;
    courtId = Number(courtId);
    if (!state.courtIds.includes(courtId)) return;
    const court = state.courts[courtId];
    const next  = state.next[courtId];
    if (court.players.length > 0 || court.playing) return;
    if (next && next.players.length > 0) return;
    state.courtIds = state.courtIds.filter(id => id !== courtId);
    delete state.courts[courtId];
    broadcast();
  });

  socket.on('fill_court', (courtId) => {
    if (!socket.isAdmin) return;
    courtId = Number(courtId);
    const court = state.courts[courtId];
    if (court) { delete courtUndoSnapshots[courtId]; smartFillInto(court.players); broadcast(); }
  });

  socket.on('fill_next', (courtId) => {
    if (!socket.isAdmin) return;
    courtId = Number(courtId);
    const next = state.next[courtId];
    if (!next) return;
    const before = new Set(next.players);
    smartFillInto(next.players);
    const added = next.players.filter(p => !before.has(p));
    if (added.length > 0) undoSnapshots[courtId] = added;
    broadcast();
  });

  socket.on('manual_assign', ({ courtId, names }) => {
    if (!socket.isAdmin) return;
    courtId = Number(courtId);
    const court = state.courts[courtId];
    if (!court) return;
    delete courtUndoSnapshots[courtId];
    const spots = 4 - court.players.length;
    const toAdd = (names || []).map(n => sanitize(n))
      .filter(n => n && state.queue.includes(n) && !court.players.includes(n))
      .slice(0, spots);
    if (toAdd.length === 0) return;
    const added = new Set(toAdd);
    state.queue = state.queue.filter(n => !added.has(n));
    toAdd.forEach(cleanupPair);
    court.players.push(...toAdd);
    broadcast();
  });

  socket.on('manual_assign_next', ({ courtId, names }) => {
    if (!socket.isAdmin) return;
    courtId = Number(courtId);
    const next = state.next[courtId];
    if (!next) return;
    const spots = 4 - next.players.length;
    const toAdd = (names || []).map(n => sanitize(n))
      .filter(n => n && state.queue.includes(n) && !next.players.includes(n))
      .slice(0, spots);
    if (toAdd.length === 0) return;
    undoSnapshots[courtId] = toAdd;
    const added = new Set(toAdd);
    state.queue = state.queue.filter(n => !added.has(n));
    toAdd.forEach(cleanupPair);
    next.players.push(...toAdd);
    broadcast();
  });

  socket.on('assign_player', ({ courtId, name }) => {
    if (!socket.isAdmin) return;
    courtId = Number(courtId);
    name = sanitize(name);
    const court = state.courts[courtId];
    if (!court || court.players.length >= 4 || court.players.includes(name)) return;
    delete courtUndoSnapshots[courtId];
    state.queue = state.queue.filter(n => n !== name);
    cleanupPair(name);
    court.players.push(name);
    broadcast();
  });

  // Atomically move a player between queue / court / on-deck (admin drag-and-drop).
  socket.on('move_player', ({ name, toType, toId }) => {
    if (!socket.isAdmin) return;
    name = sanitize(name);
    if (!name) return;
    toId = (toId === undefined || toId === null) ? null : Number(toId);

    const from = findPlayer(name);
    if (!from) return; // unknown / stale player

    // Validate destination + capacity (and ignore no-op moves to same slot)
    if (toType === 'court') {
      const dest = state.courts[toId];
      if (!dest) return;
      if (from.type === 'court' && from.id === toId) return;
      if (dest.players.length >= 4) return;
    } else if (toType === 'next') {
      const dest = state.next[toId];
      if (!dest) return;
      if (from.type === 'next' && from.id === toId) return;
      if (dest.players.length >= 4) return;
    } else if (toType === 'queue') {
      if (from.type === 'queue') return;
    } else {
      return;
    }

    // Remove from source
    if (from.type === 'queue') {
      state.queue = state.queue.filter(n => n !== name);
    } else if (from.type === 'court') {
      const c = state.courts[from.id];
      c.players = c.players.filter(n => n !== name);
      if (c.players.length < 4) c.playing = false;
      delete courtUndoSnapshots[from.id];
    } else if (from.type === 'next') {
      state.next[from.id].players = state.next[from.id].players.filter(n => n !== name);
      delete undoSnapshots[from.id];
    }

    // Add to destination
    if (toType === 'court') {
      cleanupPair(name);
      state.courts[toId].players.push(name);
      delete courtUndoSnapshots[toId];
    } else if (toType === 'next') {
      cleanupPair(name);
      state.next[toId].players.push(name);
      delete undoSnapshots[toId];
    } else if (toType === 'queue') {
      state.queue.push(name);
    }

    broadcast();
  });

  socket.on('remove_from_court', ({ courtId, name }) => {
    if (!socket.isAdmin) return;
    courtId = Number(courtId);
    name = sanitize(name);
    const court = state.courts[courtId];
    if (!court) return;
    delete courtUndoSnapshots[courtId];
    court.players = court.players.filter(n => n !== name);
    if (court.players.length < 4) court.playing = false;
    broadcast();
  });

  socket.on('remove_from_next', ({ courtId, name }) => {
    if (!socket.isAdmin) return;
    courtId = Number(courtId);
    name = sanitize(name);
    const next = state.next[courtId];
    if (!next) return;
    const wasIn = next.players.includes(name);
    next.players = next.players.filter(n => n !== name);
    if (wasIn) {
      cleanupPair(name);
      state.queue = [name, ...state.queue];
      delete undoSnapshots[courtId];
    }
    broadcast();
  });

  socket.on('promote_next', ({ fromCourtId, toCourtId }) => {
    if (!socket.isAdmin) return;
    fromCourtId = Number(fromCourtId);
    toCourtId   = Number(toCourtId);
    const next  = state.next[fromCourtId];
    const court = state.courts[toCourtId];
    if (!next || !court || next.players.length === 0) return;
    if (court.players.length > 0 || court.playing) return;
    courtUndoSnapshots[toCourtId] = { type: 'promote', players: [...next.players], wasPlaying: false };
    court.players = [...next.players];
    next.players  = [];
    shiftNextSlots();
    broadcast();
  });

  socket.on('undo_deck', (courtId) => {
    if (!socket.isAdmin) return;
    courtId = Number(courtId);
    const added = undoSnapshots[courtId];
    if (!added) return;
    delete undoSnapshots[courtId];
    const addedSet = new Set(added);
    state.next[courtId].players = state.next[courtId].players.filter(p => !addedSet.has(p));
    state.queue = [...added, ...state.queue];
    broadcast();
  });

  socket.on('start_game', (courtId) => {
    if (!socket.isAdmin) return;
    courtId = Number(courtId);
    const court = state.courts[courtId];
    if (court && court.players.length === 4) { delete courtUndoSnapshots[courtId]; court.playing = true; broadcast(); }
  });

  socket.on('end_game', ({ courtId, toQueue }) => {
    if (!socket.isAdmin) return;
    courtId = Number(courtId);
    const court = state.courts[courtId];
    if (!court) return;
    if (court.players.length > 0) {
      courtUndoSnapshots[courtId] = { type: 'end_game', players: [...court.players], wasPlaying: court.playing };
      state.history.unshift({ time: Date.now(), courtId, players: [...court.players] });
      if (state.history.length > 100) state.history.pop();
    }
    if (toQueue) state.queue.push(...court.players);
    court.players = [];
    court.playing = false;
    broadcast();
  });


  socket.on('undo_court_action', (courtId) => {
    if (!socket.isAdmin) return;
    courtId = Number(courtId);
    const snap = courtUndoSnapshots[courtId];
    if (!snap) return;
    const court = state.courts[courtId];
    if (!court) return;

    if (snap.type === 'end_game') {
      if (court.players.length > 0 || court.playing) return;
      const snapSet = new Set(snap.players);
      state.queue = state.queue.filter(n => !snapSet.has(n));
      court.players = [...snap.players];
      court.playing = snap.wasPlaying;
    } else if (snap.type === 'promote') {
      if (court.playing) return;
      const snapSet = new Set(snap.players);
      const sameSet = court.players.length === snap.players.length && court.players.every(p => snapSet.has(p));
      if (!sameSet) return;
      court.players = [];
      state.queue = [...snap.players, ...state.queue];
    }

    delete courtUndoSnapshots[courtId];
    broadcast();
  });

  socket.on('set_player_level', ({ name, level }) => {
    if (!socket.isAdmin) return;
    name = sanitize(name);
    if (!name || !LEVELS.includes(level)) return;
    if (state.players[name] === undefined) return;
    state.players[name] = level;
    broadcast();
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🏸  N1 Badminton Social 已启动`);
  console.log(`   本机访问: http://localhost:${PORT}`);
  console.log(`   局域网访问: http://<你的IP>:${PORT}\n`);
});
