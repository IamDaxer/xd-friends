const { WebSocketServer } = require('ws');

// Asignación de puerto dinámica para servidores en la nube / local
const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

// La IP del owner se define SOLO por variable de entorno, nunca en el código
// ni en el cliente. En Render: Settings -> Environment -> OWNER_IP = tu IP.
const OWNER_IP = (process.env.OWNER_IP || '').trim();

const MAP_WIDTH = 800;
const MAP_HEIGHT = 600;
const PLAYER_SPEED = 4.5;
const PLAYER_RADIUS = 16;
const BULLET_SPEED = 12;
const BULLET_RADIUS = 4;
const BULLET_DAMAGE = 25;
const SHOOT_COOLDOWN = 180;

// --- Configuración de sala ---
const MAX_ACTIVE_PLAYERS = 10;
const MAX_QUEUE = 20; // más allá de esto, se rechaza la conexión directamente

// --- Configuración Anti-Cheat ---
const MAX_MESSAGE_SIZE = 300;        // bytes, ningún mensaje legítimo supera esto
const INPUT_RATE_WINDOW = 1000;      // ventana en ms
const INPUT_RATE_MAX = 90;           // máx. mensajes INPUT/seg
const SUSPICION_BAN_THRESHOLD = 150;
const CONN_RATE_WINDOW = 15000;      // ventana para limitar conexiones nuevas por IP
const CONN_RATE_MAX = 5;             // máx. intentos de conexión por IP en esa ventana

// --- Power-ups ---
const POWERUP_TYPES = ['HEALTH', 'SPEED', 'SHIELD'];
const POWERUP_RADIUS = 14;
const MAX_POWERUPS = 4;
const POWERUP_SPAWN_INTERVAL = 6000;
const HEALTH_HEAL_AMOUNT = 40;
const SPEED_BOOST_MULT = 1.35;
const SPEED_BOOST_DURATION = 5000;
const SHIELD_DURATION = 5000;
const SHIELD_DAMAGE_MULT = 0.5; // con escudo activo, recibes la mitad de daño

// --- Chat ---
const CHAT_MAX_LEN = 60;
const CHAT_RATE_MS = 1200;

// --- Marcador persistente ---
const STATS_RESET_MS = 2 * 60 * 60 * 1000; // 2 horas

function isFiniteNumber(n) {
  return typeof n === 'number' && Number.isFinite(n);
}
function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}
function normalizeIP(ip) {
  if (!ip) return '';
  return ip.replace('::ffff:', '').trim();
}
function getClientIP(req) {
  const fwd = req.headers['x-forwarded-for'];
  if (fwd) return normalizeIP(fwd.split(',')[0]);
  return normalizeIP(req.socket.remoteAddress || '');
}

// --- Estado global ---
const activePlayers = new Map();   // ws -> playerState (los que están jugando)
const waitingQueue = [];           // [{ ws, id, ip }] en orden de llegada
const bullets = [];
const powerUps = [];
const bannedIPs = new Set();
const banReasons = new Map();
const banByMap = new Map();
const ipToId = new Map();          // ip -> id estable durante la vida del proceso
const connAttempts = new Map();    // ip -> [timestamps de intentos de conexión]
const playerStats = new Map();     // ip -> {kills, deaths} — persiste entre reconexiones, se limpia cada 2h

function getIdForIp(ip) {
  if (!ipToId.has(ip)) {
    ipToId.set(ip, Math.random().toString(36).substring(2, 7).toUpperCase());
  }
  return ipToId.get(ip);
}

function getStatsForIp(ip) {
  if (!playerStats.has(ip)) {
    playerStats.set(ip, { kills: 0, deaths: 0 });
  }
  return playerStats.get(ip);
}

function isConnRateLimited(ip) {
  const now = Date.now();
  const attempts = (connAttempts.get(ip) || []).filter(t => now - t < CONN_RATE_WINDOW);
  attempts.push(now);
  connAttempts.set(ip, attempts);
  return attempts.length > CONN_RATE_MAX;
}

function broadcastAll(payload) {
  const msg = JSON.stringify(payload);
  wss.clients.forEach((client) => {
    if (client.readyState === 1) client.send(msg);
  });
}

function broadcastAnnounce(text) {
  console.log(`[ANUNCIO] ${text}`);
  broadcastAll({ type: 'ANNOUNCE', text });
}

function broadcastLobby() {
  broadcastAll({
    type: 'LOBBY',
    activeCount: activePlayers.size,
    maxActive: MAX_ACTIVE_PLAYERS,
    waitingIds: waitingQueue.map(r => r.id)
  });
}

function makePlayerState(id, ip) {
  return {
    id,
    ip,
    stats: getStatsForIp(ip), // referencia compartida: al mutar aquí, se mutan las stats persistentes
    x: Math.floor(Math.random() * (MAP_WIDTH - 100) + 50),
    y: Math.floor(Math.random() * (MAP_HEIGHT - 100) + 50),
    hp: 100,
    maxHp: 100,
    angle: 0,
    moveX: 0,
    moveY: 0,
    isShooting: false,
    lastShootTime: 0,
    lastInputTime: Date.now(),
    lastChatTime: 0,
    suspicionScore: 0,
    msgWindowStart: Date.now(),
    msgCount: 0,
    speedBoostUntil: 0,
    shieldUntil: 0
  };
}

function admit(record) {
  if (activePlayers.size < MAX_ACTIVE_PLAYERS) {
    const state = makePlayerState(record.id, record.ip);
    activePlayers.set(record.ws, state);
    record.ws.send(JSON.stringify({ type: 'PROMOTED' }));
  } else {
    waitingQueue.push(record);
    record.ws.send(JSON.stringify({ type: 'WAITING', position: waitingQueue.length }));
  }
  broadcastLobby();
}

function promoteNextInQueue() {
  if (activePlayers.size >= MAX_ACTIVE_PLAYERS) return;
  const next = waitingQueue.shift();
  if (!next) return;
  if (next.ws.readyState !== 1) { promoteNextInQueue(); return; }
  const state = makePlayerState(next.id, next.ip);
  activePlayers.set(next.ws, state);
  next.ws.send(JSON.stringify({ type: 'PROMOTED' }));
  waitingQueue.forEach((r, idx) => {
    r.ws.send(JSON.stringify({ type: 'WAITING', position: idx + 1 }));
  });
  broadcastLobby();
}

function removeConnection(ws) {
  if (activePlayers.has(ws)) {
    activePlayers.delete(ws);
    promoteNextInQueue();
  } else {
    const idx = waitingQueue.findIndex(r => r.ws === ws);
    if (idx !== -1) {
      waitingQueue.splice(idx, 1);
      waitingQueue.forEach((r, i) => {
        r.ws.send(JSON.stringify({ type: 'WAITING', position: i + 1 }));
      });
    }
  }
  broadcastLobby();
}

function banIp(ip, reason, by) {
  bannedIPs.add(ip);
  banReasons.set(ip, reason);
  banByMap.set(ip, by);
  const id = getIdForIp(ip);

  broadcastAnnounce(`🚫 Jugador ${id} fue baneado por ${by}. Motivo: ${reason}`);

  for (const [ws, state] of activePlayers) {
    if (state.ip === ip) {
      ws.send(JSON.stringify({ type: 'BANNED', id, reason, bannedBy: by }));
      ws.close();
      activePlayers.delete(ws);
      promoteNextInQueue();
    }
  }
  for (let i = waitingQueue.length - 1; i >= 0; i--) {
    if (waitingQueue[i].ip === ip) {
      waitingQueue[i].ws.send(JSON.stringify({ type: 'BANNED', id, reason, bannedBy: by }));
      waitingQueue[i].ws.close();
      waitingQueue.splice(i, 1);
    }
  }
  broadcastLobby();
}

function unbanIp(ip) {
  bannedIPs.delete(ip);
  banReasons.delete(ip);
  banByMap.delete(ip);
  const id = getIdForIp(ip);
  broadcastAnnounce(`✅ Jugador ${id} fue desbaneado por Owner.`);
}

// --- Power-ups ---
function spawnPowerUp() {
  if (powerUps.length >= MAX_POWERUPS) return;
  const type = POWERUP_TYPES[Math.floor(Math.random() * POWERUP_TYPES.length)];
  powerUps.push({
    id: Math.random().toString(36).substring(2, 8),
    type,
    x: Math.floor(Math.random() * (MAP_WIDTH - 80) + 40),
    y: Math.floor(Math.random() * (MAP_HEIGHT - 80) + 40)
  });
}
setInterval(spawnPowerUp, POWERUP_SPAWN_INTERVAL);

function applyPowerUp(player, type) {
  const now = Date.now();
  if (type === 'HEALTH') {
    player.hp = Math.min(player.maxHp, player.hp + HEALTH_HEAL_AMOUNT);
  } else if (type === 'SPEED') {
    player.speedBoostUntil = now + SPEED_BOOST_DURATION;
  } else if (type === 'SHIELD') {
    player.shieldUntil = now + SHIELD_DURATION;
  }
}

// Consola local del servidor sigue disponible como respaldo de emergencia
process.stdin.on('data', (data) => {
  if (data.toString().trim().toLowerCase() === 'unban') {
    bannedIPs.forEach(ip => unbanIp(ip));
    console.log('\n[ADMIN CONSOLA] Todos los baneos eliminados.\n');
  }
});

// Reinicio del marcador cada 2 horas
setInterval(() => {
  playerStats.clear();
  broadcastAnnounce('🔄 El marcador se reinició (ciclo de 2 horas).');
}, STATS_RESET_MS);

wss.on('connection', (ws, req) => {
  const clientIP = getClientIP(req);
  const isOwner = OWNER_IP.length > 0 && clientIP === OWNER_IP;

  if (activePlayers.size + waitingQueue.length >= MAX_ACTIVE_PLAYERS + MAX_QUEUE && !isOwner) {
    ws.send(JSON.stringify({ type: 'REJECTED', reason: 'Servidor lleno. Intenta más tarde.' }));
    ws.close();
    return;
  }

  if (!isOwner && isConnRateLimited(clientIP)) {
    ws.close();
    return;
  }

  if (bannedIPs.has(clientIP)) {
    const id = getIdForIp(clientIP);
    ws.send(JSON.stringify({
      type: 'BANNED',
      id,
      reason: banReasons.get(clientIP) || 'Baneado',
      bannedBy: banByMap.get(clientIP) || 'Owner'
    }));
    ws.close();
    return;
  }

  const id = getIdForIp(clientIP);
  const record = { ws, id, ip: clientIP };

  ws.send(JSON.stringify({ type: 'ASSIGNED', id, isOwner }));
  admit(record);

  ws.on('message', (message) => {
    if (message.length > MAX_MESSAGE_SIZE) {
      const state = activePlayers.get(ws);
      if (state) {
        state.suspicionScore += 50;
        checkSuspicion(ws, state);
      }
      return;
    }

    let data;
    try {
      data = JSON.parse(message);
    } catch (e) {
      return;
    }

    if (data.type === 'OWNER_CMD') {
      if (!isOwner) return;
      handleOwnerCommand(ws, data);
      return;
    }

    if (data.type === 'PING') {
      if (isFiniteNumber(data.t)) {
        ws.send(JSON.stringify({ type: 'PONG', t: data.t }));
      }
      return;
    }

    if (bannedIPs.has(clientIP)) return;

    const state = activePlayers.get(ws);
    if (!state) return; // sigue en cola

    if (data.type === 'CHAT') {
      const now = Date.now();
      if (now - state.lastChatTime < CHAT_RATE_MS) return;
      if (typeof data.text !== 'string') return;
      const text = data.text.slice(0, CHAT_MAX_LEN).replace(/[\r\n]/g, ' ').trim();
      if (!text) return;
      state.lastChatTime = now;
      broadcastAll({ type: 'CHAT_MSG', id: state.id, text });
      return;
    }

    if (data.type === 'INPUT') {
      const now = Date.now();

      if (now - state.msgWindowStart > INPUT_RATE_WINDOW) {
        state.msgWindowStart = now;
        state.msgCount = 0;
      }
      state.msgCount++;
      if (state.msgCount > INPUT_RATE_MAX) {
        state.suspicionScore += 8;
        checkSuspicion(ws, state);
        return;
      }

      if (now - state.lastInputTime < 3) {
        state.suspicionScore += 2;
      }
      state.lastInputTime = now;

      if (isFiniteNumber(data.moveX) && isFiniteNumber(data.moveY)) {
        state.moveX = clamp(data.moveX, -1, 1);
        state.moveY = clamp(data.moveY, -1, 1);
      } else {
        state.suspicionScore += 20;
      }

      if (isFiniteNumber(data.angle)) {
        state.angle = data.angle;
      } else {
        state.suspicionScore += 20;
      }

      state.isShooting = data.isShooting === true;

      checkSuspicion(ws, state);
    }
  });

  ws.on('close', () => {
    removeConnection(ws);
  });
});

function checkSuspicion(ws, state) {
  if (state.suspicionScore >= SUSPICION_BAN_THRESHOLD) {
    banIp(state.ip, 'Actividad anómala detectada (anti-cheat)', 'Sistema Anti-Cheat');
  }
}

function handleOwnerCommand(ws, data) {
  const action = data.action;
  const targetId = String(data.targetId || '').trim().toUpperCase();

  if (action === 'BAN') {
    const reason = String(data.reason || 'Sin motivo especificado').slice(0, 100) || 'Sin motivo especificado';
    const targetIp = [...ipToId.entries()].find(([ip, idv]) => idv === targetId)?.[0];
    if (!targetIp) {
      ws.send(JSON.stringify({ type: 'OWNER_RESULT', ok: false, msg: `No se encontró el ID ${targetId}` }));
      return;
    }
    banIp(targetIp, reason, 'Owner');
    ws.send(JSON.stringify({ type: 'OWNER_RESULT', ok: true, msg: `Baneado: ${targetId}` }));
  } else if (action === 'UNBAN') {
    const targetIp = [...ipToId.entries()].find(([ip, idv]) => idv === targetId)?.[0];
    if (!targetIp || !bannedIPs.has(targetIp)) {
      ws.send(JSON.stringify({ type: 'OWNER_RESULT', ok: false, msg: `${targetId} no está baneado` }));
      return;
    }
    unbanIp(targetIp);
    ws.send(JSON.stringify({ type: 'OWNER_RESULT', ok: true, msg: `Desbaneado: ${targetId}` }));
  }
}

// Bucle de decaimiento de sospecha
setInterval(() => {
  activePlayers.forEach((p) => {
    if (p.suspicionScore > 0) p.suspicionScore = Math.max(0, p.suspicionScore - 30);
  });
}, 500);

// Bucle de física (60 FPS)
setInterval(() => {
  const now = Date.now();

  activePlayers.forEach((player) => {
    if (player.hp <= 0) return;

    const effectiveSpeed = (player.speedBoostUntil > now) ? PLAYER_SPEED * SPEED_BOOST_MULT : PLAYER_SPEED;

    let dx = player.moveX * effectiveSpeed;
    let dy = player.moveY * effectiveSpeed;

    const mag = Math.hypot(dx, dy);
    if (mag > effectiveSpeed) {
      dx = (dx / mag) * effectiveSpeed;
      dy = (dy / mag) * effectiveSpeed;
    }

    player.x = Math.max(PLAYER_RADIUS, Math.min(MAP_WIDTH - PLAYER_RADIUS, player.x + dx));
    player.y = Math.max(PLAYER_RADIUS, Math.min(MAP_HEIGHT - PLAYER_RADIUS, player.y + dy));

    if (player.isShooting && now - player.lastShootTime >= SHOOT_COOLDOWN) {
      player.lastShootTime = now;
      bullets.push({
        id: Math.random().toString(36).substring(2),
        ownerId: player.id,
        x: player.x + Math.cos(player.angle) * (PLAYER_RADIUS + 4),
        y: player.y + Math.sin(player.angle) * (PLAYER_RADIUS + 4),
        vx: Math.cos(player.angle) * BULLET_SPEED,
        vy: Math.sin(player.angle) * BULLET_SPEED,
        life: 70
      });
    }

    // Recoger power-ups
    for (let i = powerUps.length - 1; i >= 0; i--) {
      const pu = powerUps[i];
      if (Math.hypot(player.x - pu.x, player.y - pu.y) < PLAYER_RADIUS + POWERUP_RADIUS) {
        applyPowerUp(player, pu.type);
        powerUps.splice(i, 1);
      }
    }
  });

  for (let i = bullets.length - 1; i >= 0; i--) {
    const b = bullets[i];
    b.x += b.vx;
    b.y += b.vy;
    b.life--;

    if (b.life <= 0 || b.x < 0 || b.x > MAP_WIDTH || b.y < 0 || b.y > MAP_HEIGHT) {
      bullets.splice(i, 1);
      continue;
    }

    let hit = false;
    activePlayers.forEach((targetPlayer) => {
      if (hit) return;

      if (targetPlayer.id !== b.ownerId && targetPlayer.hp > 0) {
        if (Math.hypot(targetPlayer.x - b.x, targetPlayer.y - b.y) < PLAYER_RADIUS + BULLET_RADIUS) {
          let damage = BULLET_DAMAGE;
          if (targetPlayer.shieldUntil > now) damage *= SHIELD_DAMAGE_MULT;
          targetPlayer.hp -= damage;
          hit = true;

          if (targetPlayer.hp <= 0) {
            targetPlayer.hp = 0;
            targetPlayer.stats.deaths++;
            const shooter = Array.from(activePlayers.values()).find(p => p.id === b.ownerId);
            if (shooter) shooter.stats.kills++;

            broadcastAll({ type: 'KILL', killerId: b.ownerId, victimId: targetPlayer.id });

            setTimeout(() => {
              targetPlayer.hp = targetPlayer.maxHp;
              targetPlayer.speedBoostUntil = 0;
              targetPlayer.shieldUntil = 0;
              targetPlayer.x = Math.floor(Math.random() * (MAP_WIDTH - 100) + 50);
              targetPlayer.y = Math.floor(Math.random() * (MAP_HEIGHT - 100) + 50);
            }, 2500);
          }
        }
      }
    });

    if (hit) bullets.splice(i, 1);
  }

  wss.clients.forEach((client) => {
    if (client.readyState === 1) {
      const currentPlayer = activePlayers.get(client);
      client.send(JSON.stringify({
        type: 'STATE',
        myId: currentPlayer ? currentPlayer.id : null,
        players: Array.from(activePlayers.values()).map(p => ({
          id: p.id,
          x: p.x,
          y: p.y,
          hp: p.hp,
          maxHp: p.maxHp,
          kills: p.stats.kills,
          deaths: p.stats.deaths,
          angle: p.angle,
          hasShield: p.shieldUntil > now,
          hasSpeed: p.speedBoostUntil > now
        })),
        bullets: bullets.map(b => ({ x: b.x, y: b.y })),
        powerUps: powerUps.map(pu => ({ id: pu.id, type: pu.type, x: pu.x, y: pu.y }))
      }));
    }
  });
}, 1000 / 60);

if (!OWNER_IP) {
  console.log('[AVISO] OWNER_IP no está configurada. Nadie tendrá permisos de owner hasta que la definas como variable de entorno.');
}
console.log(`Servidor iniciado en el puerto ${PORT}. Máximo ${MAX_ACTIVE_PLAYERS} jugadores activos, cola hasta ${MAX_QUEUE}.`);
