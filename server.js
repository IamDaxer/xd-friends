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
const INPUT_RATE_MAX = 90;           // máx. mensajes INPUT/seg (mousemove/touchmove legítimos pueden ir a 60-90Hz)
const SUSPICION_BAN_THRESHOLD = 150; // más estricto que antes
const CONN_RATE_WINDOW = 15000;      // ventana para limitar conexiones nuevas por IP
const CONN_RATE_MAX = 5;             // máx. intentos de conexión por IP en esa ventana

function isFiniteNumber(n) {
  return typeof n === 'number' && Number.isFinite(n);
}
function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}
// Normaliza IPv6-mapped IPv4 (::ffff:1.2.3.4 -> 1.2.3.4) para comparar bien
function normalizeIP(ip) {
  if (!ip) return '';
  return ip.replace('::ffff:', '').trim();
}
// Render (y la mayoría de PaaS) ponen al server detrás de un proxy: la IP
// real del cliente viaja en X-Forwarded-For, no en el socket directamente.
function getClientIP(req) {
  const fwd = req.headers['x-forwarded-for'];
  if (fwd) return normalizeIP(fwd.split(',')[0]);
  return normalizeIP(req.socket.remoteAddress || '');
}

// --- Estado global ---
const activePlayers = new Map();   // ws -> playerState (los 10 que están jugando)
const waitingQueue = [];           // [{ ws, id, ip }] en orden de llegada
const bullets = [];
const bannedIPs = new Set();
const banReasons = new Map();      // ip -> motivo
const banByMap = new Map();        // ip -> quién baneó ("Owner" | "Sistema Anti-Cheat")
const ipToId = new Map();          // ip -> id estable durante la vida del proceso
const connAttempts = new Map();    // ip -> [timestamps de intentos de conexión]

function getIdForIp(ip) {
  if (!ipToId.has(ip)) {
    ipToId.set(ip, Math.random().toString(36).substring(2, 7).toUpperCase());
  }
  return ipToId.get(ip);
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
    x: Math.floor(Math.random() * (MAP_WIDTH - 100) + 50),
    y: Math.floor(Math.random() * (MAP_HEIGHT - 100) + 50),
    hp: 100,
    maxHp: 100,
    kills: 0,
    deaths: 0,
    angle: 0,
    moveX: 0,
    moveY: 0,
    isShooting: false,
    lastShootTime: 0,
    lastInputTime: Date.now(),
    suspicionScore: 0,
    msgWindowStart: Date.now(),
    msgCount: 0
  };
}

// Intenta meter a un recién llegado directo a la partida, o lo pone en cola
function admit(record) {
  if (activePlayers.size < MAX_ACTIVE_PLAYERS) {
    const state = makePlayerState(record.id, record.ip);
    state.ws = record.ws;
    activePlayers.set(record.ws, state);
    record.ws.send(JSON.stringify({ type: 'PROMOTED' }));
  } else {
    waitingQueue.push(record);
    record.ws.send(JSON.stringify({ type: 'WAITING', position: waitingQueue.length }));
  }
  broadcastLobby();
}

// Cuando un jugador activo se va, sube al primero de la cola
function promoteNextInQueue() {
  if (activePlayers.size >= MAX_ACTIVE_PLAYERS) return;
  const next = waitingQueue.shift();
  if (!next) return;
  if (next.ws.readyState !== 1) { promoteNextInQueue(); return; } // ya se desconectó, sigue con el siguiente
  const state = makePlayerState(next.id, next.ip);
  state.ws = next.ws;
  activePlayers.set(next.ws, state);
  next.ws.send(JSON.stringify({ type: 'PROMOTED' }));
  // Actualiza posición en cola de los que quedan
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

// Banea una IP: cierra sus conexiones activas/en cola, anuncia el motivo y quién baneó
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

// Consola local del servidor sigue disponible como respaldo de emergencia
process.stdin.on('data', (data) => {
  if (data.toString().trim().toLowerCase() === 'unban') {
    bannedIPs.forEach(ip => unbanIp(ip));
    console.log('\n[ADMIN CONSOLA] Todos los baneos eliminados.\n');
  }
});

wss.on('connection', (ws, req) => {
  const clientIP = getClientIP(req);
  const isOwner = OWNER_IP.length > 0 && clientIP === OWNER_IP;

  // --- Capa 1: tope duro de conexiones totales (protege contra floods de miles de sockets) ---
  if (activePlayers.size + waitingQueue.length >= MAX_ACTIVE_PLAYERS + MAX_QUEUE && !isOwner) {
    ws.send(JSON.stringify({ type: 'REJECTED', reason: 'Servidor lleno. Intenta más tarde.' }));
    ws.close();
    return;
  }

  // --- Capa 2: rate-limit de conexiones nuevas por IP ---
  if (!isOwner && isConnRateLimited(clientIP)) {
    ws.close();
    return;
  }

  // --- Capa 3: IP baneada ---
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

    // --- Comandos de Owner: se verifica la IP en CADA mensaje, nunca se confía en el cliente ---
    if (data.type === 'OWNER_CMD') {
      if (!isOwner) return; // se ignora en silencio, no delata nada
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
    if (!state) return; // sigue en cola, sus INPUT no cuentan hasta que juegue

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
    const targetIp = [...ipToId.entries()].find(([ip, id]) => id === targetId)?.[0];
    if (!targetIp) {
      ws.send(JSON.stringify({ type: 'OWNER_RESULT', ok: false, msg: `No se encontró el ID ${targetId}` }));
      return;
    }
    banIp(targetIp, reason, 'Owner');
    ws.send(JSON.stringify({ type: 'OWNER_RESULT', ok: true, msg: `Baneado: ${targetId}` }));
  } else if (action === 'UNBAN') {
    const targetIp = [...ipToId.entries()].find(([ip, id]) => id === targetId)?.[0];
    if (!targetIp || !bannedIPs.has(targetIp)) {
      ws.send(JSON.stringify({ type: 'OWNER_RESULT', ok: false, msg: `${targetId} no está baneado` }));
      return;
    }
    unbanIp(targetIp);
    ws.send(JSON.stringify({ type: 'OWNER_RESULT', ok: true, msg: `Desbaneado: ${targetId}` }));
  }
}

// Bucle de decaimiento de sospecha (solo aplica a jugadores activos)
setInterval(() => {
  activePlayers.forEach((p) => {
    if (p.suspicionScore > 0) p.suspicionScore = Math.max(0, p.suspicionScore - 30);
  });
}, 500);

// Bucle de física (60 FPS) — solo recorre a los jugadores activos, nunca a la cola
setInterval(() => {
  const now = Date.now();

  activePlayers.forEach((player) => {
    if (player.hp <= 0) return;

    let dx = player.moveX * PLAYER_SPEED;
    let dy = player.moveY * PLAYER_SPEED;

    const mag = Math.hypot(dx, dy);
    if (mag > PLAYER_SPEED) {
      dx = (dx / mag) * PLAYER_SPEED;
      dy = (dy / mag) * PLAYER_SPEED;
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
          targetPlayer.hp -= BULLET_DAMAGE;
          hit = true;

          if (targetPlayer.hp <= 0) {
            targetPlayer.hp = 0;
            targetPlayer.deaths++;
            const shooter = Array.from(activePlayers.values()).find(p => p.id === b.ownerId);
            if (shooter) shooter.kills++;

            setTimeout(() => {
              targetPlayer.hp = targetPlayer.maxHp;
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
          kills: p.kills,
          deaths: p.deaths,
          angle: p.angle
        })),
        bullets: bullets.map(b => ({ x: b.x, y: b.y }))
      }));
    }
  });
}, 1000 / 60);

if (!OWNER_IP) {
  console.log('[AVISO] OWNER_IP no está configurada. Nadie tendrá permisos de owner hasta que la definas como variable de entorno.');
}
console.log(`Servidor iniciado en el puerto ${PORT}. Máximo ${MAX_ACTIVE_PLAYERS} jugadores activos, cola hasta ${MAX_QUEUE}.`);
