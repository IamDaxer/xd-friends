const { WebSocketServer } = require('ws');

// Asignación de puerto dinámica para servidores en la nube / local
const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

const MAP_WIDTH = 800;
const MAP_HEIGHT = 600;
const PLAYER_SPEED = 4.5;
const PLAYER_RADIUS = 16;
const BULLET_SPEED = 12;
const BULLET_RADIUS = 4;
const BULLET_DAMAGE = 25;
const SHOOT_COOLDOWN = 180;

const players = new Map();
const bullets = [];
const bannedIPs = new Set();

process.stdin.on('data', (data) => {
  if (data.toString().trim().toLowerCase() === 'unban') {
    bannedIPs.clear();
    console.log('\n[ADMIN CONSOLA] Todos los baneos eliminados.\n');
  }
});

wss.on('connection', (ws, req) => {
  const clientIP = req.socket.remoteAddress || '127.0.0.1';
  let isBanned = bannedIPs.has(clientIP);
  const playerId = Math.random().toString(36).substring(2, 7);

  const playerState = {
    id: playerId,
    ip: clientIP,
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
    suspicionScore: 0
  };

  if (!isBanned) {
    players.set(ws, playerState);
  } else {
    ws.send(JSON.stringify({ type: 'BANNED', reason: 'IP baneada. Usa "unban" en la consola.' }));
  }

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);

      if (data.type === 'ADMIN_UNBAN' && data.adminKey === "UNBAN_KEY_DANIEL_2026") {
        bannedIPs.clear();
        isBanned = false;
        players.set(ws, playerState);
        ws.send(JSON.stringify({ type: 'NOTIFY', msg: 'Has sido desbaneado con éxito.' }));
        return;
      }

      if (isBanned) return;

      if (data.type === 'INPUT') {
        const now = Date.now();
        if (now - playerState.lastInputTime < 3) {
          playerState.suspicionScore += 1;
        }
        playerState.lastInputTime = now;

        if (typeof data.moveX === 'number') playerState.moveX = data.moveX;
        if (typeof data.moveY === 'number') playerState.moveY = data.moveY;
        if (typeof data.angle === 'number') playerState.angle = data.angle;
        playerState.isShooting = !!data.isShooting;

        if (playerState.suspicionScore >= 350) {
          bannedIPs.add(clientIP);
          isBanned = true;
          players.delete(ws);
          ws.send(JSON.stringify({ type: 'BANNED', reason: 'Detección anti-bot extrema' }));
        }
      }
    } catch (e) {}
  });

  ws.on('close', () => {
    players.delete(ws);
  });
});

setInterval(() => {
  players.forEach((p) => {
    if (p.suspicionScore > 0) p.suspicionScore = Math.max(0, p.suspicionScore - 30);
  });
}, 500);

// Bucle de física (60 FPS)
setInterval(() => {
  const now = Date.now();

  players.forEach((player) => {
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
    players.forEach((targetPlayer) => {
      if (hit) return;

      if (targetPlayer.id !== b.ownerId && targetPlayer.hp > 0) {
        if (Math.hypot(targetPlayer.x - b.x, targetPlayer.y - b.y) < PLAYER_RADIUS + BULLET_RADIUS) {
          targetPlayer.hp -= BULLET_DAMAGE;
          hit = true;

          if (targetPlayer.hp <= 0) {
            targetPlayer.hp = 0;
            targetPlayer.deaths++;
            const shooter = Array.from(players.values()).find(p => p.id === b.ownerId);
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
      const currentPlayer = players.get(client);
      client.send(JSON.stringify({
        type: 'STATE',
        myId: currentPlayer ? currentPlayer.id : null,
        players: Array.from(players.values()).map(p => ({
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

console.log(`Servidor iniciado en el puerto ${PORT} con soporte Mobile/PC.`);