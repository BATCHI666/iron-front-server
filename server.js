const { WebSocketServer, WebSocket } = require("ws");
const { randomUUID } = require("crypto");
const http = require("http");
const fs = require("fs");
const path = require("path");

const PORT = Number(process.env.PORT) || 3000;
const ROOT = __dirname;

// HTTP serves the game files; the same server also supports WebSocket multiplayer.
const httpServer = http.createServer((req, res) => {
  const pathname = decodeURIComponent((req.url || "/").split("?")[0]);
  const requested = pathname === "/" ? "/index.html" : pathname;
  const filePath = path.resolve(ROOT, "." + requested);

  if (filePath !== ROOT && !filePath.startsWith(ROOT + path.sep)) {
    res.writeHead(403, { "Content-Type": "text/plain; charset=utf-8" });
    return res.end("Forbidden");
  }

  fs.stat(filePath, (err, stat) => {
    if (err || !stat.isFile()) {
      res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
      return res.end("Not found");
    }

    const ext = path.extname(filePath).toLowerCase();
    const types = {
      ".html": "text/html; charset=utf-8",
      ".js": "text/javascript; charset=utf-8",
      ".css": "text/css; charset=utf-8",
      ".json": "application/json; charset=utf-8",
      ".png": "image/png",
      ".jpg": "image/jpeg",
      ".jpeg": "image/jpeg",
      ".webp": "image/webp",
      ".svg": "image/svg+xml",
      ".ico": "image/x-icon",
      ".mp3": "audio/mpeg",
      ".ogg": "audio/ogg",
      ".wav": "audio/wav"
    };
    res.writeHead(200, { "Content-Type": types[ext] || "application/octet-stream" });
    fs.createReadStream(filePath).pipe(res);
  });
});

const wss = new WebSocketServer({ server: httpServer });

const clients = new Map();
const rooms = new Map();

function send(ws, data) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function sendError(client, message) {
  send(client.ws, { type: "error", message });
}

function broadcastRoom(room, data, exceptId = null) {
  for (const player of room.players.values()) {
    if (player.id !== exceptId) {
      send(player.ws, data);
    }
  }
}

function defaultPlayer(p, index = 0) {
  return {
    id: p.id,
    name: p.name,
    color: p.player?.color || "#829f60",
    x: Number.isFinite(p.player?.x)
      ? p.player.x
      : 170 + (index % 4) * 85,
    y: Number.isFinite(p.player?.y)
      ? p.player.y
      : 170 + Math.floor(index / 4) * 85,
    angle: Number.isFinite(p.player?.angle)
      ? p.player.angle
      : 0,
    turret: Number.isFinite(p.player?.turret)
      ? p.player.turret
      : 0,
    hp: Number.isFinite(p.player?.hp)
      ? p.player.hp
      : 100,
    maxHp: Number.isFinite(p.player?.maxHp)
      ? p.player.maxHp
      : 100
  };
}

function roomInfo(room) {
  const members = [...room.players.values()].map(
    (p, index) => defaultPlayer(p, index)
  );

  return {
    id: room.id,
    name: room.name,
    ownerId: room.hostId,
    hostId: room.hostId,
    players: room.players.size,
    playerNames: members.map(p => p.name),
    hasPassword: Boolean(room.password),
    maxPlayers: room.maxPlayers,
    started: room.started,
    members
  };
}

function sendRooms(ws) {
  const list = [...rooms.values()]
    .filter(room => room.players.size < room.maxPlayers)
    .map(roomInfo);

  send(ws, {
    type: "rooms",
    rooms: list
  });
}

function refreshRoomLists() {
  for (const client of clients.values()) {
    if (!client.roomId) {
      sendRooms(client.ws);
    }
  }
}

function updateRoom(room) {
  if (!rooms.has(room.id)) return;

  const info = roomInfo(room);

  broadcastRoom(room, {
    type: "roomUpdate",
    room: info,
    players: info.members
  });

  refreshRoomLists();
}

function leaveRoom(client) {
  if (!client.roomId) return;

  const room = rooms.get(client.roomId);

  client.roomId = null;
  client.player = null;

  if (!room) {
    send(client.ws, { type: "left" });
    refreshRoomLists();
    return;
  }

  room.players.delete(client.id);

  if (room.players.size === 0) {
    rooms.delete(room.id);
  } else {
    if (room.hostId === client.id) {
      room.hostId = room.players.keys().next().value;
    }

    const info = roomInfo(room);

    broadcastRoom(room, {
      type: "playerLeft",
      id: client.id,
      playerId: client.id,
      room: info
    });

    updateRoom(room);
  }

  send(client.ws, { type: "left" });
  refreshRoomLists();
}

function startGame(client) {
  const room = rooms.get(client.roomId);

  if (!room) {
    sendError(
      client,
      "Сначала создай комнату или присоединись к ней."
    );
    return;
  }

  if (room.hostId !== client.id) {
    sendError(
      client,
      "Начать бой может только создатель комнаты."
    );
    return;
  }

  if (room.started) return;

  room.started = true;

  const members = [...room.players.values()];

  const players = members.map((p, index) => {
    const player = {
      id: p.id,
      name: p.name,
      color: p.player?.color || "#829f60",
      x: 170 + (index % 4) * 85,
      y: 170 + Math.floor(index / 4) * 85,
      angle: 0,
      turret: 0,
      hp: 100,
      maxHp: 100
    };

    p.player = player;
    return player;
  });

  broadcastRoom(room, {
    type: "started",
    roomId: room.id,
    players
  });

  updateRoom(room);

  console.log(
    `Бой начался: "${room.name}", игроков: ${room.players.size}`
  );
}

function joinRoom(client, requestedId, password = "") {
  const room = rooms.get(String(requestedId || ""));

  if (!room) {
    sendError(client, "Комната не найдена.");
    sendRooms(client.ws);
    return;
  }

  if (
    room.password &&
    String(password) !== room.password
  ) {
    sendError(client, "Неверный пароль комнаты.");
    return;
  }

  if (
    room.players.size >= room.maxPlayers &&
    !room.players.has(client.id)
  ) {
    sendError(client, "Комната заполнена.");
    return;
  }

  if (client.roomId === room.id) {
    send(client.ws, {
      type: "joined",
      room: roomInfo(room),
      players: roomInfo(room).members,
      id: client.id
    });

    if (room.started) {
      send(client.ws, {
        type: "started",
        roomId: room.id,
        players: [...room.players.values()].map(
          (p, index) => defaultPlayer(p, index)
        )
      });
    }

    return;
  }

  if (client.roomId) {
    leaveRoom(client);
  }

  client.roomId = room.id;
  client.player = null;

  room.players.set(client.id, {
    id: client.id,
    ws: client.ws,
    name: client.name,
    player: null
  });

  const info = roomInfo(room);

  send(client.ws, {
    type: "joined",
    room: info,
    players: info.members,
    id: client.id
  });

  broadcastRoom(
    room,
    {
      type: "playerJoined",
      id: client.id,
      playerId: client.id,
      player: {
        id: client.id,
        name: client.name
      },
      name: client.name,
      room: info
    },
    client.id
  );

  updateRoom(room);

  // Подключившийся игрок входит в уже начавшийся бой.
  if (room.started) {
    send(client.ws, {
      type: "started",
      roomId: room.id,
      players: [...room.players.values()].map(
        (p, index) => defaultPlayer(p, index)
      )
    });
  }
}

function handleMessage(client, message) {
  if (!message || typeof message.type !== "string") {
    return;
  }

  switch (message.type) {
    case "hello":
    case "setName": {
      if (typeof message.name === "string") {
        const name = message.name.trim().slice(0, 16);
        if (name) client.name = name;
      }

      send(client.ws, {
        type: "welcome",
        id: client.id,
        name: client.name
      });

      sendRooms(client.ws);
      break;
    }

    case "getRooms":
    case "listRooms": {
      sendRooms(client.ws);
      break;
    }

    case "createRoom": {
      const name = String(
        message.name || message.roomName || ""
      ).trim().slice(0, 28);

      if (!name) {
        sendError(client, "Введи название комнаты.");
        return;
      }

      if (client.roomId) {
        leaveRoom(client);
      }

      const maxPlayers = Math.max(
        2,
        Math.min(12, Number(message.maxPlayers) || 4)
      );

      const password = String(
        message.password || ""
      ).slice(0, 40);

      const id = randomUUID().slice(0, 8);

      const room = {
        id,
        name,
        password,
        hostId: client.id,
        maxPlayers,
        players: new Map(),
        started: false
      };

      rooms.set(id, room);

      client.roomId = id;
      client.player = null;

      room.players.set(client.id, {
        id: client.id,
        ws: client.ws,
        name: client.name,
        player: null
      });

      const info = roomInfo(room);

      send(client.ws, {
        type: "roomCreated",
        room: info,
        roomId: id
      });

      send(client.ws, {
        type: "joined",
        room: info,
        players: info.members,
        id: client.id
      });

      updateRoom(room);

      // Автоматически начинаем бой после создания комнаты.
      startGame(client);

      break;
    }

    case "joinRoom": {
      joinRoom(
        client,
        message.roomId || message.id || message.room,
        message.password || ""
      );
      break;
    }

    case "leaveRoom": {
      leaveRoom(client);
      break;
    }

    case "startGame": {
      startGame(client);
      break;
    }

    case "state": {
      const room = rooms.get(client.roomId);

      if (!room || !room.started || !message.player) {
        return;
      }

      const p = message.player;

      const validNumber = value =>
        typeof value === "number" &&
        Number.isFinite(value);

      if (
        !validNumber(p.x) ||
        !validNumber(p.y) ||
        Math.abs(p.x) > 10000 ||
        Math.abs(p.y) > 10000
      ) {
        return;
      }

      const playerState = {
        x: p.x,
        y: p.y,
        angle: validNumber(p.angle) ? p.angle : 0,
        turret: validNumber(p.turret) ? p.turret : 0,
        hp: validNumber(p.hp)
          ? Math.max(0, Math.min(1000, p.hp))
          : 100,
        maxHp: validNumber(p.maxHp)
          ? Math.max(1, Math.min(1000, p.maxHp))
          : 100,
        name: client.name,
        color: typeof p.color === "string"
          ? p.color.slice(0, 24)
          : "#829f60"
      };

      client.player = playerState;

      const roomPlayer = room.players.get(client.id);

      if (roomPlayer) {
        roomPlayer.player = playerState;
      }

      broadcastRoom(
        room,
        {
          type: "state",
          id: client.id,
          playerId: client.id,
          player: playerState
        },
        client.id
      );

      break;
    }

    case "shoot": {
      const room = rooms.get(client.roomId);

      if (!room || !room.started) return;

      const numberOr = (value, fallback = 0) =>
        typeof value === "number" &&
        Number.isFinite(value)
          ? value
          : fallback;

      broadcastRoom(
        room,
        {
          type: "shoot",
          id: client.id,
          playerId: client.id,
          x: numberOr(message.x),
          y: numberOr(message.y),
          vx: numberOr(message.vx),
          vy: numberOr(message.vy),
          damage: Math.max(
            0,
            Math.min(100, numberOr(message.damage, 20))
          )
        },
        client.id
      );

      break;
    }

    case "chat": {
      const room = rooms.get(client.roomId);

      const text = String(
        message.message || message.text || ""
      ).trim().slice(0, 180);

      if (!room || !text) return;

      broadcastRoom(room, {
        type: "chat",
        id: client.id,
        name: client.name,
        message: text
      });

      break;
    }

    default: {
      sendError(
        client,
        "Неизвестное событие: " + message.type
      );
    }
  }
}

wss.on("connection", ws => {
  const id = randomUUID();

  const client = {
    id,
    ws,
    name: "Игрок_" + id.slice(0, 4),
    roomId: null,
    player: null
  };

  clients.set(id, client);

  send(ws, {
    type: "welcome",
    id,
    name: client.name
  });

  sendRooms(ws);

  ws.on("message", raw => {
    try {
      const message = JSON.parse(raw.toString());
      handleMessage(client, message);
    } catch (error) {
      console.error("Ошибка обработки сообщения:", error);
      sendError(client, "Не удалось обработать сообщение.");
    }
  });

  ws.on("close", () => {
    leaveRoom(client);
    clients.delete(id);
    refreshRoomLists();
  });

  ws.on("error", error => {
    console.error("Ошибка WebSocket:", error.message);
  });
});

httpServer.listen(PORT, "0.0.0.0", () => {
  console.log("==================================");
  console.log(" IRON FRONT — сервер запущен");
  console.log(" HTTP: http://localhost:" + PORT);
  console.log(" WebSocket подключается к тому же адресу хостинга");
  console.log("==================================");
});

wss.on("error", error => {
  console.error("Ошибка сервера:", error.message);
});
