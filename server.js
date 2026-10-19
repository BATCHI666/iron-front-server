const { WebSocketServer, WebSocket } = require("ws");
const { randomUUID } = require("crypto");
const fs = require("fs");
const path = require("path");

const PORT = Number(process.env.PORT) || 3000;
const wss = new WebSocketServer({ port: PORT });

const clients = new Map();
const rooms = new Map();
// Social data is kept in memory for this server process; stable IDs live in each player's local account.
const socialProfiles = new Map();
const idToClient = new Map();
const friendships = new Map();
const friendRequests = new Map();
const directMessages = new Map();
const dmReadAt = new Map();
const DATA_DIR = process.env.IRONFRONT_DATA_DIR || __dirname;
try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch {}
const SOCIAL_DB = path.join(DATA_DIR, "social-data.json");
function loadSocialData() {
  try {
    const data = JSON.parse(fs.readFileSync(SOCIAL_DB, "utf8"));
    for (const p of data.profiles || []) if (p && /^\d{9}$/.test(String(p.gameId))) socialProfiles.set(String(p.gameId), p);
    for (const [id, list] of data.friendships || []) friendships.set(String(id), new Set((list || []).map(String)));
    for (const [id, list] of data.friendRequests || []) friendRequests.set(String(id), new Set((list || []).map(String)));
    for (const [key, list] of data.directMessages || []) directMessages.set(String(key), Array.isArray(list) ? list.slice(-300) : []);
    for (const [id, stamp] of data.dmReadAt || []) dmReadAt.set(String(id), Number(stamp) || 0);
  } catch {}
}
function persistSocial() {
  try {
    const data = {
      profiles: [...socialProfiles.values()],
      friendships: [...friendships.entries()].map(([id, set]) => [id, [...set]]),
      friendRequests: [...friendRequests.entries()].map(([id, set]) => [id, [...set]]),
      directMessages: [...directMessages.entries()],
      dmReadAt: [...dmReadAt.entries()]
    };
    const tmp = SOCIAL_DB + ".tmp";
    fs.writeFileSync(tmp, JSON.stringify(data), "utf8");
    fs.renameSync(tmp, SOCIAL_DB);
  } catch (error) { console.error("Не удалось сохранить данные друзей:", error.message); }
}
loadSocialData();
function getSet(map, key) { if (!map.has(key)) map.set(key, new Set()); return map.get(key); }
function cleanProfile(raw, gameId, fallbackName = "Игрок") {
  raw = raw && typeof raw === "object" ? raw : {};
  return {
    gameId,
    name: String(raw.name || fallbackName || "Игрок").trim().slice(0, 16) || "Игрок",
    level: Math.max(1, Math.min(9999, Number(raw.level) || 1)),
    xp: Math.max(0, Math.min(1e9, Number(raw.xp) || 0)),
    kills: Math.max(0, Math.min(1e9, Number(raw.kills) || 0)),
    deaths: Math.max(0, Math.min(1e9, Number(raw.deaths) || 0)),
    matches: Math.max(0, Math.min(1e9, Number(raw.matches) || 0)),
    playTimeSeconds: Math.max(0, Math.min(1e12, Number(raw.playTimeSeconds) || 0)),
    created: String(raw.created || new Date().toISOString()).slice(0, 40),
    tankColor: /^#[0-9a-f]{6}$/i.test(String(raw.tankColor || "")) ? String(raw.tankColor) : "#e51f2a"
  };
}
function profileFor(gameId) { return socialProfiles.get(String(gameId)) || null; }
let leaderboardCache = null;
let leaderboardCacheAt = 0;
const LEADERBOARD_REFRESH_MS = 30 * 60 * 1000;
function getLeaderboard() {
  const now = Date.now();
  if (!leaderboardCache || now - leaderboardCacheAt >= LEADERBOARD_REFRESH_MS) {
    const profiles = [...socialProfiles.values()].filter(p => p && /^\d{9}$/.test(String(p.gameId)));
    leaderboardCache = {
      topKills: [...profiles].sort((a, b) => (Number(b.kills) || 0) - (Number(a.kills) || 0) || String(a.name).localeCompare(String(b.name), 'ru')).slice(0, 50),
      topTime: [...profiles].sort((a, b) => (Number(b.playTimeSeconds) || 0) - (Number(a.playTimeSeconds) || 0) || String(a.name).localeCompare(String(b.name), 'ru')).slice(0, 50)
    };
    leaderboardCacheAt = now;
  }
  return { type: 'leaderboard', topKills: leaderboardCache.topKills, topTime: leaderboardCache.topTime, updatedAt: new Date(leaderboardCacheAt).toISOString() };
}
function dmKey(a, b) { return [String(a), String(b)].sort().join(":"); }
function areFriends(a, b) { return getSet(friendships, String(a)).has(String(b)); }
function profileWithPresence(gameId) {
  const profile = profileFor(gameId) || { gameId: String(gameId), name: "Игрок " + String(gameId), level: 1, xp: 0, kills: 0, deaths: 0, matches: 0, created: new Date().toISOString(), tankColor: "#829f60" };
  const live = idToClient.get(String(gameId));
  const room = live?.roomId ? rooms.get(live.roomId) : null;
  let activity = live?.activity || "В меню";
  if (room) activity = room.started ? "В сетевом бою" : "В комнате ожидания";
  return {
    ...profile,
    online: Boolean(live && live.ws.readyState === WebSocket.OPEN),
    activity: live && live.ws.readyState === WebSocket.OPEN ? activity : "Не в сети",
    roomId: room?.id || null,
    roomName: room?.name || null,
    roomStarted: Boolean(room?.started),
    roomHasPassword: Boolean(room?.password)
  };
}
function sendFriendLists(client) {
  if (!client.gameId) return;
  const friends = [...getSet(friendships, client.gameId)].map(profileWithPresence).filter(Boolean);
  const requests = [...getSet(friendRequests, client.gameId)].map(profileWithPresence).filter(Boolean);
  send(client.ws, { type: "friendsList", friends });
  send(client.ws, { type: "friendRequests", requests });
}
function pushFriendLists(gameId) {
  const c = idToClient.get(String(gameId));
  if (c) sendFriendLists(c);
}
function refreshFriendsOf(gameId) {
  for (const friendId of getSet(friendships, String(gameId))) pushFriendLists(friendId);
  pushFriendLists(String(gameId));
}

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
    if (player.id !== exceptId) send(player.ws, data);
  }
}

function roomInfo(room) {
  const members = [...room.players.values()].map(p => ({
    id: p.id,
    name: p.name,
    color: p.player?.color || "#829f60",
    x: Number.isFinite(p.player?.x) ? p.player.x : 170,
    y: Number.isFinite(p.player?.y) ? p.player.y : 170,
    angle: Number.isFinite(p.player?.angle) ? p.player.angle : 0,
    turret: Number.isFinite(p.player?.turret) ? p.player.turret : 0,
    hp: Number.isFinite(p.player?.hp) ? p.player.hp : 100,
    maxHp: Number.isFinite(p.player?.maxHp) ? p.player.maxHp : 100,
    armor: Number.isFinite(p.player?.armor) ? p.player.armor : 0
  }));

  return {
    id: room.id,
    name: room.name,

    // Поля, которые ожидает HTML-клиент
    ownerId: room.hostId,
    hostId: room.hostId,
    players: room.players.size,
    playerNames: members.map(p => p.name),
    hasPassword: Boolean(room.password),

    maxPlayers: room.maxPlayers,
    started: room.started,

    // Полный список нужен для запуска боя
    members
  };
}

function sendRooms(ws) {
  // Показываем и уже начавшиеся комнаты: в них можно подключиться к текущему бою.
  const list = [...rooms.values()]
    .map(roomInfo);

  send(ws, { type: "rooms", rooms: list });
}

function updateRoom(room) {
  if (rooms.has(room.id)) {
    const info = roomInfo(room);

    broadcastRoom(room, {
      type: "roomUpdate",
      room: info,
      players: info.members
    });
  }

  for (const client of clients.values()) {
    if (!client.roomId) sendRooms(client.ws);
  }
  // Keep friends' online/activity/room indicators in sync when a room changes.
  for (const player of room.players.values()) {
    const memberClient = clients.get(player.id);
    if (memberClient?.gameId) refreshFriendsOf(memberClient.gameId);
  }
}

function leaveRoom(client) {
  if (!client.roomId) return;

  const room = rooms.get(client.roomId);
  client.roomId = null;
  client.player = null;

  if (!room) {
    send(client.ws, { type: "left" });
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
  if (client.gameId) refreshFriendsOf(client.gameId);
}

function joinRoom(client, requestedId, password = "") {
  const room = rooms.get(String(requestedId || ""));

  if (!room) {
    sendError(client, "Комната не найдена.");
    return;
  }

  // В начавшийся бой тоже можно подключаться, если есть свободные места.
  if (room.password && String(password) !== room.password) {
    sendError(client, "Неверный пароль комнаты.");
    return;
  }

  if (room.players.size >= room.maxPlayers) {
    sendError(client, "Комната заполнена.");
    return;
  }

  if (client.roomId === room.id) {
    send(client.ws, {
      type: "joined",
      room: roomInfo(room),
      id: client.id
    });
    return;
  }

  if (client.roomId) leaveRoom(client);

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

  broadcastRoom(room, {
    type: "playerJoined",
    id: client.id,
    playerId: client.id,
    player: { id: client.id, name: client.name },
    name: client.name,
    room: info
  }, client.id);

  updateRoom(room);
  if (client.gameId) refreshFriendsOf(client.gameId);

  if (room.started) {
    const players = [...room.players.values()].map((p, index) => ({
      id: p.id,
      name: p.name,
      color: p.player?.color || "#829f60",
      x: Number.isFinite(p.player?.x) ? p.player.x : 170 + (index % 4) * 85,
      y: Number.isFinite(p.player?.y) ? p.player.y : 170 + Math.floor(index / 4) * 85,
      angle: Number.isFinite(p.player?.angle) ? p.player.angle : 0,
      turret: Number.isFinite(p.player?.turret) ? p.player.turret : 0,
      hp: Number.isFinite(p.player?.hp) ? p.player.hp : 100,
      maxHp: Number.isFinite(p.player?.maxHp) ? p.player.maxHp : 100
    }));

    send(client.ws, {
      type: "started",
      roomId: room.id,
      players
    });
  }
}

function startGame(client) {
  const room = rooms.get(client.roomId);

  if (!room) {
    sendError(client, "Сначала создай комнату или присоединись к ней.");
    return;
  }

  if (room.hostId !== client.id) {
    sendError(client, "Начать бой может только создатель комнаты.");
    return;
  }

  if (room.started) {
    sendError(client, "Бой уже начался.");
    return;
  }

  room.started = true;
  room.claimedCoins = new Set();
  room.claimedPickups = new Set();
  room.chatMessageIds = new Set();

  const members = [...room.players.values()];

  const players = members.map((p, index) => ({
    id: p.id,
    name: p.name,
    color: p.player?.color || "#829f60",

    // Разводим танки по разным точкам старта
    x: 170 + (index % 4) * 85,
    y: 170 + Math.floor(index / 4) * 85,
    angle: 0,
    turret: 0,
    hp: 100,
    maxHp: 100
  }));

  const message = {
    type: "started",
    roomId: room.id,
    players
  };

  // Отправляем событие всем участникам, включая создателя
  broadcastRoom(room, message);
  updateRoom(room);
  for (const p of room.players.values()) if (p.ws && p.ws.readyState === WebSocket.OPEN) { const c = clients.get(p.id); if (c?.gameId) refreshFriendsOf(c.gameId); }
}

function handleMessage(client, message) {
  if (!message || typeof message.type !== "string") return;

  switch (message.type) {
    case "hello":
    case "setName": {
      if (typeof message.name === "string") {
        const name = message.name.trim().slice(0, 16);
        if (name) client.name = name;
      }

      const requestedGameId = String(message.gameId || "");
      if (/^\d{9}$/.test(requestedGameId)) {
        if (client.gameId && idToClient.get(client.gameId) === client) idToClient.delete(client.gameId);
        client.gameId = requestedGameId;
        idToClient.set(client.gameId, client);
        const profile = cleanProfile(message.profile, client.gameId, client.name);
        if (typeof message.name === "string" && message.name.trim()) profile.name = message.name.trim().slice(0, 16);
        client.name = profile.name;
        socialProfiles.set(client.gameId, profile);
        persistSocial();
      }

      send(client.ws, {
        type: "welcome",
        id: client.id,
        name: client.name,
        gameId: client.gameId || null
      });
      if (client.gameId) {
        const unread = [];
        for (const list of directMessages.values()) for (const item of list) {
          const lastRead = dmReadAt.get(`${client.gameId}:${String(item.fromGameId)}`) || 0;
          if (String(item.toGameId) === String(client.gameId) && Date.parse(item.createdAt) > lastRead) unread.push(item);
        }
        unread.sort((a,b)=>Date.parse(a.createdAt)-Date.parse(b.createdAt));
        if (unread.length) send(client.ws, { type: "friendDmNotifications", messages: unread.slice(-50) });
      }
      sendFriendLists(client);
      refreshFriendsOf(client.gameId);
      // Deliver pending friend requests when an account reconnects.
      if (client.gameId) {
        for (const requesterId of getSet(friendRequests, client.gameId)) {
          send(client.ws, { type: "friendRequestReceived", from: requesterId, profile: profileFor(requesterId) });
        }
      }
      sendRooms(client.ws);
      break;
    }

    case "profileUpdate": {
      if (!client.gameId) return;
      const profile = cleanProfile(message.profile, client.gameId, message.name || client.name);
      client.name = profile.name;
      socialProfiles.set(client.gameId, profile);
      persistSocial();
      sendFriendLists(client);
      break;
    }
    case "getLeaderboard": {
      send(client.ws, getLeaderboard());
      break;
    }
    case "presenceUpdate": {
      const allowed = new Set(["В меню", "Одиночная игра", "Мультиплеер"]);
      if (allowed.has(String(message.activity || ""))) client.activity = String(message.activity);
      if (client.gameId) refreshFriendsOf(client.gameId);
      break;
    }
    case "friendConversation": {
      const otherId = String(message.withGameId || "");
      if (!client.gameId || !/^\d{9}$/.test(otherId) || !areFriends(client.gameId, otherId)) {
        send(client.ws, { type: "friendError", message: "Личный чат доступен только друзьям." }); return;
      }
      dmReadAt.set(`${client.gameId}:${otherId}`, Date.now());
      persistSocial();
      send(client.ws, { type: "friendConversation", withGameId: otherId, requestId: String(message.requestId || "").slice(0, 80), messages: directMessages.get(dmKey(client.gameId, otherId)) || [] });
      break;
    }
    case "friendMarkRead": {
      const otherId = String(message.withGameId || "");
      if (!client.gameId || !/^\d{9}$/.test(otherId) || !areFriends(client.gameId, otherId)) return;
      dmReadAt.set(`${client.gameId}:${otherId}`, Date.now());
      persistSocial();
      send(client.ws, { type: "friendReadMarked", withGameId: otherId });
      break;
    }
    case "friendMessage": {
      const toId = String(message.toGameId || "");
      const body = String(message.message || "").trim().slice(0, 500);
      if (!client.gameId || !/^\d{9}$/.test(toId) || !areFriends(client.gameId, toId) || !body) {
        send(client.ws, { type: "friendError", message: "Сообщения можно отправлять только друзьям." }); return;
      }
      const entry = { messageId: randomUUID(), fromGameId: client.gameId, toGameId: toId, fromName: client.name, message: body, createdAt: new Date().toISOString() };
      const key = dmKey(client.gameId, toId);
      const history = directMessages.get(key) || [];
      history.push(entry);
      directMessages.set(key, history.slice(-300));
      persistSocial();
      send(client.ws, { type: "friendMessageSent", ...entry, toGameId: toId, requestId: String(message.requestId || "").slice(0, 80) });
      const target = idToClient.get(toId);
      if (target && target.ws.readyState === WebSocket.OPEN) send(target.ws, { type: "friendMessageReceived", ...entry });
      break;
    }
    case "friendJoin": {
      const targetId = String(message.gameId || "");
      if (!client.gameId || !getSet(friendships, client.gameId).has(targetId)) {
        send(client.ws, { type: "friendJoinResult", ok: false, message: "Зайти можно только к игроку из списка друзей." });
        return;
      }
      const target = idToClient.get(targetId);
      if (!target || target.ws.readyState !== WebSocket.OPEN) {
        send(client.ws, { type: "friendJoinResult", ok: false, message: "Друг сейчас не в сети." });
        return;
      }
      const targetRoom = target.roomId ? rooms.get(target.roomId) : null;
      if (!targetRoom) {
        send(client.ws, { type: "friendJoinResult", ok: false, message: "Друг сейчас не в сетевой комнате. Он может играть в одиночном режиме или находиться в меню." });
        return;
      }
      if (targetRoom.players.size >= targetRoom.maxPlayers && !targetRoom.players.has(client.id)) {
        send(client.ws, { type: "friendJoinResult", ok: false, message: "Комната друга заполнена." });
        return;
      }
      send(client.ws, { type: "friendRoomFound", roomId: targetRoom.id, roomName: targetRoom.name, hasPassword: Boolean(targetRoom.password), started: Boolean(targetRoom.started) });
      break;
    }
    case "friendsList": {
      sendFriendLists(client);
      break;
    }
    case "friendRequests": {
      sendFriendLists(client);
      break;
    }
    case "friendSearch": {
      const gameId = String(message.gameId || "");
      if (!/^\d{9}$/.test(gameId)) {
        send(client.ws, { type: "friendSearchResult", id: gameId, profile: null });
        return;
      }
      const profile = profileFor(gameId);
      send(client.ws, { type: "friendSearchResult", id: gameId, profile: profile && gameId !== client.gameId ? profileWithPresence(gameId) : null });
      break;
    }
    case "friendRequest": {
      const targetId = String(message.gameId || "");
      if (!client.gameId || !/^\d{9}$/.test(targetId) || targetId === client.gameId) {
        send(client.ws, { type: "friendError", message: "Нельзя отправить заявку этому игроку." }); return;
      }
      if (!profileFor(targetId)) { send(client.ws, { type: "friendError", message: "Игрок не найден на сервере. Попроси его один раз подключиться к игре." }); return; }
      if (getSet(friendships, client.gameId).has(targetId)) { send(client.ws, { type: "friendError", message: "Этот игрок уже у тебя в друзьях." }); return; }
      const requests = getSet(friendRequests, targetId);
      if (requests.has(client.gameId)) { send(client.ws, { type: "friendError", message: "Заявка уже отправлена." }); return; }
      requests.add(client.gameId);
      persistSocial();
      send(client.ws, { type: "friendRequestSent", message: "Заявка в друзья отправлена." });
      const target = idToClient.get(targetId);
      if (target) send(target.ws, { type: "friendRequestReceived", from: client.gameId, profile: profileFor(client.gameId) });
      pushFriendLists(targetId);
      break;
    }
    case "friendAccept": {
      const fromId = String(message.gameId || "");
      if (!client.gameId || !getSet(friendRequests, client.gameId).has(fromId)) { send(client.ws, { type: "friendError", message: "Заявка не найдена." }); return; }
      getSet(friendRequests, client.gameId).delete(fromId);
      getSet(friendships, client.gameId).add(fromId);
      getSet(friendships, fromId).add(client.gameId);
      persistSocial();
      send(client.ws, { type: "friendAccepted", gameId: fromId });
      const fromClient = idToClient.get(fromId);
      if (fromClient) send(fromClient.ws, { type: "friendAccepted", gameId: client.gameId, profile: profileFor(client.gameId) });
      pushFriendLists(client.gameId); pushFriendLists(fromId);
      break;
    }
    case "friendDecline": {
      const fromId = String(message.gameId || "");
      if (client.gameId) getSet(friendRequests, client.gameId).delete(fromId);
      persistSocial();
      sendFriendLists(client);
      break;
    }
    case "friendRemove": {
      const friendId = String(message.gameId || "");
      if (client.gameId) getSet(friendships, client.gameId).delete(friendId);
      getSet(friendships, friendId).delete(client.gameId);
      persistSocial();
      send(client.ws, { type: "friendRemoved", gameId: friendId });
      pushFriendLists(client.gameId); pushFriendLists(friendId);
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

      if (client.roomId) leaveRoom(client);

      const maxPlayers = Math.max(
        2,
        Math.min(12, Number(message.maxPlayers) || 4)
      );

      const password = String(message.password || "").slice(0, 40);
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

      // Автоматически запускаем бой сразу после создания комнаты.
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
      sendRooms(client.ws);
      break;
    }

    case "startGame": {
      startGame(client);
      break;
    }

    case "state": {
      const room = rooms.get(client.roomId);
      if (!room || !room.started || !message.player) return;

      const p = message.player;

      // Проверяем числа, чтобы не принимать некорректные координаты
      const validNumber = value =>
        typeof value === "number" && Number.isFinite(value);

      if (
        !validNumber(p.x) ||
        !validNumber(p.y) ||
        Math.abs(p.x) > 10000 ||
        Math.abs(p.y) > 10000
      ) return;

      const playerState = {
        x: p.x,
        y: p.y,
        angle: validNumber(p.angle) ? p.angle : 0,
        turret: validNumber(p.turret) ? p.turret : 0,
        hp: validNumber(p.hp) ? Math.max(0, Math.min(1000, p.hp)) : 100,
        maxHp: validNumber(p.maxHp) ? Math.max(1, Math.min(1000, p.maxHp)) : 100,
        armor: validNumber(p.armor) ? Math.max(0, Math.min(100, p.armor)) : 0,
        name: client.name,
        color: typeof p.color === "string" ? p.color.slice(0, 24) : "#829f60"
      };

      client.player = playerState;

      const roomPlayer = room.players.get(client.id);
      if (roomPlayer) roomPlayer.player = playerState;

      broadcastRoom(room, {
        type: "state",
        id: client.id,
        playerId: client.id,
        player: playerState
      }, client.id);
      break;
    }

    case "shoot": {
      const room = rooms.get(client.roomId);
      if (!room || !room.started) return;

      const numberOr = (v, fallback = 0) =>
        typeof v === "number" && Number.isFinite(v) ? v : fallback;

      broadcastRoom(room, {
        type: "shoot",
        id: client.id,
        playerId: client.id,
        x: numberOr(message.x),
        y: numberOr(message.y),
        vx: numberOr(message.vx),
        vy: numberOr(message.vy),
        damage: Math.max(0, Math.min(100, numberOr(message.damage, 20)))
      }, client.id);
      break;
    }

    case "coinCollect": {
      const room = rooms.get(client.roomId);
      const coinId = Number(message.coinId);
      if (!room || !room.started || !Number.isInteger(coinId) || coinId < 0 || coinId > 10000) return;
      if (!room.claimedCoins) room.claimedCoins = new Set();
      if (room.claimedCoins.has(coinId)) return;
      room.claimedCoins.add(coinId);
      broadcastRoom(room, { type: "coinCollect", coinId, collectorId: client.id });
      break;
    }

    case "pickupCollect": {
      const room = rooms.get(client.roomId);
      const kind = String(message.kind || "");
      const pickupId = Number(message.pickupId);
      const validKind = kind === "armor" || kind === "medkit";
      if (!room || !room.started || !validKind || !Number.isInteger(pickupId) || pickupId < 1 || pickupId > 10000) return;
      if (!room.claimedPickups) room.claimedPickups = new Set();
      const key = `${kind}:${pickupId}`;
      if (room.claimedPickups.has(key)) return;
      room.claimedPickups.add(key);
      broadcastRoom(room, { type: "pickupCollect", kind, pickupId, collectorId: client.id });
      break;
    }

    case "chat": {
      const room = rooms.get(client.roomId);
      const text = String(message.message || message.text || "")
        .trim()
        .slice(0, 180);

      if (!room || !text) return;
      const clientMsgId = typeof message.clientMsgId === "string" ? message.clientMsgId.slice(0, 80) : "";
      if (clientMsgId) {
        if (!room.chatMessageIds) room.chatMessageIds = new Set();
        if (room.chatMessageIds.has(clientMsgId)) return;
        room.chatMessageIds.add(clientMsgId);
        if (room.chatMessageIds.size > 2000) room.chatMessageIds.clear();
      }

      broadcastRoom(room, {
        type: "chat",
        id: client.id,
        name: client.name,
        message: text,
        clientMsgId
      });
      break;
    }

    default:
      sendError(client, "Неизвестное событие: " + message.type);
  }
}

wss.on("connection", ws => {
  const id = randomUUID();

  const client = {
    id,
    ws,
    name: "Игрок_" + id.slice(0, 4),
    gameId: null,
    roomId: null,
    player: null,
    activity: "В меню"
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
      sendError(client, "Не удалось обработать сообщение.");
    }
  });

  ws.on("close", () => {
    leaveRoom(client);
    clients.delete(id);
    if (client.gameId && idToClient.get(client.gameId) === client) { idToClient.delete(client.gameId); refreshFriendsOf(client.gameId); }
  });

  ws.on("error", error => {
    console.error("Ошибка WebSocket:", error.message);
  });
});

wss.on("listening", () => {
  console.log("==================================");
  console.log(" IRON FRONT — сервер запущен");
  console.log(" Адрес: ws://localhost:" + PORT);
  console.log("==================================");
});

wss.on("error", error => {
  console.error("Ошибка сервера:", error.message);
});
process.on("SIGTERM", () => { try { persistSocial(); } finally { wss.close(() => process.exit(0)); setTimeout(() => process.exit(0), 1500).unref(); } });
process.on("SIGINT", () => { try { persistSocial(); } finally { wss.close(() => process.exit(0)); setTimeout(() => process.exit(0), 1500).unref(); } });
