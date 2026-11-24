// npm init -y, npm i express socket.io
const express = require("express");
const http = require("http");
const path = require("path");
const fs = require("fs");
const { Server } = require("socket.io");

const app = express();
const httpserver = http.createServer(app);
const io = new Server(httpserver);

// Serve the client so you can also open http://localhost:8080
app.get("/", (req, res) => res.sendFile(path.join(__dirname, "client.html")));

/* ------------------------------------------------------------------ *
 * CONFIG
 * ------------------------------------------------------------------ */
const ROOMS = ["General", "Doubts", "Announcements"];
const HISTORY_LIMIT = 30;     // messages sent on join / room switch
const RATE_LIMIT_COUNT = 5;   // max messages ...
const RATE_LIMIT_WINDOW = 3000; // ... per 3 seconds
const MAX_MSG_LENGTH = 500;
const MAX_NAME_LENGTH = 20;

/* ------------------------------------------------------------------ *
 * FEATURE 4: MESSAGE PERSISTENCE (JSON file)
 * Messages live in memory for fast reads and are saved to messages.json
 * so history survives refreshes and server restarts.
 * ------------------------------------------------------------------ */
const DB_FILE = path.join(__dirname, "messages.json");
let messages = [];
try {
  messages = JSON.parse(fs.readFileSync(DB_FILE, "utf8"));
} catch {
  messages = []; // first run or unreadable file -> start empty
}

function saveMessages() {
  fs.writeFile(DB_FILE, JSON.stringify(messages, null, 2), (err) => {
    if (err) console.error("Could not save messages:", err.message);
  });
}

function getHistory(room) {
  return messages.filter((m) => m.room === room).slice(-HISTORY_LIMIT);
}

/* ------------------------------------------------------------------ *
 * FEATURE 3: ONLINE USERS  (ephemeral state: Map<socket.id, username>)
 * ------------------------------------------------------------------ */
const users = new Map();

// Usernames of everyone currently inside a room.
// io.sockets.adapter.rooms.get(room) is the Set of socket ids in that room.
function getRoomUsers(room) {
  const ids = io.sockets.adapter.rooms.get(room) || new Set();
  return [...ids].map((id) => users.get(id)).filter(Boolean);
}

function broadcastUsers(room) {
  io.to(room).emit("users", getRoomUsers(room));
}

function isNameTaken(name) {
  const lower = name.toLowerCase();
  for (const existing of users.values()) {
    if (existing.toLowerCase() === lower) return true;
  }
  return false;
}

function systemMessage(room, text) {
  io.to(room).emit("system", { room, text, timestamp: Date.now() });
}

// Put a socket in a room and sync it (history + user lists)
function enterRoom(socket, room) {
  socket.join(room);
  socket.room = room;
  socket.emit("history", { room, messages: getHistory(room) });
  broadcastUsers(room);
}

/* ------------------------------------------------------------------ *
 * CONNECTION HANDLING
 * ------------------------------------------------------------------ */
io.on("connection", (socket) => {
  console.log("Socket connected:", socket.id);

  // FEATURE 5: per-socket list of recent message timestamps
  socket.sentTimes = [];

  // FEATURE 1: username is stored ONCE on the socket object,
  // so we never send it again with each message.
  socket.on("join", (data, ack) => {
    if (typeof ack !== "function") return;
    const username = String((data && data.username) || "").trim().slice(0, MAX_NAME_LENGTH);

    if (!username) return ack({ ok: false, error: "Please enter a username." });
    if (isNameTaken(username)) return ack({ ok: false, error: "That username is already taken." });

    socket.username = username;
    users.set(socket.id, username);

    ack({ ok: true, username, rooms: ROOMS, room: "General" });
    enterRoom(socket, "General");
    systemMessage("General", `${username} joined the room`);
    console.log(`${username} joined. Online: ${users.size}`);
  });

  // FEATURE 2: switch rooms
  socket.on("switch-room", (room) => {
    if (!socket.username || !ROOMS.includes(room) || room === socket.room) return;

    const oldRoom = socket.room;
    socket.leave(oldRoom);
    systemMessage(oldRoom, `${socket.username} left the room`);
    io.to(oldRoom).emit("typing", { username: socket.username, isTyping: false });
    broadcastUsers(oldRoom);

    enterRoom(socket, room);
    systemMessage(room, `${socket.username} joined the room`);
  });

  // Chat message
  socket.on("message", (text) => {
    if (!socket.username) return;
    if (typeof text !== "string") return;
    text = text.trim();
    if (!text || text.length > MAX_MSG_LENGTH) return;

    // FEATURE 5: sliding-window rate limit (5 messages / 3 seconds)
    const now = Date.now();
    socket.sentTimes = socket.sentTimes.filter((t) => now - t < RATE_LIMIT_WINDOW);
    if (socket.sentTimes.length >= RATE_LIMIT_COUNT) {
      return socket.emit("rate-limited", "Slow down! Max 5 messages every 3 seconds.");
    }
    socket.sentTimes.push(now);

    // Server decides username, room and timestamp - the client can't fake them
    const msg = { username: socket.username, room: socket.room, text, timestamp: now };
    messages.push(msg);
    saveMessages();

    // FEATURE 2: only sockets inside this room receive the packet
    io.to(socket.room).emit("message", msg);
  });

  // FEATURE 3: typing indicator (sent to everyone in the room except the sender)
  socket.on("typing", (isTyping) => {
    if (!socket.username) return;
    socket.to(socket.room).emit("typing", { username: socket.username, isTyping: !!isTyping });
  });

  // Cleanup: remove stale user automatically
  socket.on("disconnect", () => {
    if (!socket.username) return;
    const room = socket.room;
    users.delete(socket.id);
    // socket has already left its rooms by now, so this list is already clean
    io.to(room).emit("typing", { username: socket.username, isTyping: false });
    systemMessage(room, `${socket.username} left the chat`);
    broadcastUsers(room);
    console.log(`${socket.username} disconnected. Online: ${users.size}`);
  });
});

httpserver.listen(8080, () => {
  console.log("Server started on http://localhost:8080");
});
