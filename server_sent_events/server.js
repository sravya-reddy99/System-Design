const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");

const PORT = Number(process.env.PORT) || 3000;
const HISTORY_SIZE = 50;

let nextId = 1;
const history = []; // recent notifications, so a client that reconnects can catch up
const clients = new Set(); // every open /events connection

// ---------------------------------------------------------------
// Turning an event into the text the browser expects
// ---------------------------------------------------------------

// One event is a few "field: value" lines, followed by a blank line.
//   id:    the browser remembers it and sends it back after a reconnect
//   event: a name. Leave it out and the browser fires the plain "message" event
//   data:  the payload. A payload with several lines becomes several data lines
function format({ id, event, data }) {
  let text = "";
  if (id !== undefined) text += `id: ${id}\n`;
  if (event) text += `event: ${event}\n`;
  for (const line of String(data).split(/\r?\n/)) text += `data: ${line}\n`;
  return text + "\n"; // the blank line ends the event
}

// An important event: gets an id, is remembered, and is sent to everyone
function publish(data) {
  const message = { id: nextId++, data };
  history.push(message);
  if (history.length > HISTORY_SIZE) history.shift();
  for (const res of clients) res.write(format(message));
  return message;
}

// A throwaway event: no id and not remembered. If you miss it, you miss it.
function broadcast(event, data) {
  for (const res of clients) res.write(format({ event, data }));
}

// ---------------------------------------------------------------
// GET /events : the stream
// ---------------------------------------------------------------

function handleEvents(req, res) {
  res.writeHead(200, {
    "Content-Type": "text/event-stream", // this is what makes it a stream of events
    "Cache-Control": "no-cache", // never reuse an old copy
    Connection: "keep-alive",
    "X-Accel-Buffering": "no", // tells nginx not to hold the data back
  });

  // Tell the browser to wait 3 seconds before reconnecting if the line drops
  res.write("retry: 3000\n\n");

  // If this is a reconnect, the browser tells us the last id it saw.
  // Send it everything it missed.
  const lastId = Number(req.headers["last-event-id"]);
  if (lastId) {
    const missed = history.filter((m) => m.id > lastId);
    for (const m of missed) res.write(format(m));
    console.log(`client reconnected after id ${lastId}, replayed ${missed.length} missed`);
  }

  clients.add(res);
  console.log(`client connected (${clients.size} connected)`);

  // The browser closed the page or lost the connection: stop writing to it
  req.on("close", () => {
    clients.delete(res);
    console.log(`client left (${clients.size} connected)`);
  });
}

// ---------------------------------------------------------------
// Small helpers for the other routes
// ---------------------------------------------------------------

function readBody(req, limit = 2048) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
      if (body.length > limit) {
        reject(new Error("body too big"));
        req.destroy();
      }
    });
    req.on("end", () => resolve(body));
    req.on("error", reject);
  });
}

function sendJson(res, status, object) {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(object));
}

// ---------------------------------------------------------------
// The server
// ---------------------------------------------------------------

const server = http.createServer(async (req, res) => {
  const { pathname } = new URL(req.url, `http://${req.headers.host}`);

  if (req.method === "GET" && pathname === "/") {
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    return res.end(fs.readFileSync(path.join(__dirname, "index.html")));
  }

  if (req.method === "GET" && pathname === "/events") {
    return handleEvents(req, res);
  }

  // Send a notification to everyone who is connected
  if (req.method === "POST" && pathname === "/notify") {
    try {
      const { text } = JSON.parse(await readBody(req));
      if (typeof text !== "string" || !text.trim() || text.length > 500) {
        return sendJson(res, 400, { error: "text must be 1 to 500 characters" });
      }
      const message = publish(text.trim());
      return sendJson(res, 202, { id: message.id });
    } catch {
      return sendJson(res, 400, { error: "send JSON like {\"text\": \"hello\"}" });
    }
  }

  // For the demo: cut every connection, as if the network dropped.
  // Browsers will reconnect by themselves and ask for what they missed.
  if (req.method === "POST" && pathname === "/drop") {
    const count = clients.size;
    for (const client of clients) client.end();
    clients.clear();
    return sendJson(res, 200, { dropped: count });
  }

  sendJson(res, 404, { error: "not found" });
});

// A clock tick every second. Throwaway event, so it has no id.
setInterval(() => {
  broadcast("tick", JSON.stringify({ time: new Date().toISOString(), connected: clients.size }));
}, 1000);

// A comment line (starts with a colon) is ignored by the browser, but it keeps
// proxies from closing a connection that looks idle.
setInterval(() => {
  for (const res of clients) res.write(": keep-alive\n\n");
}, 15000);

server.listen(PORT, () => {
  console.log(`Open http://localhost:${PORT} in your browser`);
});
