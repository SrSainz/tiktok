const http = require("http");
const https = require("https");
const { Client } = require("ssh2");

function getEnv(name, fallback = "") {
  const value = process.env[name];
  return typeof value === "string" && value.trim() ? value.trim() : fallback;
}

function getConfig() {
  const host = getEnv("NAS_SSH_HOST", "nas.polysainz.com");
  const port = Number(getEnv("NAS_SSH_PORT", "22"));
  const username = getEnv("NAS_SSH_USER", "SrSainz");
  const privateKey = getEnv("NAS_SSH_PRIVATE_KEY").replace(/\\n/g, "\n");
  const backendHost = getEnv("NAS_BACKEND_HOST", "127.0.0.1");
  const backendPort = Number(getEnv("NAS_BACKEND_PORT", "8780"));
  const publicBackendUrl = getEnv("NAS_PUBLIC_BACKEND_URL", getEnv("BACKEND_PUBLIC_URL", ""));
  return { host, port, username, privateKey, backendHost, backendPort, publicBackendUrl };
}

function validateConfig(config) {
  const missing = [];
  if (!config.host) missing.push("NAS_SSH_HOST");
  if (!config.username) missing.push("NAS_SSH_USER");
  if (!config.privateKey) missing.push("NAS_SSH_PRIVATE_KEY");
  if (!config.backendHost) missing.push("NAS_BACKEND_HOST");
  if (!config.backendPort || Number.isNaN(config.backendPort)) missing.push("NAS_BACKEND_PORT");
  return missing;
}

function buildUpstreamPath(req, basePrefix) {
  const pathParam = req.query?.path;
  const pathParts = Array.isArray(pathParam) ? pathParam : pathParam ? [pathParam] : [];
  const suffix = pathParts.length ? `/${pathParts.map((p) => encodeURIComponent(String(p))).join("/")}` : "";

  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(req.query || {})) {
    if (key === "path") continue;
    if (Array.isArray(value)) {
      value.forEach((entry) => search.append(key, String(entry)));
    } else if (value !== undefined) {
      search.append(key, String(value));
    }
  }
  const qs = search.toString();
  return `${basePrefix}${suffix}${qs ? `?${qs}` : ""}`;
}

function filterRequestHeaders(headers, backendHost, backendPort) {
  const next = { ...headers };
  delete next.host;
  delete next.connection;
  delete next["content-length"];
  next.host = `${backendHost}:${backendPort}`;
  return next;
}

function applyResponseHeaders(res, headers) {
  for (const [key, value] of Object.entries(headers || {})) {
    if (key.toLowerCase() === "connection") continue;
    if (typeof value === "undefined") continue;
    res.setHeader(key, value);
  }
}

function proxyViaPublicBackend(req, res, basePrefix, publicBackendUrl, failLabel = "Public backend request failed") {
  let target;
  try {
    target = new URL(publicBackendUrl);
  } catch (err) {
    res.statusCode = 500;
    res.setHeader("content-type", "application/json; charset=utf-8");
    res.end(JSON.stringify({ error: "Invalid public backend URL", detail: String(err) }));
    return;
  }

  const upstreamPath = buildUpstreamPath(req, basePrefix);
  const client = target.protocol === "https:" ? https : http;
  const upstreamReq = client.request(
    {
      protocol: target.protocol,
      hostname: target.hostname,
      port: target.port || (target.protocol === "https:" ? 443 : 80),
      method: req.method,
      path: `${target.pathname.replace(/\/+$/, "")}${upstreamPath}`,
      headers: filterRequestHeaders(req.headers || {}, target.hostname, target.port || (target.protocol === "https:" ? 443 : 80)),
      agent: false,
    },
    (upstreamRes) => {
      res.statusCode = upstreamRes.statusCode || 502;
      applyResponseHeaders(res, upstreamRes.headers);
      upstreamRes.pipe(res);
    }
  );

  upstreamReq.on("error", (proxyErr) => {
    if (!res.headersSent) {
      res.statusCode = 502;
      res.setHeader("content-type", "application/json; charset=utf-8");
      res.end(JSON.stringify({ error: failLabel, detail: String(proxyErr) }));
    } else {
      res.end();
    }
  });

  req.on("aborted", () => {
    upstreamReq.destroy();
  });

  req.pipe(upstreamReq);
}

function proxyViaNas(req, res, basePrefix) {
  const config = getConfig();
  const missing = validateConfig(config);
  if (missing.length) {
    if (config.publicBackendUrl) {
      proxyViaPublicBackend(req, res, basePrefix, config.publicBackendUrl);
      return;
    }
    res.statusCode = 500;
    res.setHeader("content-type", "application/json; charset=utf-8");
    res.end(JSON.stringify({ error: "Missing Vercel env vars", missing }));
    return;
  }

  const conn = new Client();
  let closed = false;
  const closeConn = () => {
    if (!closed) {
      closed = true;
      conn.end();
    }
  };

  const fail = (status, message, extra = {}) => {
    if (config.publicBackendUrl && !res.headersSent) {
      proxyViaPublicBackend(req, res, basePrefix, config.publicBackendUrl, message);
      closeConn();
      return;
    }
    if (res.headersSent) {
      res.end();
      closeConn();
      return;
    }
    res.statusCode = status;
    res.setHeader("content-type", "application/json; charset=utf-8");
    res.end(JSON.stringify({ error: message, ...extra }));
    closeConn();
  };

  conn
    .on("ready", () => {
      conn.forwardOut("127.0.0.1", 0, config.backendHost, config.backendPort, (err, stream) => {
        if (err) {
          fail(502, "SSH tunnel error", { detail: String(err) });
          return;
        }

        const upstreamPath = buildUpstreamPath(req, basePrefix);
        const upstreamReq = http.request(
          {
            createConnection: () => stream,
            host: config.backendHost,
            port: config.backendPort,
            method: req.method,
            path: upstreamPath,
            headers: filterRequestHeaders(req.headers || {}, config.backendHost, config.backendPort),
            agent: false,
          },
          (upstreamRes) => {
            res.statusCode = upstreamRes.statusCode || 502;
            applyResponseHeaders(res, upstreamRes.headers);
            upstreamRes.pipe(res);
            upstreamRes.on("end", closeConn);
          }
        );

        upstreamReq.on("error", (proxyErr) => {
          fail(502, "Upstream request failed", { detail: String(proxyErr) });
        });

        req.on("aborted", () => {
          upstreamReq.destroy();
          closeConn();
        });

        req.pipe(upstreamReq);
      });
    })
    .on("error", (err) => {
      fail(502, "SSH connection failed", { detail: String(err) });
    })
    .connect({
      host: config.host,
      port: config.port,
      username: config.username,
      privateKey: config.privateKey,
      readyTimeout: 20000,
      keepaliveInterval: 10000,
      keepaliveCountMax: 3,
    });
}

module.exports = { proxyViaNas };
