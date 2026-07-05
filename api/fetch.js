const dns = require('dns').promises;
const net = require('net');

const MAX_BYTES = Number(process.env.RELAY_MAX_BYTES || 1024 * 1024); // 1 MiB
const DEFAULT_TIMEOUT_MS = 12000;
const MAX_TIMEOUT_MS = 25000;

function json(res, status, body) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.end(JSON.stringify(body));
}

function isPrivateIp(ip) {
  if (!net.isIP(ip)) return true;
  if (ip === '127.0.0.1' || ip === '0.0.0.0' || ip === '::1') return true;
  if (ip.startsWith('10.')) return true;
  if (ip.startsWith('192.168.')) return true;
  const parts = ip.split('.').map(Number);
  if (parts.length === 4) {
    if (parts[0] === 172 && parts[1] >= 16 && parts[1] <= 31) return true;
    if (parts[0] === 169 && parts[1] === 254) return true;
    if (parts[0] === 100 && parts[1] >= 64 && parts[1] <= 127) return true;
    if (parts[0] === 127) return true;
  }
  const lower = ip.toLowerCase();
  if (lower.startsWith('fc') || lower.startsWith('fd') || lower.startsWith('fe80:')) return true;
  return false;
}

async function assertSafeUrl(rawUrl) {
  let url;
  try {
    url = new URL(rawUrl);
  } catch {
    throw new Error('invalid url');
  }
  if (!['http:', 'https:'].includes(url.protocol)) throw new Error('protocol blocked');
  if (url.username || url.password) throw new Error('credentials in URL blocked');
  const host = url.hostname.toLowerCase();
  if (host === 'localhost' || host.endsWith('.local')) throw new Error('local host blocked');
  if (net.isIP(host) && isPrivateIp(host)) throw new Error('private IP blocked');
  const records = await dns.lookup(host, { all: true, verbatim: true });
  if (!records.length) throw new Error('dns lookup failed');
  for (const record of records) {
    if (isPrivateIp(record.address)) throw new Error('private DNS target blocked');
  }
  return url.toString();
}

module.exports = async function handler(req, res) {
  if (req.method !== 'GET') return json(res, 405, { ok: false, error: 'method not allowed' });

  const requiredToken = process.env.RELAY_TOKEN || '';
  if (requiredToken) {
    const got = String(req.headers.authorization || '').replace(/^Bearer\s+/i, '');
    if (got !== requiredToken) return json(res, 401, { ok: false, error: 'unauthorized' });
  }

  try {
    const target = await assertSafeUrl(req.query.url || '');
    const timeoutMs = Math.min(Number(req.query.timeout || DEFAULT_TIMEOUT_MS), MAX_TIMEOUT_MS);
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    const upstream = await fetch(target, {
      signal: controller.signal,
      redirect: 'follow',
      headers: {
        'user-agent': req.headers['x-source-user-agent'] || 'Mozilla/5.0 (compatible; ProxyScraperRelay/1.0)',
        'accept': 'text/html,application/json,text/plain,*/*',
      },
    });
    clearTimeout(timer);

    const chunks = [];
    let total = 0;
    for await (const chunk of upstream.body) {
      total += chunk.length;
      if (total > MAX_BYTES) throw new Error('response too large');
      chunks.push(chunk);
    }
    const buffer = Buffer.concat(chunks);
    const type = upstream.headers.get('content-type') || '';
    const asText = type.includes('text') || type.includes('json') || type.includes('xml') || !type;

    return json(res, 200, {
      ok: true,
      status: upstream.status,
      contentType: type,
      bytes: buffer.length,
      encoding: asText ? 'utf8' : 'base64',
      body: asText ? buffer.toString('utf8') : buffer.toString('base64'),
    });
  } catch (error) {
    return json(res, 400, { ok: false, error: error.message || 'fetch failed' });
  }
};
