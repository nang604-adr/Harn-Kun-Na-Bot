/* =========================================================
   หารกันนะ — เซิร์ฟเวอร์รวม (Railway)
   - บอท LINE Messaging API: พิมพ์ #หาร ในกลุ่ม -> ส่งลิงก์เปิดแอป
   - เรียลไทม์ผ่าน Socket.IO: ทุกคนในกลุ่ม (space = groupId) เห็น/แก้ข้อมูลชุดเดียวกัน
   - เก็บข้อมูลใน Postgres (ถ้าไม่มี DATABASE_URL จะใช้หน่วยความจำชั่วคราว)
   ========================================================= */
const express = require('express');
const http = require('http');
const path = require('path');
const line = require('@line/bot-sdk');
const { Server } = require('socket.io');
let pg = null; try { pg = require('pg'); } catch (e) { /* optional */ }

// ----- ค่าตั้งต้น -----
const config = {
  channelAccessToken: process.env.LINE_CHANNEL_ACCESS_TOKEN,
  channelSecret: process.env.LINE_CHANNEL_SECRET,
};
const LIFF_URL = process.env.LIFF_URL || 'https://liff.line.me/2009937945-DrzlxH49';
// ลิงก์เปิดแอปตรง ๆ (Railway) — ใช้กับปุ่มในกลุ่ม เพื่อส่งค่า ?space ได้ชัวร์ทุกเครื่อง
const APP_URL = process.env.APP_URL || 'https://harn-kun-na-bot-production.up.railway.app';
const TRIGGERS = ['#หาร', '#หารกันนะ', '#หารเงิน'];
const SUMMARY_TRIGGERS = ['#สรุป', '#ยอด', '#summary'];
const PORT = process.env.PORT || 3000;

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*', methods: ['GET', 'POST'] } });
const hasLineConfig = !!(config.channelAccessToken && config.channelSecret);
const client = hasLineConfig ? new line.Client(config) : null;

/* =========================================================
   ชั้นเก็บข้อมูล (Postgres หรือ in-memory)
   ข้อมูลต่อ space (1 space = 1 groupId) เก็บเป็น JSON: { trips:[...], roundMode }
   ========================================================= */
let pool = null;
const mem = new Map();

async function dbInit() {
  if (process.env.DATABASE_URL && pg) {
    try {
      pool = new pg.Pool({
        connectionString: process.env.DATABASE_URL,
        ssl: { rejectUnauthorized: false },
      });
      await pool.query(`CREATE TABLE IF NOT EXISTS spaces (
        id TEXT PRIMARY KEY,
        data JSONB NOT NULL,
        updated_at TIMESTAMPTZ DEFAULT now()
      )`);
      console.log('Postgres connected ✓');
    } catch (e) {
      console.error('⚠️ Postgres เชื่อมต่อไม่สำเร็จ -> ใช้ in-memory (ข้อมูลจะไม่ถูกบันทึกถาวร):', e.message);
      pool = null;
    }
  } else {
    console.log('No DATABASE_URL — using in-memory store (ข้อมูลจะหายเมื่อรีสตาร์ท)');
  }
}
async function loadSpace(id) {
  if (pool) {
    const r = await pool.query('SELECT data FROM spaces WHERE id=$1', [id]);
    return (r.rows[0] && r.rows[0].data) || { trips: [] };
  }
  return mem.get(id) || { trips: [] };
}
async function saveSpace(id, data) {
  if (pool) {
    await pool.query(
      `INSERT INTO spaces (id, data, updated_at) VALUES ($1, $2, now())
       ON CONFLICT (id) DO UPDATE SET data=$2, updated_at=now()`,
      [id, data]
    );
  } else {
    mem.set(id, data);
  }
}

/* คิวประมวลผล op ต่อ space (กัน race condition: op หลายตัวจากคน/แอ็กชันเดียวกันมาชิดกัน) */
const spaceQueues = new Map();
function enqueueOp(space, op) {
  const prev = spaceQueues.get(space) || Promise.resolve();
  const next = prev.catch(() => {}).then(async () => {
    const data = await loadSpace(space);
    applyOp(data, op);
    await saveSpace(space, data);
  });
  spaceQueues.set(space, next);
  // ล้างคิวเมื่อจบ (กัน memory ค้าง)
  next.finally(() => { if (spaceQueues.get(space) === next) spaceQueues.delete(space); });
  return next;
}

/* =========================================================
   นำ op มาปรับข้อมูล (ตรงกับ action ในหน้าแอป)
   ========================================================= */
function applyOp(d, op) {
  d.trips = d.trips || [];
  const T = (id) => d.trips.find((t) => t.id === id);
  switch (op.type) {
    case 'createTrip':
      if (op.trip && !T(op.trip.id)) d.trips.push(op.trip);
      break;
    case 'updateTrip': {
      const t = T(op.tripId); if (t && op.fields) Object.assign(t, op.fields); break;
    }
    case 'deleteTrip':
      d.trips = d.trips.filter((t) => t.id !== op.tripId); break;
    case 'addMember': {
      const t = T(op.tripId); if (t) { t.members = t.members || []; if (!t.members.find((m) => m.id === op.member.id)) t.members.push(op.member); } break;
    }
    case 'updateMember': {
      const t = T(op.tripId); if (t) { const m = (t.members || []).find((x) => x.id === op.member.id); if (m) Object.assign(m, op.member); } break;
    }
    case 'removeMember': {
      const t = T(op.tripId);
      if (t) {
        t.members = (t.members || []).filter((m) => m.id !== op.memberId);
        const first = t.members[0];
        (t.expenses || []).forEach((e) => {
          e.splitIds = (e.splitIds || []).filter((x) => x !== op.memberId);
          if (e.payerId === op.memberId && first) e.payerId = first.id;
        });
      }
      break;
    }
    case 'setMembers': {
      const t = T(op.tripId); if (t) t.members = op.members || []; break;
    }
    case 'addExpense': {
      const t = T(op.tripId); if (t) { t.expenses = t.expenses || []; if (!t.expenses.find((e) => e.id === op.expense.id)) t.expenses.push(op.expense); } break;
    }
    case 'updateExpense': {
      const t = T(op.tripId); if (t) { const e = (t.expenses || []).find((x) => x.id === op.expense.id); if (e) Object.assign(e, op.expense); } break;
    }
    case 'deleteExpense': {
      const t = T(op.tripId); if (t) t.expenses = (t.expenses || []).filter((e) => e.id !== op.expenseId); break;
    }
    case 'setSettle': {
      const t = T(op.tripId); if (t) { t.settle = t.settle || {}; t.settle[op.key] = op.rec; } break;
    }
    case 'setContact': {
      d.directory = d.directory || {}; if (op.contact && op.contact.key) d.directory[op.contact.key] = op.contact; break;
    }
    case 'setRoundMode':
      d.roundMode = op.mode; break;
    default:
      break;
  }
  return d;
}

/* =========================================================
   Socket.IO เรียลไทม์
   ========================================================= */
io.on('connection', (socket) => {
  socket.on('join', async ({ space }) => {
    if (!space) return;
    socket.join(space);
    socket.data.space = space;
    try {
      const data = await loadSpace(space);
      socket.emit('state', { data });
    } catch (e) { console.error('join load error', e); }
  });

  socket.on('op', (op) => {
    const space = socket.data.space;
    if (!space || !op || !op.type) return;
    // ใช้คิว/promise chain ต่อ space — ป้องกัน op หลายตัวโหลด-เซฟทับกัน (race)
    enqueueOp(space, op).then(() => {
      socket.to(space).emit('op', op);
    }).catch((e) => console.error('op error', e));
  });
});

/* =========================================================
   REST + บอท
   ========================================================= */
// เสิร์ฟหน้าแอป (public/index.html) — origin เดียวกับ API/WebSocket
app.use(express.static(path.join(__dirname, 'public')));
app.get('/health', (req, res) => res.send('Harn Kan Na server is running ✅ (web + bot + realtime)'));

// โหลดข้อมูล space แบบ HTTP (เผื่อใช้ตอนเริ่ม / สำรอง)
app.get('/api/space/:id', async (req, res) => {
  try { res.json(await loadSpace(req.params.id)); }
  catch (e) { res.status(500).json({ error: 'load failed' }); }
});

// ให้แอปสั่งบอทโพสต์สรุปเข้ากลุ่ม (ใช้ express.json เฉพาะ route นี้ ไม่กระทบ /webhook)
app.post('/api/share', express.json({ limit: '256kb' }), async (req, res) => {
  try {
    if (!hasLineConfig || !client) return res.status(503).json({ error: 'no line config' });
    const { space, text } = req.body || {};
    let target = null;
    if (typeof space === 'string') {
      if (space.startsWith('group:')) target = space.slice(6);
      else if (space.startsWith('room:')) target = space.slice(5);
    }
    if (!target) return res.status(400).json({ error: 'not a group/room' });
    if (!text || typeof text !== 'string') return res.status(400).json({ error: 'no text' });
    await client.pushMessage(target, { type: 'text', text: text.slice(0, 4900) });
    res.json({ ok: true });
  } catch (e) {
    console.error('share error:', e.message);
    res.status(500).json({ error: 'push failed' });
  }
});

// webhook ของบอท
if (hasLineConfig) {
  app.post('/webhook', line.middleware(config), async (req, res) => {
    try {
      await Promise.all((req.body.events || []).map(handleEvent));
      res.json({ ok: true });
    } catch (err) { console.error('webhook error:', err); res.status(500).end(); }
  });
} else {
  app.post('/webhook', (req, res) => res.status(503).send('LINE config missing'));
}

// คำขอที่ลายเซ็นไม่ถูกต้อง -> 401 เงียบ ๆ
app.use((err, req, res, next) => {
  if (err && (err instanceof line.SignatureValidationFailed || /signature/i.test(err.message || ''))) {
    return res.status(401).end();
  }
  console.error('unhandled error:', err);
  res.status(500).end();
});

/* สร้าง space key จาก source ของ event (กลุ่ม/ห้อง/เดี่ยว) */
function spaceKeyFromSource(s) {
  s = s || {};
  if (s.groupId) return 'group:' + s.groupId;
  if (s.roomId) return 'room:' + s.roomId;
  if (s.userId) return 'user:' + s.userId;
  return '';
}
/* ลิงก์เปิดแอปที่ฝัง space ของกลุ่มนั้น ๆ ไว้ -> ทุกคนในกลุ่มเข้าพื้นที่เดียวกันแน่นอน
   ใช้ APP_URL (Railway ตรง ๆ) เพราะส่งค่า ?space ผ่านได้ชัวร์กว่า liff.line.me */
function liffLink(event) {
  const key = spaceKeyFromSource(event.source);
  const base = APP_URL.replace(/\/+$/, '');
  return key ? (base + '/?space=' + encodeURIComponent(key)) : base;
}

async function handleEvent(event) {
  if (event.type === 'join') {
    const link = liffLink(event);
    return client.replyMessage(event.replyToken, [
      { type: 'text', text: 'สวัสดีครับ 👋 พิมพ์ #หาร เมื่อไหร่ก็ได้ เดี๋ยวผมส่งลิงก์เปิดแอปหารเงินให้' },
      buildFlex(link),
    ]);
  }
  if (event.type === 'message' && event.message.type === 'text') {
    const text = (event.message.text || '').trim();
    // #สรุป -> โพสต์สรุปยอดของกลุ่มลงในแชต
    if (SUMMARY_TRIGGERS.some((t) => text === t || text.startsWith(t))) {
      const key = spaceKeyFromSource(event.source);
      const data = key ? await loadSpace(key) : { trips: [] };
      const tp = latestTrip(data);
      if (!tp) {
        return client.replyMessage(event.replyToken, { type: 'text', text: 'ยังไม่มีข้อมูลทริปในกลุ่มนี้ครับ พิมพ์ #หาร เพื่อเปิดแอปแล้วเริ่มบันทึกได้เลย' });
      }
      const mode = (data && data.roundMode) || 'baht';
      return client.replyMessage(event.replyToken, { type: 'text', text: buildSummaryReply(tp, mode, liffLink(event)) });
    }
    if (TRIGGERS.some((t) => text === t || text.startsWith(t))) {
      return client.replyMessage(event.replyToken, buildFlex(liffLink(event)));
    }
  }
  return null;
}

function buildFlex(link) {
  link = link || LIFF_URL;
  return {
    type: 'flex',
    altText: 'เปิดแอปหารกันนะ 🧮 ' + link,
    contents: {
      type: 'bubble',
      body: {
        type: 'box', layout: 'vertical', spacing: 'sm',
        contents: [
          { type: 'text', text: 'หารกันนะ 🧮', weight: 'bold', size: 'xl', color: '#06C755' },
          { type: 'text', text: 'หารค่าใช้จ่ายทริปกับเพื่อน ๆ — ทุกคนในกลุ่มเห็นและแก้ข้อมูลชุดเดียวกันแบบเรียลไทม์', size: 'sm', color: '#7a8a83', wrap: true },
        ],
      },
      footer: {
        type: 'box', layout: 'vertical',
        contents: [
          { type: 'button', style: 'primary', color: '#06C755',
            action: { type: 'uri', label: 'เปิดแอป', uri: link } },
        ],
      },
    },
  };
}

/* =========================================================
   คำนวณยอด (mirror กับฝั่งแอป) สำหรับคำสั่ง #สรุป
   ========================================================= */
function splitCents(total, n) {
  const b = Math.floor(total / n); let r = total - b * n; const o = [];
  for (let i = 0; i < n; i++) o.push(b + (i < r ? 1 : 0));
  return o;
}
function memberById(tp, id) { return (tp.members || []).find((m) => m.id === id); }
function memberUnitKey(m) { const f = (m.family || '').trim(); return f ? 'f:' + f : 'm:' + m.id; }
function tripUnits(tp) {
  const order = [], map = {};
  for (const m of (tp.members || [])) {
    const k = memberUnitKey(m);
    if (!map[k]) { map[k] = { key: k, name: (m.family || '').trim() || m.name, members: [] }; order.push(k); }
    map[k].members.push(m);
  }
  return order.map((k) => map[k]);
}
function unitExists(tp, key) { return (tp.members || []).some((m) => memberUnitKey(m) === key); }
function expShares(tp, e) {
  const cents = Math.round(e.amount * (e.rate || 1) * 100); const map = {};
  if (e.splitBy === 'family') {
    const fams = (e.splitFams || []).filter((k) => unitExists(tp, k)); if (!fams.length) return {};
    const fp = splitCents(cents, fams.length);
    fams.forEach((fk, i) => {
      const fm = (tp.members || []).filter((m) => memberUnitKey(m) === fk); if (!fm.length) return;
      const mp = splitCents(fp[i], fm.length); fm.forEach((m, j) => map[m.id] = (map[m.id] || 0) + mp[j] / 100);
    });
  } else {
    const sp = (e.splitIds || []).filter((id) => memberById(tp, id)); if (!sp.length) return {};
    const parts = splitCents(cents, sp.length); sp.forEach((id, i) => map[id] = parts[i] / 100);
  }
  return map;
}
function computeBalances(tp) {
  const bal = {}; (tp.members || []).forEach((m) => bal[m.id] = { paid: 0, owed: 0, net: 0 });
  let T = 0;
  for (const e of (tp.expenses || [])) {
    const c = Math.round(e.amount * (e.rate || 1) * 100); T += c;
    if (bal[e.payerId]) bal[e.payerId].paid += c;
    const sh = expShares(tp, e); for (const id in sh) { if (bal[id]) bal[id].owed += Math.round(sh[id] * 100); }
  }
  Object.values(bal).forEach((b) => { b.paid /= 100; b.owed /= 100; b.net = b.paid - b.owed; });
  return { bal, total: T / 100 };
}
function familyBalances(tp) {
  const { bal: mbal, total } = computeBalances(tp); const units = tripUnits(tp);
  const ubal = {}; units.forEach((u) => ubal[u.key] = { paid: 0, owed: 0, net: 0 });
  for (const m of (tp.members || [])) { const k = memberUnitKey(m); const b = mbal[m.id]; if (b && ubal[k]) { ubal[k].paid += b.paid; ubal[k].owed += b.owed; } }
  Object.values(ubal).forEach((b) => b.net = Math.round((b.paid - b.owed) * 100) / 100);
  return { bal: ubal, total, units };
}
function roundNetsToBaht(nets) {
  const ids = Object.keys(nets); const ex = ids.map((i) => nets[i]); let r = ex.map((v) => Math.round(v));
  let d = r.reduce((a, b) => a + b, 0); const res = ids.map((_, i) => r[i] - ex[i]); const o = ids.map((_, i) => i);
  if (d > 0) { o.sort((a, b) => res[b] - res[a]); for (let k = 0; k < d && k < o.length; k++) r[o[k]] -= 1; }
  else if (d < 0) { o.sort((a, b) => res[a] - res[b]); for (let k = 0; k < (-d) && k < o.length; k++) r[o[k]] += 1; }
  const out = {}; ids.forEach((i, k) => out[i] = r[k]); return out;
}
function effectiveUnitBalances(tp, mode) {
  const { bal, total, units } = familyBalances(tp);
  if (mode !== 'baht') return { bal, total, units };
  const nets = {}; for (const k in bal) nets[k] = bal[k].net;
  const r = roundNetsToBaht(nets); const b2 = {};
  for (const k in bal) b2[k] = { paid: bal[k].paid, net: r[k], owed: bal[k].paid - r[k] };
  return { bal: b2, total, units };
}
function settle(bal) {
  const EPS = 0.005, cr = [], de = [];
  for (const id in bal) { const net = Math.round(bal[id].net * 100) / 100; if (net > EPS) cr.push({ id, amt: net }); else if (net < -EPS) de.push({ id, amt: -net }); }
  cr.sort((a, b) => b.amt - a.amt); de.sort((a, b) => b.amt - a.amt);
  const r = []; let i = 0, j = 0;
  while (i < de.length && j < cr.length) {
    const p = Math.min(de[i].amt, cr[j].amt);
    r.push({ from: de[i].id, to: cr[j].id, amount: Math.round(p * 100) / 100 });
    de[i].amt -= p; cr[j].amt -= p; if (de[i].amt < EPS) i++; if (cr[j].amt < EPS) j++;
  }
  return r;
}
function fmtBaht(n) { const v = Math.round(n * 100) / 100; return v.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }); }
function latestTrip(data) {
  const trips = (data && data.trips) || []; if (!trips.length) return null;
  const sorted = [...trips].sort((a, b) => String(b.createdAt || '').localeCompare(String(a.createdAt || '')));
  return sorted[0] || trips[trips.length - 1];
}
function unitNameByKey(units, key) { const u = units.find((x) => x.key === key); return u ? u.name : '?'; }
function buildSummaryReply(tp, mode, link) {
  const { bal, total, units } = effectiveUnitBalances(tp, mode);
  const tx = settle(bal);
  const hasFam = units.length < (tp.members || []).length;
  const metaCount = hasFam ? `${units.length} บ้าน (${(tp.members || []).length} คน)` : `${(tp.members || []).length} คน`;
  let s = `📋 สรุป: ${tp.name}\n💰 ยอดรวม ฿${fmtBaht(total)} · ${metaCount} · ${(tp.expenses || []).length} รายการ\n`;
  s += `\n👤 ยอดแต่ละ${hasFam ? 'บ้าน' : 'คน'}\n`;
  for (const u of units) {
    const b = bal[u.key] || { paid: 0, net: 0 }; const net = Math.round(b.net * 100) / 100;
    const tag = net > 0.005 ? `ได้คืน ฿${fmtBaht(net)}` : (net < -0.005 ? `จ่ายเพิ่ม ฿${fmtBaht(-net)}` : 'ลงตัว');
    const isFam = u.members.length > 1 || u.key.startsWith('f:');
    const nm = isFam ? `${u.name} (${u.members.map((m) => m.name).join(',')})` : u.name;
    s += `• ${nm}: จ่าย ฿${fmtBaht(b.paid)} (${tag})\n`;
  }
  s += `\n🔁 ใครโอนให้ใคร\n`;
  if (!tx.length) s += `ทุกคนเคลียร์กันแล้ว 🎉\n`;
  else for (const t of tx) s += `• ${unitNameByKey(units, t.from)} → ${unitNameByKey(units, t.to)}: ฿${fmtBaht(t.amount)}\n`;
  s += `\n📲 เปิดแอป: ${link}`;
  return s;
}

dbInit()
  .catch((e) => console.error('dbInit error (ทำงานต่อแบบ in-memory):', e.message))
  .finally(() => server.listen(PORT, () => console.log('Harn Kan Na server listening on ' + PORT)));
