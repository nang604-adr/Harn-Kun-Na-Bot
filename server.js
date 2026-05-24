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
const TRIGGERS = ['#หาร', '#หารกันนะ', '#หารเงิน'];
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

  socket.on('op', async (op) => {
    const space = socket.data.space;
    if (!space || !op || !op.type) return;
    try {
      const data = await loadSpace(space);
      applyOp(data, op);
      await saveSpace(space, data);
      socket.to(space).emit('op', op);     // ส่ง op ให้คนอื่นในกลุ่มเอาไป apply
    } catch (e) { console.error('op error', e); }
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

async function handleEvent(event) {
  if (event.type === 'join') {
    return client.replyMessage(event.replyToken, [
      { type: 'text', text: 'สวัสดีครับ 👋 พิมพ์ #หาร เมื่อไหร่ก็ได้ เดี๋ยวผมส่งลิงก์เปิดแอปหารเงินให้' },
      buildFlex(),
    ]);
  }
  if (event.type === 'message' && event.message.type === 'text') {
    const text = (event.message.text || '').trim();
    if (TRIGGERS.some((t) => text === t || text.startsWith(t))) {
      return client.replyMessage(event.replyToken, buildFlex());
    }
  }
  return null;
}

function buildFlex() {
  return {
    type: 'flex',
    altText: 'เปิดแอปหารกันนะ 🧮 ' + LIFF_URL,
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
            action: { type: 'uri', label: 'เปิดแอป', uri: LIFF_URL } },
        ],
      },
    },
  };
}

dbInit()
  .catch((e) => console.error('dbInit error (ทำงานต่อแบบ in-memory):', e.message))
  .finally(() => server.listen(PORT, () => console.log('Harn Kan Na server listening on ' + PORT)));
