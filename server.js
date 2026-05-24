/* =========================================================
   หารกันนะ — LINE Messaging API bot
   พิมพ์ "#หาร" ในกลุ่ม -> บอทตอบลิงก์เปิดแอป LIFF ให้
   ========================================================= */
const express = require('express');
const line = require('@line/bot-sdk');

// ----- ค่าตั้งต้น (อ่านจาก Environment Variables บน Railway) -----
const config = {
  channelAccessToken: process.env.LINE_CHANNEL_ACCESS_TOKEN,
  channelSecret: process.env.LINE_CHANNEL_SECRET,
};
// ลิงก์ LIFF ของแอปหารกันนะ (เปลี่ยนได้ผ่าน env LIFF_URL)
const LIFF_URL = process.env.LIFF_URL || 'https://liff.line.me/2009937945-DrzlxH49';

// คำที่ใช้เรียกบอท (พิมพ์ขึ้นต้นด้วยคำเหล่านี้)
const TRIGGERS = ['#หาร', '#หารกันนะ', '#หารเงิน'];

const app = express();
const client = new line.Client(config);

// health check (Railway / เปิดดูเฉย ๆ)
app.get('/', (req, res) => res.send('Harn Kan Na bot is running ✅'));

// webhook ของ LINE
app.post('/webhook', line.middleware(config), async (req, res) => {
  try {
    const events = req.body.events || [];
    await Promise.all(events.map(handleEvent));
    res.json({ ok: true });
  } catch (err) {
    console.error('handler error:', err);
    res.status(500).end();
  }
});

// คำขอที่ลายเซ็นไม่ถูกต้อง (ไม่ได้มาจาก LINE) -> ตอบ 401 เงียบ ๆ
app.use((err, req, res, next) => {
  if (err && (err instanceof line.SignatureValidationFailed || /signature/i.test(err.message || ''))) {
    return res.status(401).end();
  }
  console.error('unhandled error:', err);
  res.status(500).end();
});

async function handleEvent(event) {
  // บอทถูกแอดเข้ากลุ่ม/ห้อง -> ทักทาย + ส่งลิงก์
  if (event.type === 'join') {
    return client.replyMessage(event.replyToken, [
      { type: 'text', text: 'สวัสดีครับ 👋 พิมพ์ #หาร เมื่อไหร่ก็ได้ เดี๋ยวผมส่งลิงก์เปิดแอปหารเงินให้' },
      buildFlex(),
    ]);
  }

  // ข้อความตัวอักษร
  if (event.type === 'message' && event.message.type === 'text') {
    const text = (event.message.text || '').trim();
    const hit = TRIGGERS.some(t => text === t || text.startsWith(t));
    if (hit) {
      return client.replyMessage(event.replyToken, buildFlex());
    }
  }
  return null;
}

// การ์ดปุ่มเปิดแอป
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
          { type: 'text', text: 'หารค่าใช้จ่ายทริปกับเพื่อน ๆ — บันทึกว่าใครจ่าย หารใคร แล้วสรุปยอดให้อัตโนมัติ', size: 'sm', color: '#7a8a83', wrap: true },
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

const port = process.env.PORT || 3000;
app.listen(port, () => console.log('Harn Kan Na bot listening on port ' + port));
