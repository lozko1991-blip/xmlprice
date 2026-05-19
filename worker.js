// ═══════════════════════════════════════════════════════
// UTRADE — Cloudflare Worker
// Назва воркера: utrade-orders
// URL буде: https://utrade-orders.lozko1991.workers.dev
//
// Environment Variables (Cloudflare → Worker → Settings → Variables):
//   GITHUB_TOKEN    — GitHub Fine-grained PAT (Contents: Read+Write на xmlprice)
//   GITHUB_OWNER    — lozko1991-blip
//   GITHUB_REPO     — xmlprice
//   ADMIN_PASSWORD  — ваш пароль для адмінки
//   NP_API_KEY      — ключ Нової Пошти (опційно, для авто-трекінгу)
//   DELIVERY_PENALTY— 120
// ═══════════════════════════════════════════════════════

const FILES = { ORDERS: 'orders.json', DROPERS: 'dropers.json' };

const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Content-Type': 'application/json',
};

const resp = (data, status = 200) =>
  new Response(JSON.stringify(data), { status, headers: CORS });

// ── RATE LIMITING (захист від перебору кодів) ────────────
// Використовує Workers KV або просто Map в пам'яті воркера
// Map живе поки живе екземпляр воркера (~декілька хвилин)
// Для продакшн — підключіть Cloudflare KV
const rateLimitMap = new Map();

function checkRateLimit(ip, action) {
  // Дозволяємо: 10 спроб входу за 60 секунд з одного IP
  const LIMITS = {
    'getOrders':  { max: 10, window: 60 },
    'getBalance': { max: 10, window: 60 },
    'addOrder':   { max: 20, window: 60 },
    'adminLogin': { max: 5,  window: 60 },
  };
  const limit = LIMITS[action];
  if (!limit) return false; // без обмежень

  const key = ip + ':' + action;
  const now = Date.now();
  const entry = rateLimitMap.get(key) || { count: 0, resetAt: now + limit.window * 1000 };

  if (now > entry.resetAt) {
    // Вікно скинулось
    entry.count   = 1;
    entry.resetAt = now + limit.window * 1000;
  } else {
    entry.count++;
  }
  rateLimitMap.set(key, entry);

  if (entry.count > limit.max) {
    const wait = Math.ceil((entry.resetAt - now) / 1000);
    throw new Error(`Забагато спроб. Спробуйте через ${wait} секунд.`);
  }
  return false;
}

// ── ГОЛОВНИЙ ОБРОБНИК ────────────────────────────────────
export default {
  async fetch(request, env) {
    if (request.method === 'OPTIONS') return new Response(null, { headers: CORS });
    if (request.method === 'GET')     return resp({ ok: true, message: 'UTRADE Worker працює' });
    if (request.method !== 'POST')    return resp({ ok: false, error: 'Method not allowed' }, 405);

    let body;
    try { body = await request.json(); }
    catch { return resp({ ok: false, error: 'Invalid JSON' }, 400); }

    const penalty = parseInt(env.DELIVERY_PENALTY) || 120;

    // Отримати IP клієнта
    const clientIP = request.headers.get('CF-Connecting-IP') ||
                     request.headers.get('X-Forwarded-For')  || 'unknown';

    try {
      // Rate limiting для чутливих дій
      checkRateLimit(clientIP, body.action);

      switch (body.action) {
        case 'addOrder':      return resp(await addOrder(body, env, penalty));
        case 'getOrders':     return resp(await getOrders(body, env));
        case 'getBalance':    return resp(await getBalance(body, env, penalty));
        case 'trackTTN':      return resp(await trackTTN(body, env));
        case 'adminLogin':    return resp(adminLogin(body, env));
        case 'getAllOrders':   return resp(await getAllOrders(body, env));
        case 'updateOrder':   return resp(await updateOrder(body, env));
        case 'deleteOrder':   return resp(await deleteOrder(body, env));
        case 'getDropers':    return resp(await getDropers(body, env));
        case 'addDroper':     return resp(await addDroper(body, env));
        case 'updateDroper':  return resp(await updateDroper(body, env));
        case 'deleteDroper':  return resp(await deleteDroper(body, env));
        case 'getAllBalances': return resp(await getAllBalances(body, env, penalty));
        default:              return resp({ ok: false, error: 'Невідома дія' }, 400);
      }
    } catch (e) {
      return resp({ ok: false, error: e.message }, 500);
    }
  }
};

// ════════════════════════════════════════════════════════
// GITHUB API
// ════════════════════════════════════════════════════════
async function ghRead(file, env) {
  const res = await fetch(
    `https://api.github.com/repos/${env.GITHUB_OWNER}/${env.GITHUB_REPO}/contents/${file}`,
    { headers: { 'Authorization': `Bearer ${env.GITHUB_TOKEN}`, 'Accept': 'application/vnd.github.v3+json', 'User-Agent': 'UTRADE' } }
  );
  if (res.status === 404) return { data: [], sha: null };
  if (!res.ok) throw new Error(`GitHub read error ${res.status}`);
  const f = await res.json();
  return { data: JSON.parse(atob(f.content.replace(/\n/g, ''))), sha: f.sha };
}

async function ghWrite(file, data, sha, env, msg) {
  const content = btoa(unescape(encodeURIComponent(JSON.stringify(data, null, 2))));
  const body = { message: msg || `update ${file}`, content, ...(sha ? { sha } : {}) };
  const res = await fetch(
    `https://api.github.com/repos/${env.GITHUB_OWNER}/${env.GITHUB_REPO}/contents/${file}`,
    { method: 'PUT', headers: { 'Authorization': `Bearer ${env.GITHUB_TOKEN}`, 'Accept': 'application/vnd.github.v3+json', 'User-Agent': 'UTRADE', 'Content-Type': 'application/json' }, body: JSON.stringify(body) }
  );
  if (!res.ok) throw new Error(`GitHub write error ${res.status}: ${await res.text()}`);
}

// ════════════════════════════════════════════════════════
// HELPERS
// ════════════════════════════════════════════════════════
function checkAdmin(body, env) {
  if (!body.adminToken || body.adminToken !== env.ADMIN_PASSWORD)
    throw new Error('Не авторизовано');
}

function findDroper(dropers, code) {
  return dropers.find(d => d.code.toUpperCase() === (code || '').toUpperCase().trim());
}

// Код дропера: DR-XXXXX (без схожих символів)
function genDroperCode(existing) {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code;
  do {
    code = 'DR-' + Array.from({ length: 5 }, () => chars[Math.floor(Math.random() * chars.length)]).join('');
  } while (existing.some(d => d.code === code));
  return code;
}

// Номер замовлення: UT-YYYYMMDD-NNN
function genOrderNum(orders) {
  const d = new Date();
  const ds = `${d.getUTCFullYear()}${String(d.getUTCMonth()+1).padStart(2,'0')}${String(d.getUTCDate()).padStart(2,'0')}`;
  const cnt = orders.filter(o => (o.orderNum || '').includes(ds)).length;
  return `UT-${ds}-${String(cnt + 1).padStart(3, '0')}`;
}

function calcProfit(buy, sell) {
  return Math.round((parseFloat(sell) || 0) - (parseFloat(buy) || 0));
}

// ════════════════════════════════════════════════════════
// АДМІН ЛОГІН
// ════════════════════════════════════════════════════════
function adminLogin(body, env) {
  if ((body.password || '') !== env.ADMIN_PASSWORD) throw new Error('Невірний пароль');
  return { ok: true, token: env.ADMIN_PASSWORD };
}

// ════════════════════════════════════════════════════════
// ДОДАТИ ЗАМОВЛЕННЯ (дропер)
// ════════════════════════════════════════════════════════
async function addOrder(body, env, penalty) {
  const code = (body.droperCode || '').toUpperCase().trim();
  if (!code) throw new Error('Код дропера не вказано');

  const items = Array.isArray(body.items) ? body.items : [];
  if (!items.length) throw new Error('Кошик порожній');

  // Перевірити дропера
  const { data: dropers } = await ghRead(FILES.DROPERS, env);
  const droper = findDroper(dropers, code);
  if (!droper) throw new Error('Код дропера не знайдено. Перевірте код.');
  if (droper.active === false) throw new Error('Доступ заблоковано. Зверніться до адміністратора.');

  const { data: orders, sha } = await ghRead(FILES.ORDERS, env);

  // Розрахунок
  let totalBuy = 0, totalSell = 0;
  items.forEach(it => {
    totalBuy  += (parseFloat(it.price)     || 0) * (parseInt(it.count) || 1);
    totalSell += (parseFloat(it.salePrice) || 0) * (parseInt(it.count) || 1);
  });
  totalBuy  = Math.round(totalBuy);
  totalSell = Math.round(totalSell);

  const orderNum = genOrderNum(orders);
  const now = new Date();

  orders.push({
    orderNum,
    date:        now.toISOString(),
    dateDisplay: now.toLocaleString('uk-UA'),
    droperCode:  code,
    droperName:  droper.name,
    clientName:  body.clientName || '',
    phone:       body.phone      || '',
    city:        body.city       || '',
    payment:     body.payment    || '',
    comment:     body.comment    || '',
    items,
    totalBuy,
    totalSell,
    profit:      calcProfit(totalBuy, totalSell),
    ttn:         '',
    npStatus:    '',
    status:      'Новий',
    adminNote:   '',
    history:     [{ ts: now.toISOString(), action: 'Замовлення створено', by: code }],
  });

  await ghWrite(FILES.ORDERS, orders, sha, env, `New order ${orderNum} by ${code}`);
  return { ok: true, orderNum, message: `Замовлення ${orderNum} прийнято!` };
}

// ════════════════════════════════════════════════════════
// ЗАМОВЛЕННЯ ДРОПЕРА
// ════════════════════════════════════════════════════════
async function getOrders(body, env) {
  const code = (body.droperCode || '').toUpperCase().trim();
  if (!code) throw new Error('Код дропера не вказано');

  const { data: dropers } = await ghRead(FILES.DROPERS, env);
  const droper = findDroper(dropers, code);
  if (!droper) throw new Error('Код дропера не знайдено');

  const { data: orders } = await ghRead(FILES.ORDERS, env);
  let filtered = orders.filter(o => o.droperCode.toUpperCase() === code);

  if (body.status && body.status !== 'all') filtered = filtered.filter(o => o.status === body.status);
  if (body.dateFrom) filtered = filtered.filter(o => new Date(o.date) >= new Date(body.dateFrom));
  if (body.dateTo)   filtered = filtered.filter(o => new Date(o.date) <= new Date(body.dateTo + 'T23:59:59'));

  filtered.sort((a, b) => new Date(b.date) - new Date(a.date));

  // Прибрати adminNote з відповіді
  return {
    ok: true,
    droperName: droper.name,
    orders: filtered.map(({ adminNote, ...rest }) => rest),
  };
}

// ════════════════════════════════════════════════════════
// БАЛАНС ДРОПЕРА
// ════════════════════════════════════════════════════════
async function getBalance(body, env, penalty) {
  const code = (body.droperCode || '').toUpperCase().trim();
  if (!code) throw new Error('Не авторизовано');

  const { data: dropers } = await ghRead(FILES.DROPERS, env);
  if (!findDroper(dropers, code)) throw new Error('Код не знайдено');

  const { data: orders } = await ghRead(FILES.ORDERS, env);
  const mine = orders.filter(o => o.droperCode.toUpperCase() === code);

  let profit = 0, done = 0, refused = 0, pending = 0, penalties = 0;
  mine.forEach(o => {
    if (o.status === 'Виконано')           { profit += parseFloat(o.profit)||0; done++; }
    else if (o.status === 'Відмова клієнта') { profit -= penalty; penalties += penalty; refused++; }
    else if (['Новий','Оброблено','Комплектація','Відправлено'].includes(o.status)) pending++;
  });

  return { ok: true, balance: { profit: Math.round(profit), totalOrders: mine.length, done, refused, pending, penalties } };
}

// ════════════════════════════════════════════════════════
// ТРЕКІНГ ТТН (Нова Пошта)
// ════════════════════════════════════════════════════════
async function trackTTN(body, env) {
  const ttn = (body.ttn || '').trim();
  if (!ttn) throw new Error('ТТН не вказано');

  // Авторизація — дропер або адмін
  if (body.adminToken !== env.ADMIN_PASSWORD) {
    if (!body.droperCode) throw new Error('Не авторизовано');
    const { data: dropers } = await ghRead(FILES.DROPERS, env);
    if (!findDroper(dropers, body.droperCode)) throw new Error('Не авторизовано');
  }

  const apiKey = env.NP_API_KEY || '';
  const npRes = await fetch('https://api.novaposhta.ua/v2.0/json/', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      apiKey,
      modelName: 'TrackingDocument',
      calledMethod: 'getStatusDocuments',
      methodProperties: { Documents: [{ DocumentNumber: ttn, Phone: '' }] },
    }),
  });

  const npData = await npRes.json();
  if (!npData.success || !npData.data?.length) return { ok: true, status: 'Не знайдено', ttn };

  const it = npData.data[0];
  return {
    ok: true, ttn,
    status:    it.Status              || '',
    statusCode:it.StatusCode          || '',
    city:      it.CityRecipient       || '',
    warehouse: it.WarehouseRecipient  || '',
    scheduled: it.ScheduledDeliveryDate || '',
    received:  it.ActualDeliveryDate  || '',
    recipient: it.RecipientFullName   || '',
  };
}

// ════════════════════════════════════════════════════════
// АДМІН — ВСІ ЗАМОВЛЕННЯ
// ════════════════════════════════════════════════════════
async function getAllOrders(body, env) {
  checkAdmin(body, env);
  const { data: orders } = await ghRead(FILES.ORDERS, env);

  let f = [...orders];
  if (body.droperCode && body.droperCode !== 'all') f = f.filter(o => o.droperCode.toUpperCase() === body.droperCode.toUpperCase());
  if (body.status && body.status !== 'all')         f = f.filter(o => o.status === body.status);
  if (body.dateFrom) f = f.filter(o => new Date(o.date) >= new Date(body.dateFrom));
  if (body.dateTo)   f = f.filter(o => new Date(o.date) <= new Date(body.dateTo + 'T23:59:59'));
  if (body.search) {
    const q = body.search.toLowerCase();
    f = f.filter(o =>
      (o.orderNum    || '').toLowerCase().includes(q) ||
      (o.droperCode  || '').toLowerCase().includes(q) ||
      (o.clientName  || '').toLowerCase().includes(q) ||
      (o.phone       || '').includes(q) ||
      (o.ttn         || '').includes(q) ||
      (o.items || []).some(i => (i.name || '').toLowerCase().includes(q))
    );
  }

  f.sort((a, b) => new Date(b.date) - new Date(a.date));
  return { ok: true, orders: f, total: f.length };
}

// ════════════════════════════════════════════════════════
// АДМІН — ОНОВИТИ ЗАМОВЛЕННЯ
// ════════════════════════════════════════════════════════
async function updateOrder(body, env) {
  checkAdmin(body, env);
  if (!body.orderNum) throw new Error('orderNum обов\'язковий');

  const { data: orders, sha } = await ghRead(FILES.ORDERS, env);
  const idx = orders.findIndex(o => o.orderNum === body.orderNum);
  if (idx === -1) throw new Error(`Замовлення ${body.orderNum} не знайдено`);

  const order = orders[idx];
  const now = new Date().toISOString();
  const changes = [];

  // Поля які можна оновити
  ['ttn','status','totalBuy','totalSell','clientName','phone','city','payment','comment','adminNote','npStatus'].forEach(f => {
    if (body[f] !== undefined && String(body[f]) !== String(order[f])) {
      changes.push(`${f}: "${order[f]}"→"${body[f]}"`);
      order[f] = body[f];
    }
  });

  // items якщо передані
  if (Array.isArray(body.items)) { order.items = body.items; changes.push('items'); }

  // Перерахувати profit
  if (body.totalBuy !== undefined || body.totalSell !== undefined) {
    const np = calcProfit(order.totalBuy, order.totalSell);
    if (np !== order.profit) { changes.push(`profit: ${order.profit}→${np}`); order.profit = np; }
  }

  if (changes.length) {
    if (!order.history) order.history = [];
    order.history.push({ ts: now, action: `Змінено: ${changes.join(', ')}`, by: 'admin' });
  }

  orders[idx] = order;
  await ghWrite(FILES.ORDERS, orders, sha, env, `Update ${body.orderNum}`);
  return { ok: true, order: orders[idx] };
}

// ════════════════════════════════════════════════════════
// АДМІН — ВИДАЛИТИ ЗАМОВЛЕННЯ
// ════════════════════════════════════════════════════════
async function deleteOrder(body, env) {
  checkAdmin(body, env);
  const { data: orders, sha } = await ghRead(FILES.ORDERS, env);
  const updated = orders.filter(o => o.orderNum !== body.orderNum);
  if (updated.length === orders.length) throw new Error('Замовлення не знайдено');
  await ghWrite(FILES.ORDERS, updated, sha, env, `Delete ${body.orderNum}`);
  return { ok: true };
}

// ════════════════════════════════════════════════════════
// АДМІН — ДРОПЕРИ
// ════════════════════════════════════════════════════════
async function getDropers(body, env) {
  checkAdmin(body, env);
  const { data: dropers } = await ghRead(FILES.DROPERS, env);
  return { ok: true, dropers };
}

async function addDroper(body, env) {
  checkAdmin(body, env);
  if (!body.name) throw new Error('Ім\'я обов\'язкове');

  const { data: dropers, sha } = await ghRead(FILES.DROPERS, env);
  const code = body.code ? body.code.toUpperCase().trim() : genDroperCode(dropers);
  if (findDroper(dropers, code)) throw new Error(`Дропер ${code} вже існує`);

  const droper = {
    code, name: body.name, phone: body.phone || '',
    site: body.site || '', card: body.card || '',
    comment: body.comment || '', active: true,
    createdAt: new Date().toISOString(),
  };

  dropers.push(droper);
  await ghWrite(FILES.DROPERS, dropers, sha, env, `Add droper ${code}`);
  return { ok: true, droper, message: `Дропер ${code} додано!` };
}

async function updateDroper(body, env) {
  checkAdmin(body, env);
  const code = (body.code || '').toUpperCase().trim();
  const { data: dropers, sha } = await ghRead(FILES.DROPERS, env);
  const idx = dropers.findIndex(d => d.code.toUpperCase() === code);
  if (idx === -1) throw new Error(`Дропер ${code} не знайдено`);

  ['name','phone','site','card','comment','active'].forEach(f => {
    if (body[f] !== undefined) dropers[idx][f] = body[f];
  });

  await ghWrite(FILES.DROPERS, dropers, sha, env, `Update droper ${code}`);
  return { ok: true, droper: dropers[idx] };
}

async function deleteDroper(body, env) {
  checkAdmin(body, env);
  const code = (body.code || '').toUpperCase().trim();
  const { data: dropers, sha } = await ghRead(FILES.DROPERS, env);
  const updated = dropers.filter(d => d.code.toUpperCase() !== code);
  if (updated.length === dropers.length) throw new Error(`Дропер ${code} не знайдено`);
  await ghWrite(FILES.DROPERS, updated, sha, env, `Delete droper ${code}`);
  return { ok: true };
}

// ════════════════════════════════════════════════════════
// АДМІН — БАЛАНСИ ВСІХ ДРОПЕРІВ
// ════════════════════════════════════════════════════════
async function getAllBalances(body, env, penalty) {
  checkAdmin(body, env);
  const { data: orders  } = await ghRead(FILES.ORDERS,  env);
  const { data: dropers } = await ghRead(FILES.DROPERS, env);

  const balances = {};
  dropers.forEach(d => { balances[d.code] = { profit:0, totalOrders:0, done:0, refused:0, pending:0, penalties:0 }; });

  orders.forEach(o => {
    const code = o.droperCode.toUpperCase();
    if (!balances[code]) balances[code] = { profit:0, totalOrders:0, done:0, refused:0, pending:0, penalties:0 };
    const b = balances[code];
    b.totalOrders++;
    if (o.status === 'Виконано')             { b.profit += parseFloat(o.profit)||0; b.done++; }
    else if (o.status === 'Відмова клієнта') { b.profit -= penalty; b.penalties += penalty; b.refused++; }
    else if (['Новий','Оброблено','Комплектація','Відправлено'].includes(o.status)) b.pending++;
  });

  Object.keys(balances).forEach(k => { balances[k].profit = Math.round(balances[k].profit); });
  return { ok: true, balances };
}
