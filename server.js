/**
 * HOW TO RUN:
 *   npm install express axios xlsx
 *   node server.js
 *
 * Then open: http://localhost:3000
 */

const express = require('express');
const axios = require('axios');
const XLSX = require('xlsx');

const app = express();
const PORT = 3000;

const BITRIX_BASE = 'https://crm.archi.ge/rest/1/1tol0pczy0mvbzmu';
const BITRIX_WEBHOOK_URL = BITRIX_BASE + '/crm.deal.list.json';

const CREATED_BY_FIELD = 'UF_CRM_1599505987'; // ვინ შექმნა

const DEFAULT_DATE_FROM = '2025-01-01';
const DEFAULT_DATE_TO = '2025-02-28';
const CATEGORY_ID = '0';
const FLAG_FIELD = 'UF_CRM_1707970657822';
const REQUIRED_FIELD = 'UF_CRM_1604662888308';
const INITIAL_STAGE_ID = 'NEW';
const MAX_PAGES = 10000;
const DELAY_MS = 500;

// Bitrix24 deal URL (clicking a row opens the deal)
const BITRIX_DEAL_URL = 'https://crm.archi.ge/crm/deal/details';

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function asStr(v) {
  return v === null || v === undefined ? '' : String(v);
}

function norm(v) {
  return asStr(v).trim();
}

function hasRequiredField(value) {
  if (value === null || value === undefined) return false;
  if (value === false) return false;
  if (typeof value === 'string' && value.trim() === '') return false;
  return true;
}

async function fetchAllDeals(dateFrom, dateTo) {
  const from = (dateFrom || DEFAULT_DATE_FROM) + 'T00:00:00';
  const to = (dateTo || DEFAULT_DATE_TO) + 'T23:59:59';
  const allDeals = [];
  let start = 0;
  let page = 0;

  while (page < MAX_PAGES) {
    page += 1;
    const payload = {
      filter: {
        CATEGORY_ID: CATEGORY_ID,
        '>=DATE_CREATE': from,
        '<=DATE_CREATE': to,
        [FLAG_FIELD]: '1',
      },
      select: ['ID', 'TITLE', 'STAGE_ID', 'DATE_CREATE', 'ASSIGNED_BY_ID', CREATED_BY_FIELD, FLAG_FIELD, REQUIRED_FIELD],
      start,
    };

    let response;
    try {
      response = await axios.post(BITRIX_WEBHOOK_URL, payload, {
        headers: { 'Content-Type': 'application/json' },
      });
    } catch (error) {
      console.error('API error:', error.response?.data || error.message);
      throw error;
    }

    const data = response.data || {};
    if (data.error) throw new Error(data.error_description || data.error);

    const batch = Array.isArray(data.result) ? data.result : [];
    allDeals.push(...batch);

    const nextRaw = data.next;
    if (nextRaw === undefined || nextRaw === null) break;

    const nextValue = Number(nextRaw);
    if (!Number.isFinite(nextValue) || nextValue <= start) break;

    start = nextValue;
    await sleep(DELAY_MS);
  }

  return allDeals;
}

async function fetchDealStages() {
  const map = {};
  const addItems = (items) => {
    for (const it of items) {
      const id = (it.id || it.ID || it.STATUS_ID || it.statusId || '').toString();
      const name = it.name || it.NAME || it.title || it.TITLE || id;
      if (id) map[id] = name;
    }
  };

  for (const entityId of ['DEAL_STAGE', 'DEAL_STAGE_0']) {
    try {
      const res = await axios.post(BITRIX_BASE + '/crm.status.entity.items.json', { entityId });
      const data = res.data || {};
      if (data.error) continue;
      const items = Array.isArray(data.result) ? data.result : [];
      addItems(items);
    } catch (e) {
      console.warn('Stage fetch for', entityId, 'failed:', e.message);
    }
  }

  try {
    const res = await axios.post(BITRIX_BASE + '/crm.dealcategory.stage.list.json', { filter: { CATEGORY_ID } });
    const data = res.data || {};
    if (!data.error && data.result) {
      const items = Array.isArray(data.result) ? data.result : Object.values(data.result);
      addItems(items);
    }
  } catch (e) {
    console.warn('Dealcategory stage fetch failed:', e.message);
  }

  return map;
}

async function fetchUserNames(userIds) {
  const ids = [...new Set(userIds)].filter((id) => id && String(id).trim());
  if (ids.length === 0) return {};
  const map = {};
  try {
    const url = BITRIX_BASE + '/user.get.json';
    const res = await axios.post(url, { filter: { '@ID': ids.map(String) } });
    const data = res.data || {};
    if (data.error) return map;
    const users = Array.isArray(data.result) ? data.result : [];
    for (const u of users) {
      const id = String(u.ID || u.id || '');
      const name = [u.NAME, u.LAST_NAME].filter(Boolean).join(' ').trim() || id;
      map[id] = name;
    }
  } catch (e) {
    console.warn('User fetch failed:', e.message);
  }
  return map;
}

function buildReport(deals) {
  const filtered = deals.filter((d) => hasRequiredField(d[REQUIRED_FIELD]));
  const metricA = filtered.length;
  let metricB = 0;
  for (const d of filtered) {
    const stageId = norm(d.STAGE_ID);
    if (stageId && stageId !== INITIAL_STAGE_ID) metricB += 1;
  }
  const percentage =
    metricA === 0 ? 0 : Number(((metricB / metricA) * 100).toFixed(2));
  return { metricA, metricB, percentage, deals: filtered };
}

function buildRows(deals, stageMap, userMap) {
  return deals.map((d) => {
    const id = norm(d.ID) || '-';
    const title = norm(d.TITLE) || '(უსახელო)';
    const stageId = norm(d.STAGE_ID) || '-';
    const stageName = stageMap[stageId] || stageId;
    const assignedBy = norm(d.ASSIGNED_BY_ID) || '';
    const assignedByName = assignedBy ? (userMap[assignedBy] || assignedBy) : '-';
    let createdBy = d[CREATED_BY_FIELD];
    if (createdBy && typeof createdBy === 'object' && createdBy.id != null) createdBy = createdBy.id;
    createdBy = norm(createdBy) || '';
    const createdByName = createdBy ? (userMap[createdBy] || createdBy) : '-';
    const dateCreate = norm(d.DATE_CREATE) || '-';
    const dealUrl = `${BITRIX_DEAL_URL}/${id}/`;
    return { id, title, stageName, assignedByName, createdByName, dateCreate, dealUrl };
  });
}

function renderHtml(report, stageMap, userMap, rows, dateFrom, dateTo) {
  const { metricA, metricB, percentage } = report;
  const from = dateFrom || DEFAULT_DATE_FROM;
  const to = dateTo || DEFAULT_DATE_TO;
  const rowsJson = JSON.stringify(rows);

  return `
<!DOCTYPE html>
<html lang="ka">
<head>
  <meta charset="UTF-8" />
  <title>Bitrix24 Sales Report</title>
  <style>
    body { font-family: system-ui, sans-serif; background:#f5f7fb; margin:0; padding:40px; }
    .container { max-width:1200px; margin:0 auto; background:#fff; border-radius:12px; box-shadow:0 10px 30px rgba(0,0,0,.08); padding:32px; }
    h1 { margin-top:0; color:#111; }
    .date-form { display:flex; gap:12px; align-items:center; flex-wrap:wrap; margin:16px 0 24px; padding:16px; background:#f9fafb; border-radius:8px; }
    .date-form label { font-weight:500; }
    .date-form input[type=date] { padding:8px 12px; border:1px solid #d1d5db; border-radius:6px; }
    .date-form button { padding:8px 20px; background:#2563eb; color:#fff; border:none; border-radius:6px; cursor:pointer; font-weight:500; }
    .date-form button:hover { background:#1d4ed8; }
    .metrics { display:flex; gap:16px; flex-wrap:wrap; margin:24px 0; }
    .card { flex:1 1 180px; background:#f9fafb; border-radius:10px; padding:16px; border:1px solid #e5e7eb; }
    .card-title { font-size:12px; text-transform:uppercase; color:#6b7280; margin-bottom:6px; }
    .card-value { font-size:24px; font-weight:600; color:#111; }
    .card-value.accent { color:#2563eb; }
    .footer { font-size:12px; color:#9ca3af; margin-top:16px; }
    .table-wrap { margin-top:24px; overflow-x:auto; }
    table { width:100%; border-collapse:collapse; font-size:14px; }
    th, td { padding:10px 12px; text-align:left; border-bottom:1px solid #e5e7eb; }
    th { background:#f9fafb; font-weight:600; color:#374151; }
    tr:hover { background:#f9fafb; }
    tr { cursor:pointer; }
    a { color:#2563eb; text-decoration:none; }
    a:hover { text-decoration:underline; }
    .table-title { font-size:16px; font-weight:600; margin-bottom:12px; color:#111; display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:12px; }
    .table-actions { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
    .btn { padding:8px 16px; border-radius:6px; cursor:pointer; font-size:14px; border:1px solid #d1d5db; background:#fff; }
    .btn:hover { background:#f3f4f6; }
    .btn-primary { background:#2563eb; color:#fff; border-color:#2563eb; }
    .btn-primary:hover { background:#1d4ed8; }
    .pagination { display:flex; align-items:center; gap:12px; margin-top:16px; flex-wrap:wrap; }
    .pagination select { padding:6px 10px; border-radius:6px; border:1px solid #d1d5db; }
    .pagination span { color:#6b7280; font-size:14px; }
  </style>
</head>
<body>
  <div class="container">
    <h1>Bitrix24 Sales Performance Report</h1>
    <form class="date-form" method="get" action="/">
      <label>თარიღიდან:</label>
      <input type="date" name="date_from" value="${escapeHtml(from)}" required />
      <label>თარიღამდე:</label>
      <input type="date" name="date_to" value="${escapeHtml(to)}" required />
      <button type="submit">განახლება</button>
    </form>
    <p>Pipeline 0 | ${escapeHtml(from)} – ${escapeHtml(to)}</p>
    <div class="metrics">
      <div class="card">
        <div class="card-title">რეგისტრირებული (Metric A)</div>
        <div class="card-value">${metricA}</div>
      </div>
      <div class="card">
        <div class="card-title">დამუშავებული სეილის მიერ (Metric B)</div>
        <div class="card-value">${metricB}</div>
      </div>
      <div class="card">
        <div class="card-title">Processing Rate</div>
        <div class="card-value accent">${percentage}%</div>
      </div>
    </div>
    <div class="table-wrap">
      <div class="table-title">
        <span>დილების ცხრილი (დააწვი რომ გადახვიდე დილზე Bitrix24-ში)</span>
        <div class="table-actions">
          <button type="button" class="btn btn-primary" id="btnExport">ექსპორტი Excel</button>
        </div>
      </div>
      <table>
        <thead>
          <tr><th>ID</th><th>სახელწოდება</th><th>ეტაპი</th><th>პასუხისმგებელი</th><th>ვინ შექმნა</th><th>შექმნის თარიღი</th></tr>
        </thead>
        <tbody id="tableBody"></tbody>
      </table>
      <div class="pagination" id="pagination"></div>
    </div>
    <div class="footer">გენერირებული: ${new Date().toISOString()}</div>
  </div>
  <script>
    var ROWS = ${rowsJson};
    var PAGE_SIZE = 25;
    var currentPage = 1;

    function escapeHtml(s) {
      if (s == null) return '';
      var t = String(s);
      return t.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
    }

    function renderTable() {
      var tbody = document.getElementById('tableBody');
      var pagination = document.getElementById('pagination');
      var total = ROWS.length;
      var totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));
      var start = (currentPage - 1) * PAGE_SIZE;
      var end = Math.min(start + PAGE_SIZE, total);
      var pageRows = ROWS.slice(start, end);

      tbody.innerHTML = '';

      if (pageRows.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6">დილები არ მოიძებნა</td></tr>';
      } else {
        pageRows.forEach(function(r) {
          var tr = document.createElement('tr');
          tr.innerHTML = '<td><a href="' + r.dealUrl + '" target="_blank" rel="noopener">' + escapeHtml(r.id) + '</a></td><td>' + escapeHtml(r.title) + '</td><td>' + escapeHtml(r.stageName) + '</td><td>' + escapeHtml(r.assignedByName) + '</td><td>' + escapeHtml(r.createdByName) + '</td><td>' + escapeHtml(r.dateCreate) + '</td>';
          tr.addEventListener('click', function(e) {
            if (!e.target.closest('a')) window.open(r.dealUrl, '_blank');
          });
          tbody.appendChild(tr);
        });
      }

      pagination.innerHTML = '';
      if (total > 0) {
        var sel = document.createElement('select');
        sel.id = 'pageSize';
        [10, 25, 50, 100].forEach(function(n) {
          var opt = document.createElement('option');
          opt.value = n;
          opt.textContent = n + ' ჩანაწერი გვერდზე';
          if (n === PAGE_SIZE) opt.selected = true;
          sel.appendChild(opt);
        });
        sel.addEventListener('change', function() {
          PAGE_SIZE = parseInt(this.value, 10);
          var maxPage = Math.max(1, Math.ceil(ROWS.length / PAGE_SIZE));
          currentPage = Math.min(currentPage, maxPage);
          renderTable();
        });
        pagination.appendChild(sel);
        if (currentPage > 1) {
          var prev = document.createElement('button');
          prev.className = 'btn';
          prev.textContent = '← წინა';
          prev.onclick = function() { currentPage--; renderTable(); };
          pagination.appendChild(prev);
        }
        var sp = document.createElement('span');
        sp.textContent = 'გვერდი ' + currentPage + ' / ' + totalPages + ' (სულ ' + total + ')';
        pagination.appendChild(sp);
        if (currentPage < totalPages) {
          var next = document.createElement('button');
          next.className = 'btn';
          next.textContent = 'შემდეგი →';
          next.onclick = function() { currentPage++; renderTable(); };
          pagination.appendChild(next);
        }
      }
    }

    document.getElementById('btnExport').onclick = function() {
      var qs = window.location.search || '?date_from=${escapeHtml(from)}&date_to=${escapeHtml(to)}';
      window.location.href = '/export' + qs;
    };

    renderTable();
  </script>
</body>
</html>
`;
}

function escapeHtml(str) {
  if (str === null || str === undefined) return '';
  const s = String(str);
  return s
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function getDateParams(req) {
  const from = (req.query.date_from || DEFAULT_DATE_FROM).toString().trim();
  const to = (req.query.date_to || DEFAULT_DATE_TO).toString().trim();
  return { dateFrom: from || DEFAULT_DATE_FROM, dateTo: to || DEFAULT_DATE_TO };
}

app.get('/', async (req, res) => {
  try {
    const { dateFrom, dateTo } = getDateParams(req);
    const deals = await fetchAllDeals(dateFrom, dateTo);
    const report = buildReport(deals);

    const userIds = new Set();
    for (const d of report.deals) {
      const a = norm(d.ASSIGNED_BY_ID);
      if (a) userIds.add(a);
      let c = d[CREATED_BY_FIELD];
      if (c && typeof c === 'object' && c.id != null) c = c.id;
      if (Array.isArray(c)) c = c[0];
      if (c != null && c !== '' && /^\d+$/.test(String(c).trim())) userIds.add(String(c).trim());
    }

    const [stageMap, userMap] = await Promise.all([
      fetchDealStages(),
      fetchUserNames([...userIds]),
    ]);

    const rows = buildRows(report.deals, stageMap, userMap);
    res.status(200).send(renderHtml(report, stageMap, userMap, rows, dateFrom, dateTo));
  } catch (err) {
    console.error(err);
    res.status(500).send(`
      <html><body style="font-family:sans-serif; padding:40px;">
        <h1 style="color:#b91c1c;">შეცდომა</h1>
        <pre>${asStr(err && err.stack ? err.stack : err)}</pre>
      </body></html>
    `);
  }
});

app.get('/export', async (req, res) => {
  try {
    const { dateFrom, dateTo } = getDateParams(req);
    const deals = await fetchAllDeals(dateFrom, dateTo);
    const report = buildReport(deals);

    const userIds = new Set();
    for (const d of report.deals) {
      const a = norm(d.ASSIGNED_BY_ID);
      if (a) userIds.add(a);
      let c = d[CREATED_BY_FIELD];
      if (c && typeof c === 'object' && c.id != null) c = c.id;
      if (Array.isArray(c)) c = c[0];
      if (c != null && c !== '' && /^\d+$/.test(String(c).trim())) userIds.add(String(c).trim());
    }

    const [stageMap, userMap] = await Promise.all([
      fetchDealStages(),
      fetchUserNames([...userIds]),
    ]);

    const rows = buildRows(report.deals, stageMap, userMap);
    const data = [
      ['ID', 'სახელწოდება', 'ეტაპი', 'პასუხისმგებელი', 'ვინ შექმნა', 'შექმნის თარიღი'],
      ...rows.map((r) => [r.id, r.title, r.stageName, r.assignedByName, r.createdByName, r.dateCreate]),
    ];

    const wb = XLSX.utils.book_new();
    const ws = XLSX.utils.aoa_to_sheet(data);
    XLSX.utils.book_append_sheet(wb, ws, 'დილები');
    const buf = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename="bitrix_report_' + dateFrom + '_' + dateTo + '.xlsx"');
    res.send(buf);
  } catch (err) {
    console.error(err);
    res.status(500).send('Export failed: ' + (err && err.message ? err.message : 'Unknown error'));
  }
});

if (require.main === module) {
  app.listen(PORT, () => {
    console.log(`Server: http://localhost:${PORT}`);
  });
}

module.exports = app;
