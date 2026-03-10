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

// Report 2: დიჯანქა → გაიყიდა (იგივე ტელეფონი)
const PHONE_MATCH_FIELD = 'UF_CRM_5E5E926786D76';
const DJANKA_STAGE_IDS = ['NEW', 'C0:NEW'];
const SOLD_STAGE_IDS = ['WON', 'C0:WON', 'PREPAYMENT_INVOICE', 'C0:PREPAYMENT_INVOICE'];
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
      select: ['ID', 'TITLE', 'STAGE_ID', 'DATE_CREATE', 'ASSIGNED_BY_ID', CREATED_BY_FIELD, FLAG_FIELD, REQUIRED_FIELD, PHONE_MATCH_FIELD],
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

async function fetchFirstCommDate(dealId, assignedById) {
  const assignedByStr = assignedById != null ? String(assignedById).trim() : '';
  const dates = [];

  // 1. კომენტარები – მხოლოდ პასუხისმგებლის მიერ დაწერილი
  try {
    const commentRes = await axios.post(BITRIX_BASE + '/crm.timeline.comment.list.json', {
      filter: { ENTITY_TYPE: 'deal', ENTITY_ID: String(dealId) },
      select: ['CREATED', 'AUTHOR_ID'],
      order: { CREATED: 'ASC' },
      start: 0,
    });
    const commentData = commentRes.data || {};
    if (!commentData.error && commentData.result && Array.isArray(commentData.result)) {
      for (const c of commentData.result) {
        const authorId = c.AUTHOR_ID != null ? String(c.AUTHOR_ID).trim() : '';
        if (assignedByStr && authorId === assignedByStr) {
          const t = norm(c.CREATED);
          if (t) dates.push(t);
          break;
        }
      }
    }
  } catch (e) {
    console.warn('Comment fetch for deal', dealId, 'failed:', e.message);
  }

  // 2. Outbound call – პასუხისმგებლის მიერ გაკეთებული
  if (assignedByStr) {
    try {
      const activityRes = await axios.post(BITRIX_BASE + '/crm.activity.list.json', {
        filter: {
          OWNER_TYPE_ID: 2,
          OWNER_ID: Number(dealId),
          PROVIDER_TYPE_ID: 'CALL',
          DIRECTION: 2,
          RESPONSIBLE_ID: assignedByStr,
        },
        select: ['CREATED', 'START_TIME'],
        order: { CREATED: 'ASC' },
        start: 0,
      });
      const activityData = activityRes.data || {};
      if (!activityData.error && activityData.result && activityData.result.length > 0) {
        const first = activityData.result[0];
        const t = norm(first.CREATED || first.START_TIME);
        if (t) dates.push(t);
      }
    } catch (e) {
      console.warn('Activity (outbound call) fetch for deal', dealId, 'failed:', e.message);
    }
  }

  // 3. ეტაპის ცვლილება – API არ აჩვენებს ვინ შეცვალა, ვითვალისწინებთ ყველას
  try {
    const stageRes = await axios.post(BITRIX_BASE + '/crm.stagehistory.list.json', {
      entityTypeId: 2,
      filter: { OWNER_ID: Number(dealId) },
      select: ['CREATED_TIME', 'TYPE_ID'],
      order: { ID: 'ASC' },
      start: 0,
    });
    const stageData = stageRes.data || {};
    let items = (stageData.result && stageData.result.items) || [];
    if (!Array.isArray(items) && Array.isArray(stageData.result)) items = stageData.result;
    if (!Array.isArray(items)) items = [];
    for (const it of items) {
      if (it.TYPE_ID !== 1) {
        const t = it.CREATED_TIME || it.CREATED;
        if (t) {
          dates.push(norm(t));
          break;
        }
      }
    }
    if (dates.length === 0 && items.length > 1 && items[1]) {
      const t = items[1].CREATED_TIME || items[1].CREATED;
      if (t) dates.push(norm(t));
    }
  } catch (e) {
    console.warn('Stage history fetch for deal', dealId, 'failed:', e.message);
  }

  if (dates.length === 0) return null;
  dates.sort((a, b) => new Date(a).getTime() - new Date(b).getTime());
  return dates[0];
}

async function fetchFirstCommDatesForDeals(deals) {
  const map = {};
  for (let i = 0; i < deals.length; i++) {
    const d = deals[i];
    const id = String(norm(d.ID) || '').trim();
    if (!id) continue;
    const assignedById = d.ASSIGNED_BY_ID != null ? String(d.ASSIGNED_BY_ID).trim() : '';
    map[id] = await fetchFirstCommDate(id, assignedById || null);
    if ((i + 1) % 5 === 0) await sleep(100);
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

function isDjankaStage(stageId) {
  const s = norm(stageId);
  return s && DJANKA_STAGE_IDS.includes(s);
}

function isSoldStage(stageId) {
  const s = norm(stageId);
  return s && SOLD_STAGE_IDS.includes(s);
}

function buildReport2(deals) {
  const filtered = deals.filter((d) => hasRequiredField(d[REQUIRED_FIELD]));
  const byPhone = {};
  for (const d of filtered) {
    const phone = norm(d[PHONE_MATCH_FIELD]);
    if (!phone) continue;
    if (!byPhone[phone]) byPhone[phone] = [];
    byPhone[phone].push(d);
  }
  const soldDeals = [];
  for (const phone of Object.keys(byPhone)) {
    const group = byPhone[phone];
    const djankas = group.filter((d) => isDjankaStage(d.STAGE_ID));
    const solds = group.filter((d) => isSoldStage(d.STAGE_ID));
    if (djankas.length === 0 || solds.length === 0) continue;
    const djankaDates = djankas.map((d) => new Date(norm(d.DATE_CREATE) || 0).getTime());
    const minDjankaDate = Math.min(...djankaDates);
    for (const s of solds) {
      const soldDate = new Date(norm(s.DATE_CREATE) || 0).getTime();
      if (soldDate >= minDjankaDate) soldDeals.push(s);
    }
  }
  return { count: soldDeals.length, deals: soldDeals };
}

function buildRows(deals, stageMap, userMap, firstCommMap) {
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
    const redistributionDate = norm(d[REQUIRED_FIELD]) || '-';
    const firstCommDate = (firstCommMap && firstCommMap[id]) || '-';
    const dealUrl = `${BITRIX_DEAL_URL}/${id}/`;
    return { id, title, stageName, assignedByName, createdByName, dateCreate, redistributionDate, firstCommDate, dealUrl };
  });
}

function buildRows2(deals, stageMap, userMap, firstCommMap) {
  return deals.map((d) => {
    const id = norm(d.ID) || '-';
    const title = norm(d.TITLE) || '(უსახელო)';
    const stageId = norm(d.STAGE_ID) || '-';
    const stageName = stageMap[stageId] || stageId;
    const phone = norm(d[PHONE_MATCH_FIELD]) || '-';
    const assignedBy = norm(d.ASSIGNED_BY_ID) || '';
    const assignedByName = assignedBy ? (userMap[assignedBy] || assignedBy) : '-';
    let createdBy = d[CREATED_BY_FIELD];
    if (createdBy && typeof createdBy === 'object' && createdBy.id != null) createdBy = createdBy.id;
    createdBy = norm(createdBy) || '';
    const createdByName = createdBy ? (userMap[createdBy] || createdBy) : '-';
    const dateCreate = norm(d.DATE_CREATE) || '-';
    const redistributionDate = norm(d[REQUIRED_FIELD]) || '-';
    const firstCommDate = (firstCommMap && firstCommMap[id]) || '-';
    const dealUrl = `${BITRIX_DEAL_URL}/${id}/`;
    return { id, title, stageName, phone, assignedByName, createdByName, dateCreate, redistributionDate, firstCommDate, dealUrl };
  });
}

function renderHtml(report, report2, stageMap, userMap, rows, rows2, dateFrom, dateTo) {
  const { metricA, metricB, percentage } = report;
  const count2 = report2 ? report2.count : 0;
  const from = dateFrom || DEFAULT_DATE_FROM;
  const to = dateTo || DEFAULT_DATE_TO;
  const rowsJson = JSON.stringify(rows);
  const rows2Json = JSON.stringify(rows2 || []);

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
    .report-section { margin-top:40px; padding-top:32px; border-top:2px solid #e5e7eb; }
    .report-section h2 { margin-top:0; margin-bottom:20px; font-size:20px; color:#111; }
    .nav-reports { display:flex; gap:12px; margin:20px 0; padding:12px 16px; background:#eff6ff; border-radius:8px; border:1px solid #bfdbfe; }
    .nav-reports a { padding:10px 20px; background:#2563eb; color:#fff; border-radius:6px; text-decoration:none; font-weight:500; }
    .nav-reports a:hover { background:#1d4ed8; }
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
    <div class="nav-reports">
      <a href="#report1">↓ რეპორტი 1</a>
      <a href="#report2">↓ რეპორტი 2 (დიჯანქა → გაიყიდა)</a>
    </div>

    <h2 id="report1">რეპორტი 1: რეგისტრირებული / დამუშავებული</h2>
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

    <div id="report2" class="report-section">
      <h2>რეპორტი 2: დიჯანქა → გაიყიდა (იგივე ტელეფონი)</h2>
      <div class="metrics">
        <div class="card">
          <div class="card-title">დილები: დიჯანქა → გაიყიდა (იგივე ტელეფონი)</div>
          <div class="card-value accent">${count2}</div>
        </div>
      </div>
      <div class="table-wrap">
        <div class="table-title">
          <span>დილების ცხრილი (დიჯანქა და შემდეგ გაყიდვა იგივე ტელეფონით)</span>
          <div class="table-actions">
            <button type="button" class="btn btn-primary" id="btnExport2">ექსპორტი Excel</button>
          </div>
        </div>
        <table>
          <thead>
            <tr><th>ID</th><th>სახელწოდება</th><th>ეტაპი</th><th>პირველი კომუნიკაცია</th><th>გადანაწილების თარიღი</th><th>ტელეფონი</th><th>პასუხისმგებელი</th><th>ვინ შექმნა</th><th>შექმნის თარიღი</th></tr>
          </thead>
          <tbody id="tableBody2"></tbody>
        </table>
        <div class="pagination" id="pagination2"></div>
      </div>
    </div>

    <div class="report-section">
      <h2>რეპორტი 1 – ცხრილი</h2>
      <div class="table-wrap">
        <div class="table-title">
          <span>დილების ცხრილი (დააწვი რომ გადახვიდე დილზე Bitrix24-ში)</span>
          <div class="table-actions">
            <button type="button" class="btn btn-primary" id="btnExport">ექსპორტი Excel</button>
          </div>
        </div>
        <table>
          <thead>
            <tr><th>ID</th><th>სახელწოდება</th><th>ეტაპი</th><th>პირველი კომუნიკაცია</th><th>გადანაწილების თარიღი</th><th>პასუხისმგებელი</th><th>ვინ შექმნა</th><th>შექმნის თარიღი</th></tr>
          </thead>
          <tbody id="tableBody"></tbody>
        </table>
        <div class="pagination" id="pagination"></div>
      </div>
    </div>

    <div class="footer">გენერირებული: ${new Date().toISOString()}</div>
  </div>
  <script>
    var ROWS = ${rowsJson};
    var ROWS2 = ${rows2Json};
    var PAGE_SIZE = 25;
    var PAGE_SIZE2 = 25;
    var currentPage = 1;
    var currentPage2 = 1;

    function escapeHtml(s) {
      if (s == null) return '';
      var t = String(s);
      return t.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
    }

    function renderPagination(tbodyId, paginationId, rows, pageSize, currentPageVar, renderFn) {
      var tbody = document.getElementById(tbodyId);
      var pagination = document.getElementById(paginationId);
      var total = rows.length;
      var totalPages = Math.max(1, Math.ceil(total / pageSize));
      var start = (currentPageVar - 1) * pageSize;
      var end = Math.min(start + pageSize, total);
      var pageRows = rows.slice(start, end);

      tbody.innerHTML = '';
      if (pageRows.length === 0) {
        tbody.innerHTML = '<tr><td colspan="' + (tbodyId === 'tableBody2' ? 9 : 8) + '">დილები არ მოიძებნა</td></tr>';
      } else {
        pageRows.forEach(function(r) {
          var tr = document.createElement('tr');
          tr.innerHTML = renderFn(r);
          tr.addEventListener('click', function(e) {
            if (!e.target.closest('a')) window.open(r.dealUrl, '_blank');
          });
          tbody.appendChild(tr);
        });
      }

      pagination.innerHTML = '';
      if (total > 0) {
        var sel = document.createElement('select');
        [10, 25, 50, 100].forEach(function(n) {
          var opt = document.createElement('option');
          opt.value = n;
          opt.textContent = n + ' ჩანაწერი გვერდზე';
          if (n === pageSize) opt.selected = true;
          sel.appendChild(opt);
        });
        sel.addEventListener('change', function() {
          if (paginationId === 'pagination') {
            PAGE_SIZE = parseInt(this.value, 10);
            currentPage = Math.min(currentPage, Math.max(1, Math.ceil(rows.length / PAGE_SIZE)));
            renderTable();
          } else {
            PAGE_SIZE2 = parseInt(this.value, 10);
            currentPage2 = Math.min(currentPage2, Math.max(1, Math.ceil(rows.length / PAGE_SIZE2)));
            renderTable2();
          }
        });
        pagination.appendChild(sel);
        if (currentPageVar > 1) {
          var prev = document.createElement('button');
          prev.className = 'btn';
          prev.textContent = '← წინა';
          prev.onclick = function() {
            if (paginationId === 'pagination') { currentPage--; renderTable(); }
            else { currentPage2--; renderTable2(); }
          };
          pagination.appendChild(prev);
        }
        var sp = document.createElement('span');
        sp.textContent = 'გვერდი ' + currentPageVar + ' / ' + totalPages + ' (სულ ' + total + ')';
        pagination.appendChild(sp);
        if (currentPageVar < totalPages) {
          var next = document.createElement('button');
          next.className = 'btn';
          next.textContent = 'შემდეგი →';
          next.onclick = function() {
            if (paginationId === 'pagination') { currentPage++; renderTable(); }
            else { currentPage2++; renderTable2(); }
          };
          pagination.appendChild(next);
        }
      }
    }

    function renderTable() {
      renderPagination('tableBody', 'pagination', ROWS, PAGE_SIZE, currentPage, function(r) {
        return '<td><a href="' + r.dealUrl + '" target="_blank" rel="noopener">' + escapeHtml(r.id) + '</a></td><td>' + escapeHtml(r.title) + '</td><td>' + escapeHtml(r.stageName) + '</td><td>' + escapeHtml(r.firstCommDate) + '</td><td>' + escapeHtml(r.redistributionDate) + '</td><td>' + escapeHtml(r.assignedByName) + '</td><td>' + escapeHtml(r.createdByName) + '</td><td>' + escapeHtml(r.dateCreate) + '</td>';
      });
    }

    function renderTable2() {
      renderPagination('tableBody2', 'pagination2', ROWS2, PAGE_SIZE2, currentPage2, function(r) {
        return '<td><a href="' + r.dealUrl + '" target="_blank" rel="noopener">' + escapeHtml(r.id) + '</a></td><td>' + escapeHtml(r.title) + '</td><td>' + escapeHtml(r.stageName) + '</td><td>' + escapeHtml(r.firstCommDate) + '</td><td>' + escapeHtml(r.redistributionDate) + '</td><td>' + escapeHtml(r.phone) + '</td><td>' + escapeHtml(r.assignedByName) + '</td><td>' + escapeHtml(r.createdByName) + '</td><td>' + escapeHtml(r.dateCreate) + '</td>';
      });
    }

    document.getElementById('btnExport').onclick = function() {
      var qs = window.location.search || '?date_from=${escapeHtml(from)}&date_to=${escapeHtml(to)}';
      window.location.href = '/export' + qs;
    };

    document.getElementById('btnExport2').onclick = function() {
      var qs = window.location.search || '?date_from=${escapeHtml(from)}&date_to=${escapeHtml(to)}&report=2';
      window.location.href = '/export' + qs;
    };

    renderTable();
    renderTable2();
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

    const report2 = buildReport2(deals);
    const seenIds = new Set();
    const allDealsForComm = [];
    for (const d of [...report.deals, ...report2.deals]) {
      const id = String(norm(d.ID) || '').trim();
      if (id && !seenIds.has(id)) {
        seenIds.add(id);
        allDealsForComm.push(d);
      }
    }
    const firstCommMap = await fetchFirstCommDatesForDeals(allDealsForComm);

    const rows = buildRows(report.deals, stageMap, userMap, firstCommMap);
    const rows2 = buildRows2(report2.deals, stageMap, userMap, firstCommMap);
    res.status(200).send(renderHtml(report, report2, stageMap, userMap, rows, rows2, dateFrom, dateTo));
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
    const isReport2 = req.query.report === '2';
    const deals = await fetchAllDeals(dateFrom, dateTo);
    const report = buildReport(deals);
    const report2 = buildReport2(deals);

    const allDeals = isReport2 ? report2.deals : report.deals;
    const userIds = new Set();
    for (const d of allDeals) {
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

    const firstCommMap = await fetchFirstCommDatesForDeals(allDeals);
    const rows = isReport2 ? buildRows2(allDeals, stageMap, userMap, firstCommMap) : buildRows(allDeals, stageMap, userMap, firstCommMap);
    const data = isReport2
      ? [
          ['ID', 'სახელწოდება', 'ეტაპი', 'პირველი კომუნიკაცია', 'გადანაწილების თარიღი', 'ტელეფონი', 'პასუხისმგებელი', 'ვინ შექმნა', 'შექმნის თარიღი'],
          ...rows.map((r) => [r.id, r.title, r.stageName, r.firstCommDate, r.redistributionDate, r.phone, r.assignedByName, r.createdByName, r.dateCreate]),
        ]
      : [
          ['ID', 'სახელწოდება', 'ეტაპი', 'პირველი კომუნიკაცია', 'გადანაწილების თარიღი', 'პასუხისმგებელი', 'ვინ შექმნა', 'შექმნის თარიღი'],
          ...rows.map((r) => [r.id, r.title, r.stageName, r.firstCommDate, r.redistributionDate, r.assignedByName, r.createdByName, r.dateCreate]),
        ];

    const wb = XLSX.utils.book_new();
    const ws = XLSX.utils.aoa_to_sheet(data);
    XLSX.utils.book_append_sheet(wb, ws, isReport2 ? 'დიჯანქა-გაყიდვა' : 'დილები');
    const buf = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename="bitrix_report' + (isReport2 ? '_djanka_sold_' : '_') + dateFrom + '_' + dateTo + '.xlsx"');
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
