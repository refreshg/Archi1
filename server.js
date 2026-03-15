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

const CREATED_BY_FIELD = 'UF_CRM_1599505987'; // ვინ შექმნა (fallback – user ID)
const CREATED_BY_NAME_FIELD = 'UF_CRM_1673261686'; // ვინ შექმნა – სახელი გვარი (პირადი)

const CATEGORY_ID = '0';
function getDefaultDateRange() {
  const now = new Date();
  const to = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const from = new Date(to);
  from.setDate(from.getDate() - 30);
  return {
    dateFrom: from.toISOString().slice(0, 10),
    dateTo: to.toISOString().slice(0, 10),
  };
}
const FLAG_FIELD = 'UF_CRM_1707970657822';
const REQUIRED_FIELD = 'UF_CRM_1604662888308';
const INITIAL_STAGE_ID = 'NEW';
const MAX_PAGES = 10000;

const PHONE_MATCH_FIELD = 'UF_CRM_5E5E926786D76';
const MOTKHOVNA_FIELD = 'UF_CRM_5F2A7F2C0F9C9'; // მოთხოვნა (dropdown)
const SERVICE_LEADS_FIELD = 'UF_CRM_1707196806355'; // Service Leads
const DELAY_MS = 500;
const CACHE_TTL_MS = 5 * 60 * 1000;
/** ექსპორტში დილების ამ რაოდენობაზე მეტისას პირველი კომუნიკაცია/კომენტარი არ იხმარს (სწრაფი ჩამოტვირთვა) */
const EXPORT_SKIP_FIRST_COMM_OVER = 350;

const reportCache = new Map();

function getCacheKey(dateFrom, dateTo) {
  return `${dateFrom}|${dateTo}`;
}

function setReportCache(key, value) {
  reportCache.set(key, { ...value, createdAt: Date.now() });
}

function getReportCache(key) {
  const entry = reportCache.get(key);
  if (!entry) return null;
  if (Date.now() - entry.createdAt > CACHE_TTL_MS) {
    reportCache.delete(key);
    return null;
  }
  return entry;
}

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

function formatDate(v) {
  if (v === null || v === undefined || String(v).trim() === '') return '-';
  const s = String(v).trim();
  const m = s.match(/^(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2})(?::\d{2})?([+-]\d{2}(?::\d{2})?)?/);
  if (m) return m[1] + ' ' + m[2] + ':' + m[3];
  return s;
}

function getDropdownValue(v, listMap) {
  if (v === null || v === undefined) return '';
  let rawId = '';
  if (typeof v === 'object' && v.value != null) return String(v.value).trim();
  if (typeof v === 'object' && v.id != null) rawId = String(v.id).trim();
  else rawId = String(v).trim();
  if (listMap && rawId && listMap[rawId]) return listMap[rawId];
  return rawId;
}

async function fetchMotkhovnaListMap() {
  const map = {};
  try {
    const res = await axios.post(BITRIX_BASE + '/crm.deal.userfield.list.json', {});
    const data = res.data || {};
    if (data.error || !data.result) return map;
    const fields = Array.isArray(data.result) ? data.result : Object.values(data.result);
    const field = fields.find((f) => (f.FIELD || f.FIELD_NAME || f.field) === MOTKHOVNA_FIELD);
    if (!field) return map;
    const list = field.LIST || field.list;
    if (!list) return map;
    const entries = Array.isArray(list)
      ? list.map((it, i) => [(it.ID ?? it.id ?? i).toString(), it])
      : Object.entries(list);
    for (const [id, it] of entries) {
      const name = (it && (it.VALUE ?? it.value ?? it.NAME ?? it.name)) ?? id;
      if (id) map[String(id)] = String(name).trim();
    }
  } catch (e) {
    console.warn('Motkhovna list fetch failed:', e.message);
  }
  return map;
}

function hasRequiredField(value) {
  if (value === null || value === undefined) return false;
  if (value === false) return false;
  if (typeof value === 'string' && value.trim() === '') return false;
  return true;
}

async function fetchAllDeals(dateFrom, dateTo) {
  const def = getDefaultDateRange();
  const from = (dateFrom || def.dateFrom) + 'T00:00:00';
  const to = (dateTo || def.dateTo) + 'T23:59:59';
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
      select: ['ID', 'TITLE', 'STAGE_ID', 'DATE_CREATE', 'ASSIGNED_BY_ID', CREATED_BY_FIELD, CREATED_BY_NAME_FIELD, FLAG_FIELD, REQUIRED_FIELD, PHONE_MATCH_FIELD, MOTKHOVNA_FIELD],
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

async function fetchServiceLeadsDeals(dateFrom, dateTo) {
  const def = getDefaultDateRange();
  const from = (dateFrom || def.dateFrom) + 'T00:00:00';
  const to = (dateTo || def.dateTo) + 'T23:59:59';
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
        [SERVICE_LEADS_FIELD]: 1,
      },
      select: ['ID', 'TITLE', 'STAGE_ID', 'DATE_CREATE', 'ASSIGNED_BY_ID', CREATED_BY_FIELD, CREATED_BY_NAME_FIELD, FLAG_FIELD, REQUIRED_FIELD, PHONE_MATCH_FIELD, MOTKHOVNA_FIELD],
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

async function fetchFirstCommDate(dealId, assignedById, redistributionDate) {
  const assignedByStr = assignedById != null ? String(assignedById).trim() : '';
  const minDate = redistributionDate ? new Date(redistributionDate).getTime() : -Infinity;
  const dates = [];
  const addIfAfterRedist = (t) => {
    if (t && new Date(t).getTime() >= minDate) dates.push(norm(t));
  };

  const SKIP_COMMENT_TEXT = 'Sale Lead Create (Observe)';
  const isSkipComment = (c) => {
    const text = (c.COMMENT ?? c.TEXT ?? c.MESSAGE ?? '').toString().trim();
    return text === SKIP_COMMENT_TEXT || text.includes(SKIP_COMMENT_TEXT);
  };

  let firstCommentText = null;
  // 1. კომენტარები – მხოლოდ პასუხისმგებლის მიერ დაწერილი; პირველი კომენტარის ტექსტი (ნებისმიერი ავტორი, "Sale Lead Create (Observe)" არ ჩაითვლება)
  try {
    const commentRes = await axios.post(BITRIX_BASE + '/crm.timeline.comment.list.json', {
      filter: { ENTITY_TYPE: 'deal', ENTITY_ID: String(dealId) },
      select: ['CREATED', 'AUTHOR_ID', 'COMMENT'],
      order: { CREATED: 'ASC' },
      start: 0,
    });
    const commentData = commentRes.data || {};
    if (!commentData.error && commentData.result && Array.isArray(commentData.result)) {
      const comments = commentData.result;
      for (const c of comments) {
        if (isSkipComment(c)) continue;
        const text = (c.COMMENT ?? c.TEXT ?? c.MESSAGE ?? '').toString().trim();
        if (firstCommentText === null) firstCommentText = text || null;
        const authorId = c.AUTHOR_ID != null ? String(c.AUTHOR_ID).trim() : '';
        if (assignedByStr && authorId === assignedByStr) {
          addIfAfterRedist(c.CREATED);
          break;
        }
      }
    }
  } catch (e) {
    console.warn('Comment fetch for deal', dealId, 'failed:', e.message);
  }

  // 2. Outbound კომუნიკაცია – ზარი, SMS და სხვა (პასუხისმგებლის მიერ)
  if (assignedByStr) {
    const commTypes = ['CALL', 'SMS'];
    for (const provType of commTypes) {
      try {
        const activityRes = await axios.post(BITRIX_BASE + '/crm.activity.list.json', {
          filter: {
            OWNER_TYPE_ID: 2,
            OWNER_ID: Number(dealId),
            PROVIDER_TYPE_ID: provType,
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
          addIfAfterRedist(first.CREATED || first.START_TIME);
        }
      } catch (e) {
        if (provType === 'CALL') console.warn('Activity (call) fetch for deal', dealId, 'failed:', e.message);
      }
    }
    // თუ CALL/SMS ვერ მოიძებნა, ვცადოთ ყველა outbound აქტივობა (Asterisk და სხვა)
    if (dates.length === 0) {
      try {
        const activityRes = await axios.post(BITRIX_BASE + '/crm.activity.list.json', {
          filter: {
            OWNER_TYPE_ID: 2,
            OWNER_ID: Number(dealId),
            DIRECTION: 2,
            RESPONSIBLE_ID: assignedByStr,
          },
          select: ['CREATED', 'START_TIME', 'PROVIDER_TYPE_ID'],
          order: { CREATED: 'ASC' },
          start: 0,
        });
        const activityData = activityRes.data || {};
        if (!activityData.error && activityData.result && activityData.result.length > 0) {
          const commTypesSet = new Set(['CALL', 'SMS', 'EMAIL']);
          for (const a of activityData.result) {
            const pt = (a.PROVIDER_TYPE_ID || '').toUpperCase();
            if (commTypesSet.has(pt) || pt.includes('CALL') || pt.includes('SMS')) {
              addIfAfterRedist(a.CREATED || a.START_TIME);
              break;
            }
          }
        }
      } catch (e) {
        console.warn('Activity (outbound) fetch for deal', dealId, 'failed:', e.message);
      }
    }
    // BINDINGS-ით ვცადოთ (აქტივობა შეიძლება დილას ბინდინგით იყოს მიბმული)
    if (dates.length === 0) {
      try {
        const activityRes = await axios.post(BITRIX_BASE + '/crm.activity.list.json', {
          filter: {
            BINDINGS: [{ OWNER_TYPE_ID: 2, OWNER_ID: Number(dealId) }],
            DIRECTION: 2,
            RESPONSIBLE_ID: assignedByStr,
          },
          select: ['CREATED', 'START_TIME', 'PROVIDER_TYPE_ID'],
          order: { CREATED: 'ASC' },
          start: 0,
        });
        const activityData = activityRes.data || {};
        if (!activityData.error && activityData.result && activityData.result.length > 0) {
          const commTypesSet = new Set(['CALL', 'SMS', 'EMAIL']);
          for (const a of activityData.result) {
            const pt = String(a.PROVIDER_TYPE_ID || '').toUpperCase();
            if (commTypesSet.has(pt) || pt.includes('CALL') || pt.includes('SMS')) {
              addIfAfterRedist(a.CREATED || a.START_TIME);
              break;
            }
          }
        }
      } catch (e) {
        console.warn('Activity (BINDINGS) fetch for deal', dealId, 'failed:', e.message);
      }
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
          addIfAfterRedist(t);
          break;
        }
      }
    }
    if (dates.length === 0 && items.length > 1 && items[1]) {
      addIfAfterRedist(items[1].CREATED_TIME || items[1].CREATED);
    }
  } catch (e) {
    console.warn('Stage history fetch for deal', dealId, 'failed:', e.message);
  }

  const firstCommDate = dates.length === 0 ? null : (dates.sort((a, b) => new Date(a).getTime() - new Date(b).getTime()), dates[0]);
  return { firstCommDate, firstCommentText: firstCommentText || null };
}

async function fetchFirstCommDatesForDeals(deals, onProgress) {
  const firstCommMap = {};
  const firstCommentMap = {};
  const total = deals.length;
  if (total === 0) return { firstCommMap, firstCommentMap };

  const concurrency = Math.min(8, Math.max(1, Math.floor(total / 10) || 4));
  let index = 0;
  let completed = 0;

  async function worker() {
    while (true) {
      const i = index++;
      if (i >= total) break;
      const d = deals[i];
      const id = String(norm(d.ID) || '').trim();
      if (!id) continue;
      const assignedById = d.ASSIGNED_BY_ID != null ? String(d.ASSIGNED_BY_ID).trim() : '';
      const redistributionDate = d[REQUIRED_FIELD] ? norm(d[REQUIRED_FIELD]) : null;
      const { firstCommDate, firstCommentText } = await fetchFirstCommDate(id, assignedById || null, redistributionDate);
      firstCommMap[id] = firstCommDate;
      firstCommentMap[id] = firstCommentText;
      completed += 1;
      if (onProgress && total > 0) {
        onProgress(Math.round((completed / total) * 100));
      }
      if (completed % 20 === 0) {
        await sleep(100);
      }
    }
  }

  const workers = [];
  const workerCount = Math.min(concurrency, total);
  for (let i = 0; i < workerCount; i++) {
    workers.push(worker());
  }
  await Promise.all(workers);
  return { firstCommMap, firstCommentMap };
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

function buildRows(deals, stageMap, userMap, firstCommMap, firstCommentMap, motkhovnaListMap) {
  return deals.map((d) => {
    const id = norm(d.ID) || '-';
    const title = norm(d.TITLE) || '(უსახელო)';
    const stageId = norm(d.STAGE_ID) || '-';
    const stageName = stageMap[stageId] || stageId;
    const assignedBy = norm(d.ASSIGNED_BY_ID) || '';
    const assignedByName = assignedBy ? (userMap[assignedBy] || assignedBy) : '-';
    const createdByRaw = (d[CREATED_BY_NAME_FIELD] != null && String(d[CREATED_BY_NAME_FIELD]).trim() !== '')
      ? String(d[CREATED_BY_NAME_FIELD]).trim()
      : '';
    const createdByName = createdByRaw === '' ? '-' : (/^\d+$/.test(createdByRaw) ? (userMap[createdByRaw] || createdByRaw) : createdByRaw);
    const dateCreate = formatDate(d.DATE_CREATE);
    const redistributionDate = formatDate(d[REQUIRED_FIELD]);
    const firstCommDate = formatDate((firstCommMap && firstCommMap[id]) || null);
    const firstComment = (firstCommentMap && firstCommentMap[id]) ? String(firstCommentMap[id]).trim() : '-';
    const motkhovna = getDropdownValue(d[MOTKHOVNA_FIELD], motkhovnaListMap) || '-';
    const dealUrl = `${BITRIX_DEAL_URL}/${id}/`;
    return { id, title, stageName, assignedByName, createdByName, dateCreate, redistributionDate, firstCommDate, firstComment, motkhovna, dealUrl };
  });
}

function renderHtml(report, serviceReport, stageMap, userMap, rows, serviceRows, dateFrom, dateTo) {
  const { metricA, metricB, percentage } = report;
  const svc = serviceReport || { metricA: 0, metricB: 0, percentage: 0 };
  const def = getDefaultDateRange();
  const from = dateFrom || def.dateFrom;
  const to = dateTo || def.dateTo;
  const rowsJson = JSON.stringify(rows);
  const serviceRowsJson = JSON.stringify(serviceRows || []);

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
    .table-wrap { margin-top:24px; overflow:visible; }
    table { width:100%; border-collapse:collapse; font-size:11px; }
    th, td { padding:6px 8px; text-align:left; border-bottom:1px solid #e5e7eb; vertical-align:top; }
    th { background:#f9fafb; font-weight:600; color:#374151; white-space:nowrap; }
    td { word-wrap:break-word; overflow-wrap:break-word; white-space:normal; }
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
    .tabs { display:flex; gap:0; margin:20px 0 0; border-bottom:2px solid #e5e7eb; }
    .tabs button { padding:12px 24px; background:#f9fafb; border:1px solid #e5e7eb; border-bottom:none; cursor:pointer; font-size:15px; font-weight:500; color:#6b7280; border-radius:8px 8px 0 0; margin-right:4px; }
    .tabs button:hover { background:#f3f4f6; color:#374151; }
    .tabs button.active { background:#fff; color:#2563eb; border-color:#e5e7eb; margin-bottom:-2px; padding-bottom:14px; }
    .tab-content { display:none; padding:24px 0 0; }
    .tab-content.active { display:block; }
  </style>
</head>
<body>
  <div class="container">
    <h1>Bitrix24 Sales Performance Report</h1>
    <form class="date-form" id="dateForm">
      <label>თარიღიდან:</label>
      <input type="date" name="date_from" value="${escapeHtml(from)}" required />
      <label>თარიღამდე:</label>
      <input type="date" name="date_to" value="${escapeHtml(to)}" required />
      <button type="submit">განახლება</button>
    </form>
    <script>
      document.getElementById('dateForm').addEventListener('submit', function(e) {
        e.preventDefault();
        var from = this.querySelector('[name="date_from"]').value;
        var to = this.querySelector('[name="date_to"]').value;
        if (!from || !to) return;
        if (from > to) { var t = from; from = to; to = t; }
        if (window.parent !== window) {
          window.parent.postMessage({ type: 'reportDateChange', date_from: from, date_to: to }, '*');
        } else {
          window.location.href = '/?date_from=' + encodeURIComponent(from) + '&date_to=' + encodeURIComponent(to);
        }
      });
    </script>
    <p>Pipeline 0 | ${escapeHtml(from)} – ${escapeHtml(to)}</p>
    <div class="tabs">
      <button type="button" class="tab-btn active" data-tab="report1">Buddys Leads</button>
      <button type="button" class="tab-btn" data-tab="serviceleads">Service Leads</button>
    </div>

    <div id="tab-report1" class="tab-content active">
      <h2>Buddys Leads</h2>
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
          <span>Deals</span>
          <div class="table-actions">
            <button type="button" class="btn btn-primary" id="btnExport">ექსპორტი Excel</button>
          </div>
        </div>
        <table>
          <thead>
            <tr><th>ვინ შექმნა</th><th>ID</th><th>სახელწოდება</th><th>პასუხისმგებელი</th><th>ეტაპი</th><th>შექმნის თარიღი</th><th>გადანაწილების თარიღი</th><th>პირველი კომუნიკაცია</th><th>პირველი კომენტარი</th><th>მოთხოვნა</th></tr>
          </thead>
          <tbody id="tableBody"></tbody>
        </table>
        <div class="pagination" id="pagination"></div>
      </div>
    </div>

    <div id="tab-serviceleads" class="tab-content">
      <h2>Service Leads</h2>
      <div class="metrics">
        <div class="card">
          <div class="card-title">რეგისტრირებული (Metric A)</div>
          <div class="card-value">${svc.metricA}</div>
        </div>
        <div class="card">
          <div class="card-title">დამუშავებული სეილის მიერ (Metric B)</div>
          <div class="card-value">${svc.metricB}</div>
        </div>
        <div class="card">
          <div class="card-title">Processing Rate</div>
          <div class="card-value accent">${svc.percentage}%</div>
        </div>
      </div>
      <div class="table-wrap">
        <div class="table-title">
          <span>Deals</span>
          <div class="table-actions">
            <button type="button" class="btn btn-primary" id="btnExportService">ექსპორტი Excel</button>
          </div>
        </div>
        <table>
          <thead>
            <tr><th>ვინ შექმნა</th><th>ID</th><th>სახელწოდება</th><th>პასუხისმგებელი</th><th>ეტაპი</th><th>შექმნის თარიღი</th><th>გადანაწილების თარიღი</th><th>პირველი კომუნიკაცია</th><th>პირველი კომენტარი</th><th>მოთხოვნა</th></tr>
          </thead>
          <tbody id="tableBodyService"></tbody>
        </table>
        <div class="pagination" id="paginationService"></div>
      </div>
    </div>

    <div class="footer">გენერირებული: ${new Date().toISOString()}</div>
  </div>
  <script>
    var ROWS = ${rowsJson};
    var SERVICE_ROWS = ${serviceRowsJson};
    var PAGE_SIZE = 25;
    var PAGE_SIZE_SERVICE = 25;
    var currentPage = 1;
    var currentPageService = 1;

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
        tbody.innerHTML = '<tr><td colspan="10">დილები არ მოიძებნა</td></tr>';
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
            PAGE_SIZE_SERVICE = parseInt(this.value, 10);
            currentPageService = Math.min(currentPageService, Math.max(1, Math.ceil(rows.length / PAGE_SIZE_SERVICE)));
            renderTableService();
          }
        });
        pagination.appendChild(sel);
        if (currentPageVar > 1) {
          var prev = document.createElement('button');
          prev.className = 'btn';
          prev.textContent = '← წინა';
          prev.onclick = function() {
            if (paginationId === 'pagination') { currentPage--; renderTable(); }
            else { currentPageService--; renderTableService(); }
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
            else { currentPageService++; renderTableService(); }
          };
          pagination.appendChild(next);
        }
      }
    }

    function renderTable() {
      renderPagination('tableBody', 'pagination', ROWS, PAGE_SIZE, currentPage, function(r) {
        return '<td>' + escapeHtml(r.createdByName) + '</td><td><a href="' + r.dealUrl + '" target="_blank" rel="noopener">' + escapeHtml(r.id) + '</a></td><td>' + escapeHtml(r.title) + '</td><td>' + escapeHtml(r.assignedByName) + '</td><td>' + escapeHtml(r.stageName) + '</td><td>' + escapeHtml(r.dateCreate) + '</td><td>' + escapeHtml(r.redistributionDate) + '</td><td>' + escapeHtml(r.firstCommDate) + '</td><td>' + escapeHtml(r.firstComment) + '</td><td>' + escapeHtml(r.motkhovna) + '</td>';
      });
    }

    function renderTableService() {
      renderPagination('tableBodyService', 'paginationService', SERVICE_ROWS, PAGE_SIZE_SERVICE, currentPageService, function(r) {
        return '<td>' + escapeHtml(r.createdByName) + '</td><td><a href="' + r.dealUrl + '" target="_blank" rel="noopener">' + escapeHtml(r.id) + '</a></td><td>' + escapeHtml(r.title) + '</td><td>' + escapeHtml(r.assignedByName) + '</td><td>' + escapeHtml(r.stageName) + '</td><td>' + escapeHtml(r.dateCreate) + '</td><td>' + escapeHtml(r.redistributionDate) + '</td><td>' + escapeHtml(r.firstCommDate) + '</td><td>' + escapeHtml(r.firstComment) + '</td><td>' + escapeHtml(r.motkhovna) + '</td>';
      });
    }

    document.querySelectorAll('.tab-btn').forEach(function(btn) {
      btn.addEventListener('click', function() {
        var tab = this.getAttribute('data-tab');
        document.querySelectorAll('.tab-btn').forEach(function(b) { b.classList.remove('active'); });
        document.querySelectorAll('.tab-content').forEach(function(c) { c.classList.remove('active'); });
        this.classList.add('active');
        document.getElementById('tab-' + tab).classList.add('active');
      });
    });

    function getSelectedDates() {
      var df = document.querySelector('[name="date_from"]');
      var dt = document.querySelector('[name="date_to"]');
      var from = (df && df.value) ? df.value : '${escapeHtml(from)}';
      var to = (dt && dt.value) ? dt.value : '${escapeHtml(to)}';
      if (from > to) { var t = from; from = to; to = t; }
      return { from: from, to: to };
    }

    function requestExport(tab) {
      var d = getSelectedDates();
      if (window.parent !== window) {
        window.parent.postMessage({ type: 'export', date_from: d.from, date_to: d.to, tab: tab || '' }, '*');
      } else {
        var url = '/export?date_from=' + encodeURIComponent(d.from) + '&date_to=' + encodeURIComponent(d.to);
        if (tab) url += '&tab=' + encodeURIComponent(tab);
        window.open(url, '_blank');
      }
    }

    document.getElementById('btnExport').onclick = function() {
      requestExport('');
    };

    document.getElementById('btnExportService').onclick = function() {
      requestExport('serviceleads');
    };

    renderTable();
    renderTableService();
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
  const def = getDefaultDateRange();
  let from = (req.query.date_from || def.dateFrom).toString().trim();
  let to = (req.query.date_to || def.dateTo).toString().trim();
  from = from || def.dateFrom;
  to = to || def.dateTo;
  if (from > to) [from, to] = [to, from];
  return { dateFrom: from, dateTo: to };
}

function renderShell(dateFrom, dateTo) {
  const def = getDefaultDateRange();
  const from = dateFrom || def.dateFrom;
  const to = dateTo || def.dateTo;
  return `
<!DOCTYPE html>
<html lang="ka">
<head>
  <meta charset="UTF-8" />
  <title>Bitrix24 Sales Performance Report</title>
  <style>
    body { font-family: system-ui, sans-serif; background:#f5f7fb; margin:0; padding:40px; }
    .loader-overlay { position:fixed; inset:0; background:rgba(255,255,255,.95); display:flex; flex-direction:column; align-items:center; justify-content:center; z-index:9999; }
    .loader-overlay.hidden { display:none; }
    .loader-spinner { width:48px; height:48px; border:4px solid #e5e7eb; border-top-color:#2563eb; border-radius:50%; animation:spin 1s linear infinite; margin-bottom:20px; }
    .loader-text { font-size:18px; font-weight:500; color:#374151; margin-bottom:12px; }
    .loader-bar { width:320px; height:8px; background:#e5e7eb; border-radius:4px; overflow:hidden; }
    .loader-fill { height:100%; background:#2563eb; border-radius:4px; transition:width .2s ease; }
    @keyframes spin { to { transform:rotate(360deg); } }
    #report-frame { width:100%; min-height:90vh; border:none; display:block; margin-top:0; }
  </style>
</head>
<body>
  <div class="loader-overlay" id="loader">
    <div class="loader-spinner"></div>
    <div class="loader-text" id="loaderText">ჩატვირთვა... 0%</div>
    <div class="loader-bar"><div class="loader-fill" id="loaderFill" style="width:0%"></div></div>
  </div>
  <iframe id="report-frame"></iframe>
  <script>
    var loader = document.getElementById('loader');
    var loaderText = document.getElementById('loaderText');
    var loaderFill = document.getElementById('loaderFill');
    var frame = document.getElementById('report-frame');

    function setProgress(value, label) {
      loaderFill.style.width = value + '%';
      loaderText.textContent = (label || 'ჩატვირთვა...') + ' ' + value + '%';
    }

    function hideLoader() {
      setProgress(100, 'მზადაა');
      setTimeout(function() {
        loader.classList.add('hidden');
      }, 300);
    }

    loader.classList.remove('hidden');
    setProgress(0, 'ჩატვირთვა...');

    window.addEventListener('message', function(event) {
      if (!event.data || !event.data.type) return;
      if (event.data.type === 'reportDateChange' && event.data.date_from && event.data.date_to) {
        window.location.href = '/?date_from=' + encodeURIComponent(event.data.date_from) + '&date_to=' + encodeURIComponent(event.data.date_to);
        return;
      }
      if (event.data.type === 'export' && event.data.date_from && event.data.date_to) {
        var url = '/export?date_from=' + encodeURIComponent(event.data.date_from) + '&date_to=' + encodeURIComponent(event.data.date_to);
        if (event.data.tab) url += '&tab=' + encodeURIComponent(event.data.tab);
        window.open(url, '_blank');
        return;
      }
    });

    function getStreamUrl() {
      var params = new URLSearchParams(window.location.search);
      var from = params.get('date_from') || '${escapeHtml(from)}';
      var to = params.get('date_to') || '${escapeHtml(to)}';
      if (from > to) { var t = from; from = to; to = t; }
      return '/api/report-stream?date_from=' + encodeURIComponent(from) + '&date_to=' + encodeURIComponent(to);
    }

    fetch(getStreamUrl(), { cache: 'no-store' })
      .then(function(r) {
        if (!r.ok) throw new Error(r.statusText);
        return r.body.getReader();
      })
      .then(function(reader) {
        var decoder = new TextDecoder();
        var buf = '';
        return reader.read().then(function processChunk(result) {
          if (result.done) return;
          buf += decoder.decode(result.value, { stream: true });
          var lines = buf.split(/\\r?\\n/);
          buf = lines.pop();
          for (var i = 0; i < lines.length; i++) {
            var line = lines[i].trim();
            if (!line) continue;
            try {
              var obj = JSON.parse(line);
              if (obj.type === 'progress') {
                setProgress(obj.value, obj.label || 'ჩატვირთვა...');
              } else if (obj.type === 'html') {
                var blob = new Blob([obj.content], { type: 'text/html; charset=utf-8' });
                var blobUrl = URL.createObjectURL(blob);
                frame.src = blobUrl;
                frame.onload = function() { URL.revokeObjectURL(blobUrl); };
                hideLoader();
                return;
              } else if (obj.type === 'error') {
                loaderText.textContent = 'შეცდომა: ' + (obj.message || 'შეცდომა');
                loaderText.style.color = '#b91c1c';
                return;
              }
            } catch (e) {
              if (e instanceof SyntaxError) continue;
              throw e;
            }
          }
          return reader.read().then(processChunk);
        });
      })
      .catch(function(err) {
        loaderText.textContent = 'შეცდომა: ' + (err.message || err);
        loaderText.style.color = '#b91c1c';
      });
  </script>
</body>
</html>`;
}

app.get('/', (req, res) => {
  const { dateFrom, dateTo } = getDateParams(req);
  res.status(200).send(renderShell(dateFrom, dateTo));
});

app.get('/api/report', async (req, res) => {
  try {
    const { dateFrom, dateTo } = getDateParams(req);
    const [deals, serviceDeals] = await Promise.all([
      fetchAllDeals(dateFrom, dateTo),
      fetchServiceLeadsDeals(dateFrom, dateTo),
    ]);
    const report = buildReport(deals);
    const serviceReport = buildReport(serviceDeals);

    const userIds = new Set();
    for (const d of [...report.deals, ...serviceReport.deals]) {
      const a = norm(d.ASSIGNED_BY_ID);
      if (a) userIds.add(a);
      let c = d[CREATED_BY_FIELD];
      if (c && typeof c === 'object' && c.id != null) c = c.id;
      if (Array.isArray(c)) c = c[0];
      if (c != null && c !== '' && /^\d+$/.test(String(c).trim())) userIds.add(String(c).trim());
      const nameField = d[CREATED_BY_NAME_FIELD];
      if (nameField != null && /^\d+$/.test(String(nameField).trim())) userIds.add(String(nameField).trim());
    }

    const [stageMap, userMap, motkhovnaListMap] = await Promise.all([
      fetchDealStages(),
      fetchUserNames([...userIds]),
      fetchMotkhovnaListMap(),
    ]);

    const seenIds = new Set();
    const allDealsForComm = [];
    for (const d of [...report.deals, ...serviceReport.deals]) {
      const id = String(norm(d.ID) || '').trim();
      if (id && !seenIds.has(id)) {
        seenIds.add(id);
        allDealsForComm.push(d);
      }
    }
    const { firstCommMap, firstCommentMap } = await fetchFirstCommDatesForDeals(allDealsForComm);

    const rows = buildRows(report.deals, stageMap, userMap, firstCommMap, firstCommentMap, motkhovnaListMap);
    const serviceRows = buildRows(serviceReport.deals, stageMap, userMap, firstCommMap, firstCommentMap, motkhovnaListMap);
    res.status(200).send(renderHtml(report, serviceReport, stageMap, userMap, rows, serviceRows, dateFrom, dateTo));
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

app.get('/api/report-stream', async (req, res) => {
  res.setHeader('Content-Type', 'application/x-ndjson');
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const send = (obj) => res.write(JSON.stringify(obj) + '\n');

  try {
    const { dateFrom, dateTo } = getDateParams(req);
    const cacheKey = getCacheKey(dateFrom, dateTo);
    send({ type: 'progress', value: 5, label: 'დილების ჩატვირთვა...' });

    const [deals, serviceDeals] = await Promise.all([
      fetchAllDeals(dateFrom, dateTo),
      fetchServiceLeadsDeals(dateFrom, dateTo),
    ]);
    send({ type: 'progress', value: 25, label: 'რეპორტის მომზადება...' });

    const report = buildReport(deals);
    const serviceReport = buildReport(serviceDeals);
    const userIds = new Set();
    for (const d of [...report.deals, ...serviceReport.deals]) {
      const a = norm(d.ASSIGNED_BY_ID);
      if (a) userIds.add(a);
      let c = d[CREATED_BY_FIELD];
      if (c && typeof c === 'object' && c.id != null) c = c.id;
      if (Array.isArray(c)) c = c[0];
      if (c != null && c !== '' && /^\d+$/.test(String(c).trim())) userIds.add(String(c).trim());
      const nameField = d[CREATED_BY_NAME_FIELD];
      if (nameField != null && /^\d+$/.test(String(nameField).trim())) userIds.add(String(nameField).trim());
    }

    const [stageMap, userMap, motkhovnaListMap] = await Promise.all([
      fetchDealStages(),
      fetchUserNames([...userIds]),
      fetchMotkhovnaListMap(),
    ]);
    send({ type: 'progress', value: 40, label: 'პირველი კომუნიკაციის თარიღები...' });

    const seenIds = new Set();
    const allDealsForComm = [];
    for (const d of [...report.deals, ...serviceReport.deals]) {
      const id = String(norm(d.ID) || '').trim();
      if (id && !seenIds.has(id)) {
        seenIds.add(id);
        allDealsForComm.push(d);
      }
    }

    const { firstCommMap, firstCommentMap } = await fetchFirstCommDatesForDeals(allDealsForComm, (pct) => {
      const v = 40 + Math.round((pct / 100) * 45);
      send({ type: 'progress', value: Math.min(v, 85), label: 'პირველი კომუნიკაციის თარიღები...' });
    });
    send({ type: 'progress', value: 90, label: 'ფინალიზაცია...' });

    const rows = buildRows(report.deals, stageMap, userMap, firstCommMap, firstCommentMap, motkhovnaListMap);
    const serviceRows = buildRows(serviceReport.deals, stageMap, userMap, firstCommMap, firstCommentMap, motkhovnaListMap);
    setReportCache(cacheKey, { report, serviceReport, stageMap, userMap, motkhovnaListMap, firstCommMap, firstCommentMap, deals, serviceDeals });
    const html = renderHtml(report, serviceReport, stageMap, userMap, rows, serviceRows, dateFrom, dateTo);
    send({ type: 'progress', value: 100, label: 'მზადაა' });
    send({ type: 'html', content: html });
  } catch (err) {
    console.error(err);
    send({ type: 'error', message: asStr(err && err.message ? err.message : err) });
  } finally {
    res.end();
  }
});

app.get('/export', async (req, res) => {
  try {
    const { dateFrom, dateTo } = getDateParams(req);
    const isServiceLeads = req.query.tab === 'serviceleads';
    const cacheKey = getCacheKey(dateFrom, dateTo);
    let cached = getReportCache(cacheKey);

    let report;
    let serviceReport;
    let stageMap;
    let userMap;
    let motkhovnaListMap;
    let firstCommMap;
    let firstCommentMap;
    let allDeals;

    if (cached) {
      report = cached.report;
      serviceReport = cached.serviceReport;
      stageMap = cached.stageMap;
      userMap = cached.userMap;
      motkhovnaListMap = cached.motkhovnaListMap;
      firstCommMap = cached.firstCommMap;
      firstCommentMap = cached.firstCommentMap || {};
      allDeals = isServiceLeads ? serviceReport.deals : report.deals;
    } else {
      const [deals, serviceDeals] = await Promise.all([
        fetchAllDeals(dateFrom, dateTo),
        fetchServiceLeadsDeals(dateFrom, dateTo),
      ]);
      report = buildReport(deals);
      serviceReport = buildReport(serviceDeals);
      allDeals = isServiceLeads ? serviceReport.deals : report.deals;
      const userIds = new Set();
      for (const d of [...report.deals, ...serviceReport.deals]) {
        const a = norm(d.ASSIGNED_BY_ID);
        if (a) userIds.add(a);
        let c = d[CREATED_BY_FIELD];
        if (c && typeof c === 'object' && c.id != null) c = c.id;
        if (Array.isArray(c)) c = c[0];
        if (c != null && c !== '' && /^\d+$/.test(String(c).trim())) userIds.add(String(c).trim());
        const nameField = d[CREATED_BY_NAME_FIELD];
        if (nameField != null && /^\d+$/.test(String(nameField).trim())) userIds.add(String(nameField).trim());
      }

      [stageMap, userMap, motkhovnaListMap] = await Promise.all([
        fetchDealStages(),
        fetchUserNames([...userIds]),
        fetchMotkhovnaListMap(),
      ]);

      const seenIds = new Set();
      const allDealsForComm = [];
      for (const d of [...report.deals, ...serviceReport.deals]) {
        const id = String(norm(d.ID) || '').trim();
        if (id && !seenIds.has(id)) {
          seenIds.add(id);
          allDealsForComm.push(d);
        }
      }
      if (allDealsForComm.length <= EXPORT_SKIP_FIRST_COMM_OVER) {
        const fetched = await fetchFirstCommDatesForDeals(allDealsForComm);
        firstCommMap = fetched.firstCommMap;
        firstCommentMap = fetched.firstCommentMap;
      } else {
        firstCommMap = {};
        firstCommentMap = {};
      }
      setReportCache(cacheKey, { report, serviceReport, stageMap, userMap, motkhovnaListMap, firstCommMap, firstCommentMap, deals, serviceDeals });
    }

    const rows = buildRows(allDeals, stageMap, userMap, firstCommMap, firstCommentMap, motkhovnaListMap);
    const data = [
      ['ვინ შექმნა', 'ID', 'სახელწოდება', 'პასუხისმგებელი', 'ეტაპი', 'შექმნის თარიღი', 'გადანაწილების თარიღი', 'პირველი კომუნიკაცია', 'პირველი კომენტარი', 'მოთხოვნა'],
      ...rows.map((r) => [r.createdByName, r.id, r.title, r.assignedByName, r.stageName, r.dateCreate, r.redistributionDate, r.firstCommDate, r.firstComment, r.motkhovna]),
    ];

    const wb = XLSX.utils.book_new();
    const ws = XLSX.utils.aoa_to_sheet(data);
    XLSX.utils.book_append_sheet(wb, ws, isServiceLeads ? 'Service Leads' : 'დილები');
    const buf = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename="bitrix_report' + (isServiceLeads ? '_serviceleads_' : '_') + dateFrom + '_' + dateTo + '.xlsx"');
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
