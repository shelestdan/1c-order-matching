/* ===== State ===== */
// On GitHub Pages, point to the Railway backend; locally use same origin.
const API = window.location.hostname.endsWith('github.io')
  ? 'https://1c-matching-api-production-0a95.up.railway.app'
  : '';
let token = localStorage.getItem('auth_token') || '';
let currentUser = JSON.parse(localStorage.getItem('auth_user') || 'null');
let currentJob = null;
let allRows = [];
let currentFilter = 'all';
let pendingApprovals = {};  // row_id -> analog code_1c
let searchResultsByRow = {}; // row_id -> manual search results
let selectionInFlightRows = new Set();
let qtyUpdateInFlightRows = new Set();
let analyticsData = null;

/* ===== Init ===== */
document.addEventListener('DOMContentLoaded', () => {
  if (token) {
    restoreSession();
  } else {
    showScreen('login-screen');
  }

  // Login form
  document.getElementById('login-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const username = document.getElementById('login-username').value.trim();
    const pw = document.getElementById('login-password').value;
    const err = document.getElementById('login-error');
    err.textContent = '';
    try {
      const res = await apiFetch('/api/login', 'POST', { username, password: pw });
      token = res.token;
      currentUser = res.user;
      localStorage.setItem('auth_token', token);
      localStorage.setItem('auth_user', JSON.stringify(currentUser));
      showApp();
    } catch (ex) {
      err.textContent = ex.message || 'Неверный логин или пароль';
    }
  });

  // Drop zone
  const dz = document.getElementById('drop-zone');
  const fi = document.getElementById('file-input');

  dz.addEventListener('click', () => fi.click());
  fi.addEventListener('change', () => { if (fi.files[0]) handleFile(fi.files[0]); });

  dz.addEventListener('dragover', (e) => { e.preventDefault(); dz.classList.add('dragover'); });
  dz.addEventListener('dragleave', () => dz.classList.remove('dragover'));
  dz.addEventListener('drop', (e) => {
    e.preventDefault();
    dz.classList.remove('dragover');
    if (e.dataTransfer.files[0]) handleFile(e.dataTransfer.files[0]);
  });

  const manualSearchInput = document.getElementById('manual-search-input');
  manualSearchInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      runManualSearch();
    }
  });

  // Ctrl+Enter in textarea to submit
  document.getElementById('text-input').addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
      e.preventDefault();
      handleTextInput();
    }
  });
});

/* ===== Screens ===== */
function showScreen(id) {
  document.querySelectorAll('.screen').forEach(s => s.classList.remove('active'));
  document.getElementById(id).classList.add('active');
}

function showApp() {
  updateUserUI();
  showScreen('app-screen');
  showUpload();
}

function showUpload() {
  document.getElementById('upload-section').style.display = '';
  document.getElementById('progress-section').style.display = 'none';
  document.getElementById('results-section').style.display = 'none';
  document.getElementById('history-section').style.display = 'none';
  document.getElementById('analytics-section').style.display = 'none';
  document.getElementById('file-input').value = '';
}

function showHistory() {
  document.getElementById('upload-section').style.display = 'none';
  document.getElementById('results-section').style.display = 'none';
  document.getElementById('history-section').style.display = '';
  document.getElementById('analytics-section').style.display = 'none';
  loadHistory();
}

function showAnalytics() {
  document.getElementById('upload-section').style.display = 'none';
  document.getElementById('results-section').style.display = 'none';
  document.getElementById('history-section').style.display = 'none';
  document.getElementById('analytics-section').style.display = '';
  loadAnalytics();
}

function logout() {
  token = '';
  currentUser = null;
  localStorage.removeItem('auth_token');
  localStorage.removeItem('auth_user');
  showScreen('login-screen');
}

async function restoreSession() {
  try {
    const me = await apiFetch('/api/me');
    currentUser = me;
    localStorage.setItem('auth_user', JSON.stringify(currentUser));
    showApp();
  } catch (ex) {
    logout();
  }
}

function updateUserUI() {
  const badge = document.getElementById('current-user-badge');
  const analyticsButton = document.getElementById('btn-analytics');
  if (!currentUser) {
    badge.textContent = '';
    analyticsButton.style.display = 'none';
    return;
  }
  const roleLabel = currentUser.is_admin ? 'Админ' : 'Менеджер';
  badge.textContent = `${currentUser.display_name} · ${roleLabel}`;
  analyticsButton.style.display = currentUser.is_admin ? '' : 'none';
}

/* ===== API helper ===== */
async function apiFetch(path, method = 'GET', body = null) {
  const opts = {
    method,
    headers: { 'Authorization': `Bearer ${token}` },
  };
  if (body && !(body instanceof FormData)) {
    opts.headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(body);
  } else if (body instanceof FormData) {
    opts.body = body;
  }
  const res = await fetch(API + path, opts);
  if (res.status === 401) {
    token = '';
    currentUser = null;
    localStorage.removeItem('auth_token');
    localStorage.removeItem('auth_user');
    showScreen('login-screen');
    throw new Error('Сессия истекла');
  }
  const data = await res.json();
  if (!res.ok) throw new Error(data.detail || 'Ошибка сервера');
  return data;
}

/* ===== File upload ===== */
async function handleFile(file) {
  document.getElementById('upload-section').style.display = 'none';
  document.getElementById('progress-section').style.display = '';
  document.getElementById('progress-text').textContent = 'Обработка файла...';
  document.getElementById('progress-detail').textContent = file.name;

  try {
    const fd = new FormData();
    fd.append('file', file);
    const data = await apiFetch('/api/upload', 'POST', fd);
    currentJob = data;
    allRows = data.rows || [];
    pendingApprovals = {};
    searchResultsByRow = {};
    renderResults(data);
  } catch (ex) {
    document.getElementById('progress-text').textContent = 'Ошибка обработки';
    document.getElementById('progress-detail').textContent = ex.message;
  }
}

/* ===== Text input ===== */
async function handleTextInput() {
  const textarea = document.getElementById('text-input');
  const text = textarea.value.trim();
  if (!text) {
    textarea.focus();
    return;
  }
  const lines = text.split('\n').filter(l => l.trim());
  if (lines.length === 0) return;

  // Wrap as plain-text file and reuse existing upload pipeline
  const blob = new Blob([text], { type: 'text/plain' });
  const file = new File([blob], 'manual_input.txt', { type: 'text/plain' });
  textarea.value = '';
  await handleFile(file);
}

/* ===== Results ===== */
function renderResults(data) {
  document.getElementById('progress-section').style.display = 'none';
  document.getElementById('results-section').style.display = '';
  const alertBox = document.getElementById('results-alert');
  if (alertBox) {
    const parsedCount = Number(data.parsed_count || 0);
    const issueCount = Number(data.issue_count || 0);
    if (parsedCount === 0 && issueCount > 0) {
      alertBox.style.display = '';
      alertBox.textContent = 'Из файла не удалось извлечь строки заявки. Для скриншотов и фото на сервере должен быть доступен OCR, иначе сайт покажет пустой результат.';
    } else {
      alertBox.style.display = 'none';
      alertBox.textContent = '';
    }
  }

  // Stats
  const counts = data.status_counts || {};
  const exactCount = (counts['Найдено полностью'] || 0) + (counts['Найдено частично'] || 0);
  const safeCount = counts['Безопасный аналог'] || 0;
  const approvalCount = (counts['Нужна проверка аналога'] || 0)
                      + (counts['Допустимая замена по согласованию'] || 0)
                      + (counts['Найдено, но остаток уже распределен'] || 0);
  const approvedCount = counts['Одобрена замена'] || 0;
  const notfoundCount = counts['Не найдено'] || 0;

  document.getElementById('stat-exact').textContent = exactCount;
  document.getElementById('stat-safe').textContent = safeCount;
  document.getElementById('stat-approval').textContent = approvalCount;
  document.getElementById('stat-approved').textContent = approvedCount;
  document.getElementById('stat-notfound').textContent = notfoundCount;

  filterRows(currentFilter);
}

const STATUS_MAP = {
  'Найдено полностью':                   { cls: 'exact',    label: 'Найдено' },
  'Найдено частично':                    { cls: 'exact',    label: 'Найдено ч.' },
  'Безопасный аналог':                   { cls: 'safe',     label: 'Аналог ✓' },
  'Нужна проверка аналога':              { cls: 'approval', label: 'На согласование' },
  'Допустимая замена по согласованию':   { cls: 'approval', label: 'На согласование' },
  'Найдено, но остаток уже распределен': { cls: 'approval', label: 'Нет в наличии' },
  'Одобрена замена':                     { cls: 'approved', label: 'Одобрено' },
  'Не найдено':                          { cls: 'notfound', label: 'Не найдено' },
};

function filterRows(filter) {
  currentFilter = filter;
  document.querySelectorAll('.tab').forEach(t => {
    t.classList.toggle('active', t.dataset.filter === filter);
  });

  let rows = allRows;
  if (filter === 'exact') {
    rows = allRows.filter(r => r.status === 'Найдено полностью' || r.status === 'Найдено частично');
  } else if (filter === 'analog') {
    rows = allRows.filter(r =>
      r.status === 'Безопасный аналог' ||
      r.status === 'Нужна проверка аналога' ||
      r.status === 'Допустимая замена по согласованию' ||
      r.status === 'Найдено, но остаток уже распределен'
    );
  } else if (filter === 'approved') {
    rows = allRows.filter(r => r.status === 'Одобрена замена');
  } else if (filter === 'notfound') {
    rows = allRows.filter(r => r.status === 'Не найдено');
  }

  renderTable(rows);
}

function renderTable(rows) {
  const tbody = document.getElementById('results-body');
  tbody.innerHTML = '';

  rows.forEach((row, i) => {
    const info = STATUS_MAP[row.status] || { cls: 'notfound', label: row.status };
    const tr = document.createElement('tr');
    tr.className = `row-${info.cls}`;

    const matchCell = buildMatchCell(row);
    const actionCell = buildActionCell(row);

    tr.innerHTML = `
      <td>${row.position || (i + 1)}</td>
      <td><span class="badge badge-${info.cls}">${info.label}</span></td>
      <td>
        <div class="match-name">${esc(row.name || '')}</div>
        ${row.mark ? `<div class="match-code">${esc(row.mark)}</div>` : ''}
        ${row.vendor ? `<div class="match-comment">${esc(row.vendor)}</div>` : ''}
      </td>
      <td class="col-qty">${buildQtyCell(row)}</td>
      <td>${matchCell}</td>
      <td class="col-score">${row.confidence != null ? row.confidence : ''}</td>
      <td class="col-remaining">${displayRemainingQty(row)}</td>
      <td>${actionCell}</td>
    `;
    tbody.appendChild(tr);
  });
}

const APPROVAL_STATUSES = new Set([
  'Нужна проверка аналога',
  'Допустимая замена по согласованию',
  'Найдено, но остаток уже распределен',
  'Безопасный аналог',
]);

function warehouseBadge(label) {
  if (!label) return '';
  return `<span class="warehouse-badge">${esc(label)}</span>`;
}

function formatQty(value) {
  if (value == null || value === '') return '';
  const num = Number(value);
  if (!Number.isFinite(num)) return esc(String(value));
  if (Number.isInteger(num)) return String(num);
  return String(num).replace(/\.0+$/, '').replace(/(\.\d*?)0+$/, '$1');
}

function isJobEditable() {
  return !!currentJob && !currentJob.saved_at && !(Number(currentJob.export_count || 0) > 0);
}

function buildQtyCell(row) {
  if (!isJobEditable()) {
    return formatQty(row.requested_qty);
  }
  const isBusy = qtyUpdateInFlightRows.has(row.id);
  return `
    <div class="qty-editor">
      <input
        id="qty-${row.id}"
        class="qty-input"
        type="number"
        min="0.001"
        step="0.001"
        value="${formatQty(row.requested_qty)}"
        ${isBusy ? 'disabled' : ''}
        onkeydown="handleQtyKey(event, '${row.id}')"
      >
      <button
        class="btn btn-secondary btn-sm qty-save-btn"
        ${isBusy ? 'disabled' : ''}
        onclick="saveRowQuantity('${row.id}')"
      >${isBusy ? '...' : 'OK'}</button>
    </div>
  `;
}

function displayRemainingQty(row) {
  if (row.approved_analog) {
    return formatQty(row.approved_analog.stock_qty ?? row.approved_analog.remaining);
  }
  if (row.matched_stock_qty != null) {
    return formatQty(row.matched_stock_qty);
  }
  if (APPROVAL_STATUSES.has(row.status) && row.analogs && row.analogs.length) {
    const sorted = [...row.analogs].sort((a, b) => {
      const aEK = a.source_label === 'ЭК' ? 0 : 1;
      const bEK = b.source_label === 'ЭК' ? 0 : 1;
      if (aEK !== bEK) return aEK - bEK;
      return (b.score || 0) - (a.score || 0);
    });
    return formatQty(sorted[0].stock_qty ?? sorted[0].remaining);
  }
  return formatQty(row.available_qty);
}

function managerChoiceBadge(enabled) {
  if (!enabled) return '';
  return '<span class="manager-choice-badge">выбор менеджеров</span>';
}

function buildMatchCell(row) {
  if (row.approved_analog) {
    const a = row.approved_analog;
    return `<div class="match-name">${esc(a.name)} ${warehouseBadge(a.source_label)} ${managerChoiceBadge(a.manager_choice)}</div>
            <div class="match-code">${esc(a.code_1c)}</div>
            <div class="match-comment" style="color:var(--approved)">Одобрено менеджером</div>`;
  }
  if (row.matched_name) {
    const depletedNote = row.status === 'Найдено, но остаток уже распределен'
      ? `<div class="match-comment" style="color:var(--approval)">Остаток исчерпан</div>` : '';
    return `<div class="match-name">${esc(row.matched_name)} ${warehouseBadge(row.matched_source_label)}</div>
            <div class="match-code">${esc(row.matched_code || '')}</div>
            ${row.comment ? `<div class="match-comment">${esc(row.comment)}</div>` : ''}
            ${depletedNote}`;
  }
  if (APPROVAL_STATUSES.has(row.status) && row.analogs && row.analogs.length) {
    // Prefer ЭК item if available
    const sorted = [...row.analogs].sort((a, b) => {
      const aEK = a.source_label === 'ЭК' ? 0 : 1;
      const bEK = b.source_label === 'ЭК' ? 0 : 1;
      if (aEK !== bEK) return aEK - bEK;
      return (b.score || 0) - (a.score || 0);
    });
    const a = sorted[0];
    const zeroStock = a.remaining === 0 || a.remaining === '0';
    return `<div class="match-name">${esc(a.name)} ${warehouseBadge(a.source_label)} ${managerChoiceBadge(a.manager_choice)}</div>
            <div class="match-code">${esc(a.code_1c)}</div>
            <div class="match-comment">${zeroStock ? '<span style="color:var(--notfound)">Нет в наличии · </span>' : ''}Лучший аналог (score ${a.score})</div>`;
  }
  return `<span style="color:var(--text-muted)">—</span>`;
}

function buildActionCell(row) {
  const id = row.id;
  if (row.status === 'Не найдено') {
    return `<button class="btn btn-secondary btn-sm" onclick="openModal('${id}')">Найти товар</button>`;
  }
  if (row.status === 'Одобрена замена') {
    return `<button class="btn btn-secondary btn-sm" onclick="openModal('${id}')">Изменить</button>`;
  }
  if (row.status === 'Безопасный аналог') {
    return `<div class="action-stack">
      <button class="btn btn-ghost btn-sm" onclick="openModal('${id}')">Другой аналог</button>
      <button class="btn btn-ghost btn-sm replace-btn" onclick="openModal('${id}')">Заменить</button>
    </div>`;
  }
  if (APPROVAL_STATUSES.has(row.status) && !row.approved_analog) {
    const hasAnalogs = row.analogs && row.analogs.length > 0;
    return `<button class="btn ${hasAnalogs ? 'btn-approve' : 'btn-secondary'} btn-sm"
      onclick="openModal('${id}')">${hasAnalogs ? 'Выбрать аналог' : 'Найти товар'}</button>`;
  }
  // Exact match — small ghost button to replace if needed
  return `<button class="btn btn-ghost btn-sm replace-btn" onclick="openModal('${id}')">Заменить</button>`;
}

function handleQtyKey(event, rowId) {
  if (event.key === 'Enter') {
    event.preventDefault();
    saveRowQuantity(rowId);
  }
}

async function saveRowQuantity(rowId) {
  if (!currentJob || qtyUpdateInFlightRows.has(rowId)) return;
  const row = allRows.find(r => r.id === rowId);
  const input = document.getElementById(`qty-${rowId}`);
  if (!row || !input) return;
  const nextQty = Number(String(input.value || '').replace(',', '.'));
  if (!Number.isFinite(nextQty) || nextQty <= 0) {
    alert('Количество должно быть больше нуля');
    input.focus();
    return;
  }
  if (Number(row.requested_qty) === nextQty) return;

  qtyUpdateInFlightRows.add(rowId);
  filterRows(currentFilter);
  try {
    const res = await apiFetch(`/api/jobs/${currentJob.job_id}/rows/${rowId}/quantity`, 'POST', {
      quantity: nextQty,
    });
    currentJob = { ...currentJob, ...res, job_id: currentJob.job_id };
    allRows = res.rows || [];
    renderResults(currentJob);
  } catch (ex) {
    qtyUpdateInFlightRows.delete(rowId);
    filterRows(currentFilter);
    alert('Ошибка изменения количества: ' + ex.message);
    return;
  }
  qtyUpdateInFlightRows.delete(rowId);
  filterRows(currentFilter);
}

function esc(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function mergeVisibleCandidates(...groups) {
  const byCode = new Map();
  groups.forEach((group) => {
    (group || []).forEach((candidate) => {
      if (!candidate || !candidate.code_1c || !candidate.name) return;
      const code = String(candidate.code_1c).trim();
      if (!code) return;
      const current = byCode.get(code);
      if (!current) {
        byCode.set(code, { ...candidate });
        return;
      }
      if ((!current.reasons || current.reasons.length === 0) && candidate.reasons?.length) {
        current.reasons = [...candidate.reasons];
      }
      if (!current.source_label && candidate.source_label) {
        current.source_label = candidate.source_label;
      }
      if ((Number(candidate.score) || 0) > (Number(current.score) || 0)) {
        current.score = candidate.score;
      }
      if ((current.remaining == null || current.remaining === '') && candidate.remaining != null) {
        current.remaining = candidate.remaining;
      }
      if ((current.stock_qty == null || current.stock_qty === '') && candidate.stock_qty != null) {
        current.stock_qty = candidate.stock_qty;
      }
      if (!current.price && candidate.price) {
        current.price = candidate.price;
      }
      if (candidate.manager_choice) {
        current.manager_choice = true;
      }
    });
  });
  return Array.from(byCode.values());
}

/* ===== Analog Modal ===== */
let modalRowId = null;

function isGenericManualSearchMark(value) {
  const mark = String(value || '').trim();
  if (!mark) return true;
  const upper = mark.toUpperCase();
  if (['HT', 'KG', 'ST'].includes(upper)) return true;
  return /^[A-ZА-Я]{1,3}$/.test(upper) && !/[0-9]/.test(upper);
}

function buildManualSearchPrefill(row) {
  const name = String(row?.name || '').trim();
  const mark = String(row?.mark || '').trim();
  const matchedCode = String(row?.matched_code || '').trim();

  if (name) {
    if (mark && !isGenericManualSearchMark(mark) && !name.toLowerCase().includes(mark.toLowerCase())) {
      return `${name} ${mark}`.trim().substring(0, 80);
    }
    if (matchedCode && /[0-9]/.test(matchedCode) && !name.toLowerCase().includes(matchedCode.toLowerCase())) {
      return `${name} ${matchedCode}`.trim().substring(0, 80);
    }
    return name.substring(0, 80);
  }

  if (mark) return mark.substring(0, 80);
  return matchedCode.substring(0, 80);
}

function openModal(rowId) {
  modalRowId = rowId;
  const row = allRows.find(r => r.id === rowId);
  if (!row) return;

  document.getElementById('modal-order-name').textContent =
    `${row.name}${row.mark ? ' · ' + row.mark : ''}${row.vendor ? ' · ' + row.vendor : ''}`;

  const searchInput = document.getElementById('manual-search-input');
  searchInput.value = buildManualSearchPrefill(row);

  renderCandidateList('analog-list', row.analogs || [], rowId, 'analog');
  renderCandidateList('search-results-list', searchResultsByRow[rowId] || [], rowId, 'search');

  document.getElementById('analog-modal').style.display = 'flex';
}

function closeModal() {
  document.getElementById('analog-modal').style.display = 'none';
  modalRowId = null;
}

document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') closeModal();
});

// Modal closes only via X button or Escape — not on backdrop click

async function selectAnalog(rowId, code) {
  pendingApprovals[rowId] = code;
  const row = allRows.find(r => r.id === rowId);
  const visibleCandidates = mergeVisibleCandidates(row?.analogs || [], searchResultsByRow[rowId] || []);

  const res = await apiFetch(`/api/jobs/${currentJob.job_id}/approve`, 'POST', {
    approvals: { [rowId]: code },
    candidate_pools: { [rowId]: visibleCandidates },
  });

  if (row) {
    const analog = row.analogs.find(a => a.code_1c === code);
    if (analog) {
      row.approved_analog = analog;
      row.status = 'Одобрена замена';
    }
  }

  if (currentJob && res.status_counts) {
    currentJob.status_counts = res.status_counts;
  }

  closeModal();
  renderResults(currentJob);
}

function renderCandidateList(containerId, candidates, rowId, source) {
  const container = document.getElementById(containerId);
  container.innerHTML = '';
  // Sort ЭК items first, then by score descending
  if (candidates && candidates.length) {
    candidates = [...candidates].sort((a, b) => {
      const aEK = a.source_label === 'ЭК' ? 0 : 1;
      const bEK = b.source_label === 'ЭК' ? 0 : 1;
      if (aEK !== bEK) return aEK - bEK;
      return (b.score || 0) - (a.score || 0);
    });
  }
  if (!candidates || !candidates.length) {
    const empty = document.createElement('div');
    empty.className = 'empty-state';
    empty.textContent = source === 'search'
      ? 'Введите запрос и выполните поиск по остаткам.'
      : 'Для этой строки пока нет предложенных аналогов.';
    container.appendChild(empty);
    return;
  }

  candidates.forEach((candidate) => {
    const isSelected = pendingApprovals[rowId] === candidate.code_1c
      || allRows.find(r => r.id === rowId)?.approved_analog?.code_1c === candidate.code_1c;
    const isBusy = selectionInFlightRows.has(rowId);
    const card = document.createElement('div');
    card.className = 'analog-card';
    card.innerHTML = `
      <div class="analog-info">
        <div class="analog-name">${esc(candidate.name)} ${warehouseBadge(candidate.source_label)} ${managerChoiceBadge(candidate.manager_choice)}</div>
        <div class="analog-code">${esc(candidate.code_1c)}</div>
        <div class="analog-meta">
          ${candidate.score != null ? `<span>Score: <strong>${candidate.score}</strong></span>` : ''}
          ${candidate.remaining != null ? `<span>Остаток: <strong>${candidate.remaining}</strong></span>` : ''}
          ${candidate.price != null && candidate.price !== '' ? `<span>Цена: <strong>${candidate.price}</strong></span>` : ''}
          ${source === 'search' ? `<span>Источник: <strong>поиск</strong></span>` : ''}
        </div>
        ${candidate.reasons && candidate.reasons.length ? `<div class="analog-reasons">${candidate.reasons.map(esc).join('; ')}</div>` : ''}
      </div>
      <div class="analog-action">
        <button class="btn btn-approve-sm ${isSelected ? 'btn-selected' : ''}"
          ${isBusy ? 'disabled' : ''}
          onclick="chooseCandidate('${rowId}', '${source}', '${esc(candidate.code_1c)}', this)">
          ${isSelected ? '✓ Выбрано' : isBusy ? 'Сохраняем...' : 'Выбрать'}
        </button>
      </div>
    `;
    container.appendChild(card);
  });
}

async function runManualSearch() {
  if (!currentJob || !modalRowId) return;
  const input = document.getElementById('manual-search-input');
  const query = input.value.trim();
  if (query.length < 2) {
    alert('Введите минимум 2 символа для поиска');
    return;
  }
  const list = document.getElementById('search-results-list');
  list.innerHTML = '<div class="empty-state">Ищем по остаткам...</div>';
  try {
    const res = await apiFetch(`/api/jobs/${currentJob.job_id}/search`, 'POST', {
      row_id: modalRowId,
      query,
      limit: 12,
    });
    searchResultsByRow[modalRowId] = res.results || [];
    renderCandidateList('search-results-list', searchResultsByRow[modalRowId], modalRowId, 'search');
  } catch (ex) {
    list.innerHTML = `<div class="empty-state">Ошибка поиска: ${esc(ex.message || 'неизвестная ошибка')}</div>`;
  }
}

async function chooseCandidate(rowId, source, code, button) {
  if (selectionInFlightRows.has(rowId)) return;
  selectionInFlightRows.add(rowId);
  renderCandidateList('analog-list', allRows.find(r => r.id === rowId)?.analogs || [], rowId, 'analog');
  renderCandidateList('search-results-list', searchResultsByRow[rowId] || [], rowId, 'search');
  if (source === 'analog') {
    try {
      await selectAnalog(rowId, code);
    } catch (ex) {
      alert('Ошибка: ' + ex.message);
      renderCandidateList('analog-list', allRows.find(r => r.id === rowId)?.analogs || [], rowId, 'analog');
      renderCandidateList('search-results-list', searchResultsByRow[rowId] || [], rowId, 'search');
    } finally {
      selectionInFlightRows.delete(rowId);
    }
    return;
  }
  const candidates = searchResultsByRow[rowId] || [];
  const candidate = candidates.find(item => item.code_1c === code);
  if (!candidate) {
    selectionInFlightRows.delete(rowId);
    renderCandidateList('analog-list', allRows.find(r => r.id === rowId)?.analogs || [], rowId, 'analog');
    renderCandidateList('search-results-list', searchResultsByRow[rowId] || [], rowId, 'search');
    alert('Кандидат поиска не найден');
    return;
  }
  try {
    const searchQuery = document.getElementById('manual-search-input')?.value?.trim() || '';
    const row = allRows.find(r => r.id === rowId);
    const visibleCandidates = mergeVisibleCandidates(row?.analogs || [], candidates);
    const res = await apiFetch(`/api/jobs/${currentJob.job_id}/select`, 'POST', {
      row_id: rowId,
      candidate,
      search_query: searchQuery,
      visible_candidates: visibleCandidates,
    });
    pendingApprovals[rowId] = code;
    if (row) {
      Object.assign(row, res.row || {});
      row.analogs = row.analogs || [];
      const approved = row.approved_analog || candidate;
      if (!row.analogs.some(a => a.code_1c === approved.code_1c)) {
        row.analogs.unshift(approved);
      }
    }
    if (currentJob && res.status_counts) {
      currentJob.status_counts = res.status_counts;
    }
    selectionInFlightRows.delete(rowId);
    closeModal();
    renderResults(currentJob);
  } catch (ex) {
    alert('Ошибка выбора: ' + ex.message);
    selectionInFlightRows.delete(rowId);
    renderCandidateList('analog-list', allRows.find(r => r.id === rowId)?.analogs || [], rowId, 'analog');
    renderCandidateList('search-results-list', searchResultsByRow[rowId] || [], rowId, 'search');
  }
}

/* ===== Export ===== */
async function exportFile() {
  if (!currentJob) return;
  try {
    const res = await fetch(`${API}/api/jobs/${currentJob.job_id}/export`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${token}` },
    });
    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.detail || 'Ошибка экспорта');
    }
    const blob = await res.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'КП_для_1С.xlsx';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);

    const updated = await apiFetch(`/api/jobs/${currentJob.job_id}`);
    currentJob = { ...updated, job_id: currentJob.job_id };
    allRows = updated.rows || [];
    renderResults(currentJob);
  } catch (ex) {
    alert('Ошибка экспорта: ' + ex.message);
  }
}

/* ===== History ===== */
async function loadHistory() {
  const list = document.getElementById('history-list');
  list.innerHTML = '<p style="color:var(--text-muted)">Загрузка...</p>';
  try {
    const data = await apiFetch('/api/jobs');
    const jobs = data.jobs || [];
    if (!jobs.length) {
      list.innerHTML = '<p style="color:var(--text-muted)">История пуста</p>';
      return;
    }
    list.innerHTML = '';
    jobs.forEach(job => {
      const counts = job.status_counts || {};
      const exactN = (counts['Найдено полностью'] || 0) + (counts['Найдено частично'] || 0);
      const analogN = (counts['Безопасный аналог'] || 0) + (counts['Нужна проверка аналога'] || 0)
                    + (counts['Одобрена замена'] || 0);
      const notfoundN = counts['Не найдено'] || 0;

      const date = job.created_at
        ? new Date(job.created_at * 1000).toLocaleString('ru-RU')
        : '';
      const savedAt = job.saved_at ? new Date(job.saved_at).toLocaleString('ru-RU') : '';
      const ownerLabel = job.created_by_display ? `<div class="history-owner">${esc(job.created_by_display)}</div>` : '';
      const savedLabel = savedAt ? `<div class="history-saved">Сохранён: ${savedAt}</div>` : '';
      const replacementsLabel = job.replacements_count
        ? `<span style="color:var(--approved)">замены ${job.replacements_count}</span>`
        : '';

      const item = document.createElement('div');
      item.className = 'history-item';
      item.innerHTML = `
        <div>
          <div class="history-name">${esc(job.filename || job.job_id)}</div>
          <div class="history-date">${date}</div>
          ${ownerLabel}
          ${savedLabel}
        </div>
        <div class="history-stats">
          <span style="color:var(--exact)">✓ ${exactN}</span>
          <span style="color:var(--approval)">~ ${analogN}</span>
          <span style="color:var(--notfound)">✗ ${notfoundN}</span>
          ${replacementsLabel}
          <span style="color:var(--text-muted)">${job.total_rows} позиций</span>
        </div>
      `;
      item.addEventListener('click', () => openHistoryJob(job.job_id));
      list.appendChild(item);
    });
  } catch (ex) {
    list.innerHTML = `<p style="color:var(--notfound)">${esc(ex.message)}</p>`;
  }
}

async function openHistoryJob(jobId) {
  document.getElementById('history-section').style.display = 'none';
  document.getElementById('progress-section').style.display = '';
  document.getElementById('progress-text').textContent = 'Загрузка результатов...';
  document.getElementById('progress-detail').textContent = '';

  try {
    const data = await apiFetch(`/api/jobs/${jobId}`);
    currentJob = { ...data, job_id: jobId };
    allRows = data.rows || [];
    pendingApprovals = {};
    renderResults(data);
  } catch (ex) {
    document.getElementById('progress-text').textContent = 'Ошибка';
    document.getElementById('progress-detail').textContent = ex.message;
  }
}

async function loadAnalytics() {
  const summary = document.getElementById('analytics-summary');
  const users = document.getElementById('analytics-users');
  const exports = document.getElementById('analytics-exports');
  const topReplacements = document.getElementById('analytics-top-replacements');

  summary.innerHTML = '<div class="empty-state">Загрузка аналитики...</div>';
  users.innerHTML = '';
  exports.innerHTML = '';
  topReplacements.innerHTML = '';

  try {
    analyticsData = await apiFetch('/api/admin/analytics');
    renderAnalytics(analyticsData);
  } catch (ex) {
    summary.innerHTML = `<div class="empty-state">${esc(ex.message || 'Ошибка загрузки аналитики')}</div>`;
  }
}

function analyticsSummaryCard(label, value, accent = '') {
  return `
    <div class="analytics-card ${accent}">
      <span class="analytics-card-value">${value}</span>
      <span class="analytics-card-label">${label}</span>
    </div>
  `;
}

function renderAnalytics(data) {
  const summary = data.summary || {};
  document.getElementById('analytics-summary').innerHTML = [
    analyticsSummaryCard('Сохранённых файлов', summary.saved_files || 0, 'is-primary'),
    analyticsSummaryCard('Пользователей с выгрузками', summary.users_with_exports || 0),
    analyticsSummaryCard('Всего замен', summary.replacement_count || 0),
    analyticsSummaryCard('Обучающих замен', summary.learned_replacement_count || 0),
  ].join('');

  const usersContainer = document.getElementById('analytics-users');
  const userItems = (data.users || []).map((user) => {
    const files = (user.files || []).slice(0, 5).map((file) => `
      <div class="analytics-subitem">
        <span>${esc(file.filename || file.job_id || '')}</span>
        <span>${file.saved_at ? new Date(file.saved_at).toLocaleString('ru-RU') : ''}</span>
      </div>
    `).join('');
    return `
      <div class="analytics-user-card">
        <div class="analytics-user-head">
          <div>
            <div class="analytics-user-name">${esc(user.display_name || user.username || '')}</div>
            <div class="analytics-user-login">${esc(user.username || '')}</div>
          </div>
          <span class="analytics-role">${user.role === 'admin' ? 'admin' : 'manager'}</span>
        </div>
        <div class="analytics-user-stats">
          <span>Файлов: <strong>${user.saved_files || 0}</strong></span>
          <span>Строк: <strong>${user.total_rows || 0}</strong></span>
          <span>Замен: <strong>${user.replacement_count || 0}</strong></span>
          <span>Обучили: <strong>${user.learned_replacement_count || 0}</strong></span>
        </div>
        ${files ? `<div class="analytics-sublist">${files}</div>` : '<div class="empty-state">Пока нет сохранённых файлов</div>'}
      </div>
    `;
  });
  usersContainer.innerHTML = userItems.length ? userItems.join('') : '<div class="empty-state">Нет данных</div>';

  const exportsContainer = document.getElementById('analytics-exports');
  const exportItems = (data.exports || []).map((entry) => {
    const replacements = (entry.replacements || []).slice(0, 8).map((replacement) => `
      <div class="analytics-subitem analytics-subitem-multiline">
        <div>
          <strong>${esc(replacement.name || '')}</strong>
          ${replacement.mark ? `<span class="analytics-inline-muted"> · ${esc(replacement.mark)}</span>` : ''}
        </div>
        <div class="analytics-inline-muted">
          ${esc(replacement.candidate_name || '')} (${esc(replacement.candidate_code || '')})
          ${replacement.learned_on_export ? ' · обучение' : ''}
        </div>
      </div>
    `).join('');
    return `
      <div class="analytics-export-card">
        <div class="analytics-export-head">
          <div>
            <div class="analytics-user-name">${esc(entry.filename || '')}</div>
            <div class="analytics-inline-muted">${esc(entry.saved_by_display || entry.saved_by || '')} · ${entry.saved_at ? new Date(entry.saved_at).toLocaleString('ru-RU') : ''}</div>
          </div>
          <div class="analytics-user-stats">
            <span>Строк: <strong>${entry.total_rows || 0}</strong></span>
            <span>Замен: <strong>${entry.replacement_count || 0}</strong></span>
            <span>Обучили: <strong>${entry.learned_replacement_count || 0}</strong></span>
          </div>
        </div>
        ${replacements ? `<div class="analytics-sublist">${replacements}</div>` : '<div class="empty-state">В этом сохранении не было замен</div>'}
      </div>
    `;
  });
  exportsContainer.innerHTML = exportItems.length ? exportItems.join('') : '<div class="empty-state">Нет сохранённых файлов</div>';

  const topContainer = document.getElementById('analytics-top-replacements');
  const topItems = (data.top_replacements || []).map((item) => `
    <div class="analytics-top-item">
      <div>
        <div class="analytics-user-name">${esc(item.candidate_name || '')}</div>
        <div class="analytics-inline-muted">${esc(item.candidate_code || '')}</div>
      </div>
      <div class="analytics-user-stats">
        <span>Использований: <strong>${item.times_used || 0}</strong></span>
        <span>Обучений: <strong>${item.times_learned || 0}</strong></span>
      </div>
    </div>
  `).join('');
  topContainer.innerHTML = topItems || '<div class="empty-state">Пока нет данных по заменам</div>';
}
