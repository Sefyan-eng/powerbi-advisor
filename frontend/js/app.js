// ── Core App State & Utilities ──

const API = '';
let currentData = null, selectedFile = null;
let chatConversation = [];

function esc(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

function setStatus(text, busy, err) {
  const el = document.getElementById('navStatus');
  const dot = el.querySelector('.dot');
  document.getElementById('statusText').textContent = text;
  el.classList.toggle('offline', !!err);
  if (busy) {
    dot.style.background = 'var(--accent)';
    dot.style.boxShadow = '0 0 8px rgba(245,158,11,.5)';
  } else if (err) {
    dot.style.background = 'var(--danger)';
    dot.style.boxShadow = '0 0 8px rgba(239,68,68,.5)';
  } else {
    dot.style.background = 'var(--success)';
    dot.style.boxShadow = '0 0 8px rgba(34,197,94,.5)';
  }
}

function collectState() {
  const tables = [];
  document.querySelectorAll('.table-card').forEach(card => {
    const name = card.querySelector('.tc-name-input')?.value || '';
    const type = card.querySelector('.type-select')?.value || 'Dimension';
    const sourceEl = card.querySelector('.tc-source');
    const source = sourceEl ? sourceEl.textContent.trim() : '';
    const desc = card.querySelector('.tc-desc')?.value || '';
    const cols = []; let pk = null;
    card.querySelectorAll('.col-row').forEach(row => {
      const v = row.querySelector('.col-input')?.value || '';
      if (v) { cols.push(v); if (row.querySelector('.pk-btn.active')) pk = v; }
    });
    if (name) tables.push({ name, type, source_sheet: source, columns: cols, primary_key: pk, description: desc });
  });

  const relationships = [];
  document.querySelectorAll('.rel-item').forEach(row => {
    const inputs = row.querySelectorAll('input');
    const selects = row.querySelectorAll('select');
    if (inputs.length >= 4) relationships.push({
      from_table: inputs[0].value, from_column: inputs[1].value,
      to_table: inputs[2].value, to_column: inputs[3].value,
      cardinality: selects[0]?.value || 'Many-to-One', cross_filter: selects[1]?.value || 'Single'
    });
  });

  const measures_suggested = [];
  document.querySelectorAll('.measure-card').forEach(card => {
    const name = card.querySelector('.msr-name')?.value || '';
    const desc = card.querySelector('.msr-desc')?.value || '';
    const dax = card.querySelector('.dax-editor')?.value || '';
    if (name) measures_suggested.push({ name, dax, description: desc });
  });

  const warnings = Array.from(document.querySelectorAll('.warn-item')).map(el => {
    const span = el.querySelector('span:last-child');
    return span ? span.textContent.trim() : el.textContent.trim();
  });
  const best_practices = Array.from(document.querySelectorAll('.tip-item')).map(el => {
    const span = el.querySelector('span:last-child');
    return span ? span.textContent.trim() : el.textContent.trim();
  });

  return {
    filename: currentData?.filename || 'model.xlsx',
    model_type: document.getElementById('modelType').value,
    summary: document.getElementById('summaryText').value,
    tables, relationships, measures_suggested, warnings, best_practices
  };
}

function updateCounts() {
  document.getElementById('tablesCount').textContent = document.querySelectorAll('.table-card').length;
  document.getElementById('relsCount').textContent = document.querySelectorAll('.rel-item').length;
  document.getElementById('measuresCount').textContent = document.querySelectorAll('.measure-card').length;
}

function reset() {
  currentData = null; selectedFile = null;
  document.getElementById('fileInput').value = '';
  document.getElementById('fileChip').style.display = 'none';
  document.getElementById('analyzeBtn').disabled = true;
  document.getElementById('errorBox').style.display = 'none';
  document.getElementById('results').style.display = 'none';
  document.getElementById('upload-section').style.display = 'block';
  setStatus('Ready');
}

// ── Nav status check on load ──
document.addEventListener('DOMContentLoaded', () => {
  // Enter to send in chat (Shift+Enter for newline)
  const input = document.getElementById('promptInput');
  if (input) {
    input.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        sendPrompt();
      }
    });
  }
});
