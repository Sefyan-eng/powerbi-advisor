// ── Inline Editing: Tables, Relationships, Measures, Warnings, Tips ──

function typeClass(type) {
  if (type === 'Fact') return 't-fact';
  if (type === 'Dimension') return 't-dim';
  if (type === 'Bridge') return 't-bridge';
  return '';
}

function updateCardType(select) {
  const card = select.closest('.table-card');
  const val = select.value;
  card.setAttribute('data-type', val);
  select.className = 'type-select ' + typeClass(val);
}

function appendTableCard(t, ti) {
  const id = `tbl-${ti}-${Date.now()}`;
  const typeOpts = ['Fact','Dimension','Bridge'].map(v => `<option ${v === t.type ? 'selected' : ''}>${v}</option>`).join('');
  const colsHtml = (t.columns || []).map((c, ci) => colRowHtml(c, c === t.primary_key)).join('');

  const div = document.createElement('div');
  div.className = 'table-card';
  div.id = id;
  div.setAttribute('data-type', t.type || 'Dimension');
  div.innerHTML = `
    <div class="tc-header">
      <select class="type-select ${typeClass(t.type)}" onchange="updateCardType(this)">${typeOpts}</select>
      <input class="tc-name-input" value="${esc(t.name)}" placeholder="Table name"/>
      <button class="tc-del" onclick="document.getElementById('${id}').remove();updateCounts()">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"/></svg>
      </button>
    </div>
    <div class="tc-body">
      <div class="tc-source"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/></svg> ${esc(t.source_sheet || '')}</div>
      <div class="cols-container" id="cols-${id}">${colsHtml}</div>
      <button class="btn-add-col" onclick="addColumn('${id}')">+ Column</button>
      <textarea class="tc-desc" placeholder="Description...">${esc(t.description || '')}</textarea>
    </div>`;
  document.getElementById('tablesGrid').appendChild(div);
  updateCounts();
}

function colRowHtml(name, isPk) {
  return `<div class="col-row">
    <input class="col-input" value="${esc(name)}" placeholder="column_name"/>
    <button class="pk-btn ${isPk ? 'active' : ''}" onclick="togglePk(this)" title="Primary Key">PK</button>
    <button class="col-del" onclick="this.closest('.col-row').remove()">&times;</button>
  </div>`;
}

function togglePk(btn) {
  btn.closest('.cols-container').querySelectorAll('.pk-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
}

function addColumn(tid) {
  const tmp = document.createElement('div');
  tmp.innerHTML = colRowHtml('', false);
  document.getElementById(`cols-${tid}`).appendChild(tmp.firstElementChild);
}

function addTable() {
  appendTableCard({ name: 'NewTable', type: 'Dimension', source_sheet: '', columns: ['id'], primary_key: 'id', description: '' }, Date.now());
}

function appendRelRow(r, ri) {
  const id = `rel-${ri}-${Date.now()}`;
  const cardOpts = ['Many-to-One','One-to-One','Many-to-Many','One-to-Many'].map(v => `<option ${v === r.cardinality ? 'selected' : ''}>${v}</option>`).join('');
  const cfOpts = ['Single','Both'].map(v => `<option ${v === r.cross_filter ? 'selected' : ''}>${v}</option>`).join('');

  const div = document.createElement('div');
  div.className = 'rel-item';
  div.id = id;
  div.innerHTML = `
    <input class="rel-input" value="${esc(r.from_table)}" placeholder="Source table"/>
    <span class="rel-dot">[</span>
    <input class="rel-input" value="${esc(r.from_column)}" placeholder="column" style="min-width:70px;"/>
    <span class="rel-dot">]</span>
    <span class="rel-arrow"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/></svg></span>
    <input class="rel-input" value="${esc(r.to_table)}" placeholder="Target table"/>
    <span class="rel-dot">[</span>
    <input class="rel-input" value="${esc(r.to_column)}" placeholder="column" style="min-width:70px;"/>
    <span class="rel-dot">]</span>
    <select class="rel-select">${cardOpts}</select>
    <select class="rel-select">${cfOpts}</select>
    <button class="tc-del" onclick="document.getElementById('${id}').remove();updateCounts()">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"/></svg>
    </button>`;
  document.getElementById('relList').appendChild(div);
  updateCounts();
}

function addRelation() {
  appendRelRow({ from_table: '', from_column: '', to_table: '', to_column: '', cardinality: 'Many-to-One', cross_filter: 'Single' }, Date.now());
}

function appendMeasureCard(m, mi) {
  const id = `msr-${mi}-${Date.now()}`;
  const div = document.createElement('div');
  div.className = 'measure-card';
  div.id = id;
  div.innerHTML = `
    <div class="msr-header">
      <div class="msr-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 15s1-1 4-1 5 2 8 2 4-1 4-1V3s-1 1-4 1-5-2-8-2-4 1-4 1z"/><line x1="4" y1="22" x2="4" y2="15"/></svg></div>
      <div class="msr-fields">
        <input class="msr-name" value="${esc(m.name)}" placeholder="Measure name"/>
        <input class="msr-desc" value="${esc(m.description || '')}" placeholder="Description..."/>
      </div>
      <button class="tc-del" onclick="document.getElementById('${id}').remove();updateCounts()">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"/></svg>
      </button>
    </div>
    <textarea class="dax-editor" placeholder="DAX formula...">${esc(m.dax || '')}</textarea>`;
  document.getElementById('measuresList').appendChild(div);
  updateCounts();
}

function addMeasure() {
  appendMeasureCard({ name: 'NewMeasure', dax: '', description: '' }, Date.now());
}

function addWarning(text) {
  const html = `<div class="warn-item"><div class="w-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg></div><span>${esc(text)}</span></div>`;
  document.getElementById('warnList').insertAdjacentHTML('beforeend', html);
}

function addTip(text) {
  const html = `<div class="tip-item"><div class="t-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg></div><span>${esc(text)}</span></div>`;
  document.getElementById('tipsList').insertAdjacentHTML('beforeend', html);
}
