// ── Upload Flow ──

(function() {
  const fileInput = document.getElementById('fileInput');
  const dropzone = document.getElementById('dropzone');

  fileInput.addEventListener('change', e => { if (e.target.files[0]) selectFile(e.target.files[0]); });
  dropzone.addEventListener('dragover', e => { e.preventDefault(); dropzone.classList.add('drag'); });
  dropzone.addEventListener('dragleave', () => dropzone.classList.remove('drag'));
  dropzone.addEventListener('drop', e => { e.preventDefault(); dropzone.classList.remove('drag'); if (e.dataTransfer.files[0]) selectFile(e.dataTransfer.files[0]); });
})();

function selectFile(f) {
  selectedFile = f;
  document.getElementById('fileName').textContent = f.name;
  document.getElementById('fileChip').style.display = 'inline-flex';
  document.getElementById('analyzeBtn').disabled = false;
  document.getElementById('errorBox').style.display = 'none';
}

async function runAnalysis() {
  if (!selectedFile) return;
  document.getElementById('upload-section').style.display = 'none';
  document.getElementById('loading').style.display = 'block';
  setStatus('Analyzing...', true);

  const steps = ['step1','step2','step3','step4'];
  let si = 0;
  steps.forEach(s => { document.getElementById(s).style.display = 'none'; document.getElementById(s).classList.remove('active'); });
  document.getElementById(steps[0]).classList.add('active');
  document.getElementById(steps[0]).style.display = 'flex';

  const iv = setInterval(() => {
    document.getElementById(steps[si]).classList.remove('active');
    si = Math.min(si + 1, steps.length - 1);
    document.getElementById(steps[si]).classList.add('active');
    document.getElementById(steps[si]).style.display = 'flex';
  }, 2200);

  try {
    const form = new FormData();
    form.append('file', selectedFile);
    const resp = await fetch(`${API}/analyze`, { method: 'POST', body: form });
    clearInterval(iv);
    if (!resp.ok) { const e = await resp.json(); throw new Error(e.detail || 'Server error'); }
    currentData = await resp.json();
    renderResults(currentData);
    document.getElementById('loading').style.display = 'none';
    document.getElementById('results').style.display = 'block';
    setStatus('Ready');
  } catch (e) {
    clearInterval(iv);
    document.getElementById('loading').style.display = 'none';
    document.getElementById('upload-section').style.display = 'block';
    const eb = document.getElementById('errorBox');
    eb.style.display = 'block';
    eb.textContent = e.message;
    setStatus('Error', false, true);
  }
}

function renderResults(d) {
  document.getElementById('modelType').value = d.model_type || '';
  document.getElementById('summaryText').value = d.summary || '';
  document.getElementById('tablesGrid').innerHTML = '';
  document.getElementById('relList').innerHTML = '';
  document.getElementById('measuresList').innerHTML = '';
  (d.tables || []).forEach((t, i) => appendTableCard(t, i));
  (d.relationships || []).forEach((r, i) => appendRelRow(r, i));
  (d.measures_suggested || []).forEach((m, i) => appendMeasureCard(m, i));
  updateCounts();

  document.getElementById('warnList').innerHTML = (d.warnings || []).map(w =>
    `<div class="warn-item"><div class="w-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg></div><span>${w}</span></div>`
  ).join('');

  document.getElementById('tipsList').innerHTML = (d.best_practices || []).map(b =>
    `<div class="tip-item"><div class="t-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg></div><span>${b}</span></div>`
  ).join('');
}
