// ── Dashboard Tab: KPI Cards, Charts, Python Scripts ──

let dashboardCharts = [];

async function loadDashboard() {
  const btn = document.getElementById('dashboardLoadBtn');
  btn.disabled = true;
  btn.textContent = 'Loading...';
  setStatus('Loading dashboard...', true);

  const mcpPath = document.getElementById('mcpExePath')?.value?.trim() || '';

  try {
    const resp = await fetch(`${API}/dashboard-data`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: collectState(),
        session_id: currentData?.session_id || null,
        mcp_exe_path: mcpPath
      })
    });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.detail || 'Failed to load dashboard data');

    renderKPIs(data.kpis || []);
    renderCharts(data.charts || []);
    renderPythonScripts(data.python_scripts || data.scripts || []);
    setStatus('Ready');
  } catch (e) {
    const container = document.getElementById('dashboardContent');
    container.innerHTML = `<div style="padding:20px;color:var(--danger);background:rgba(239,68,68,.08);border:1px solid rgba(239,68,68,.25);border-radius:var(--radius);font-family:var(--mono);font-size:13px;">${esc(e.message)}</div>`;
    setStatus('Error', false, true);
  } finally {
    btn.disabled = false;
    btn.textContent = 'Load Dashboard';
  }
}

function renderKPIs(kpis) {
  const grid = document.getElementById('kpiGrid');
  if (!kpis.length) {
    grid.innerHTML = '<div style="color:var(--text3);font-size:13px;font-family:var(--mono);">No KPI data available</div>';
    return;
  }

  const colors = ['#3b82f6', '#f59e0b', '#22c55e', '#a855f7', '#ef4444', '#06b6d4', '#ec4899', '#14b8a6'];
  grid.innerHTML = kpis.map((kpi, i) => {
    const color = colors[i % colors.length];
    const value = typeof kpi.value === 'number'
      ? kpi.value.toLocaleString(undefined, { maximumFractionDigits: 2 })
      : (kpi.value || '--');
    return `<div class="kpi-card" style="border-top:2px solid ${color};">
      <div class="kpi-label">${esc(kpi.name || kpi.label || 'Measure')}</div>
      <div class="kpi-value" style="color:${color};">${value}</div>
    </div>`;
  }).join('');
}

function renderCharts(charts) {
  const grid = document.getElementById('chartGrid');

  // Destroy previous chart instances
  dashboardCharts.forEach(c => c.destroy());
  dashboardCharts = [];

  if (!charts.length) {
    grid.innerHTML = '<div style="color:var(--text3);font-size:13px;font-family:var(--mono);">No chart data available</div>';
    return;
  }

  grid.innerHTML = charts.map((ch, i) => `
    <div class="chart-card">
      <h4>${esc(ch.title || 'Chart')}</h4>
      <canvas id="dashChart${i}"></canvas>
    </div>
  `).join('');

  charts.forEach((ch, i) => {
    const ctx = document.getElementById(`dashChart${i}`);
    if (!ctx) return;

    const labels = ch.labels || [];
    const values = ch.values || ch.data || [];

    const chart = new Chart(ctx, {
      type: ch.type || 'bar',
      data: {
        labels: labels,
        datasets: [{
          label: ch.title || 'Value',
          data: values,
          backgroundColor: ch.colors || 'rgba(59, 130, 246, 0.7)',
          borderColor: ch.borderColors || 'rgba(59, 130, 246, 1)',
          borderWidth: 1,
          borderRadius: 4,
        }]
      },
      options: {
        indexAxis: 'y',
        responsive: true,
        maintainAspectRatio: true,
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: '#111827',
            titleColor: '#e2e8f0',
            bodyColor: '#94a3b8',
            borderColor: '#1e2d45',
            borderWidth: 1,
          }
        },
        scales: {
          x: {
            grid: { color: 'rgba(30,45,69,0.5)' },
            ticks: { color: '#94a3b8', font: { family: "'IBM Plex Mono', monospace", size: 11 } },
          },
          y: {
            grid: { display: false },
            ticks: { color: '#e2e8f0', font: { family: "'IBM Plex Mono', monospace", size: 11 } },
          }
        }
      }
    });
    dashboardCharts.push(chart);
  });
}

function renderPythonScripts(scripts) {
  const container = document.getElementById('scriptsAccordion');
  if (!scripts.length) {
    container.innerHTML = '<div style="color:var(--text3);font-size:13px;font-family:var(--mono);">No Python scripts available</div>';
    return;
  }

  container.innerHTML = scripts.map((s, i) => `
    <div class="script-card">
      <div class="script-card-header" onclick="toggleScript(${i})">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="var(--primary-soft)" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg>
        <h4>${esc(s.title || s.name || 'Script')}</h4>
        <span class="script-toggle" id="scriptToggle${i}">&#9660;</span>
      </div>
      <div class="script-card-body" id="scriptBody${i}">
        <button class="script-copy-btn" onclick="copyScript(${i})">Copy</button>
        <pre id="scriptCode${i}">${esc(s.script || s.code || s.content || '')}</pre>
      </div>
    </div>
  `).join('');
}

function toggleScript(index) {
  const body = document.getElementById(`scriptBody${index}`);
  const toggle = document.getElementById(`scriptToggle${index}`);
  if (body.classList.contains('open')) {
    body.classList.remove('open');
    toggle.classList.remove('open');
  } else {
    body.classList.add('open');
    toggle.classList.add('open');
  }
}

function copyScript(index) {
  const code = document.getElementById(`scriptCode${index}`);
  if (!code) return;
  const text = code.textContent;
  navigator.clipboard.writeText(text).then(() => {
    const btn = code.parentElement.querySelector('.script-copy-btn');
    btn.textContent = 'Copied!';
    setTimeout(() => { btn.textContent = 'Copy'; }, 2000);
  }).catch(() => {
    // Fallback for older browsers
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.style.position = 'fixed';
    ta.style.left = '-9999px';
    document.body.appendChild(ta);
    ta.select();
    document.execCommand('copy');
    document.body.removeChild(ta);
    const btn = code.parentElement.querySelector('.script-copy-btn');
    btn.textContent = 'Copied!';
    setTimeout(() => { btn.textContent = 'Copy'; }, 2000);
  });
}
