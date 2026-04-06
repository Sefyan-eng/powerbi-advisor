// ── Export Functions ──

async function generateReport() {
  const payload = collectState();
  const spinner = document.getElementById('miniSpinner');
  spinner.style.display = 'flex';
  setStatus('Generating...', true);
  try {
    const resp = await fetch(`${API}/generate-report`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload)
    });
    if (!resp.ok) { const e = await resp.json(); throw new Error(e.detail || 'Error'); }
    const blob = await resp.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `rapport_powerbi_${payload.filename.replace(/\.[^.]+$/, '')}.html`;
    document.body.appendChild(a); a.click(); document.body.removeChild(a); URL.revokeObjectURL(url);
    setStatus('Ready');
  } catch (e) {
    alert(e.message);
    setStatus('Error', false, true);
  } finally {
    spinner.style.display = 'none';
  }
}

async function downloadBim() {
  const payload = collectState();
  setStatus('Generating .bim...', true);
  try {
    const resp = await fetch(`${API}/generate-bim`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload)
    });
    if (!resp.ok) { const e = await resp.json(); throw new Error(e.detail || 'Error generating .bim'); }
    const blob = await resp.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${payload.filename.replace(/\.[^.]+$/, '')}.bim`;
    document.body.appendChild(a); a.click(); document.body.removeChild(a); URL.revokeObjectURL(url);
    setStatus('Ready');
  } catch (e) {
    alert(e.message);
    setStatus('Error', false, true);
  }
}

async function downloadTeScript() {
  const payload = collectState();
  setStatus('Generating script...', true);
  try {
    const resp = await fetch(`${API}/generate-te-script`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload)
    });
    if (!resp.ok) { const e = await resp.json(); throw new Error(e.detail || 'Error generating script'); }
    const blob = await resp.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${payload.filename.replace(/\.[^.]+$/, '')}_setup.csx`;
    document.body.appendChild(a); a.click(); document.body.removeChild(a); URL.revokeObjectURL(url);
    setStatus('Ready');
  } catch (e) {
    alert(e.message);
    setStatus('Error', false, true);
  }
}

async function downloadPbip() {
  const filePath = document.getElementById('pbipFilePath').value.trim();
  if (!filePath || !filePath.includes('\\')) {
    alert('Please enter the full Windows path to your data file.\nExample: C:\\Users\\YourName\\Documents\\data.xlsx');
    document.getElementById('pbipFilePath').focus();
    return;
  }
  const payload = {
    model: collectState(),
    session_id: currentData?.session_id || null,
    file_path: filePath
  };
  setStatus('Generating Power BI project...', true);
  try {
    const resp = await fetch(`${API}/generate-pbip`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload)
    });
    if (!resp.ok) { const e = await resp.json(); throw new Error(e.detail || 'Error generating project'); }
    const blob = await resp.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${payload.model.filename.replace(/\.[^.]+$/, '')}_PowerBI.zip`;
    document.body.appendChild(a); a.click(); document.body.removeChild(a); URL.revokeObjectURL(url);
    setStatus('Ready');
  } catch (e) {
    alert(e.message);
    setStatus('Error', false, true);
  }
}

function openPushModal() {
  document.getElementById('pushModal').classList.add('open');
  document.getElementById('pushResult').className = 'push-result';
  document.getElementById('pushResult').style.display = 'none';
  document.getElementById('btnPush').disabled = false;
}

function closePushModal() {
  document.getElementById('pushModal').classList.remove('open');
}

// Close modal on overlay click
document.getElementById('pushModal').addEventListener('click', function(e) {
  if (e.target === this) closePushModal();
});

async function pushToWorkspace() {
  const tenantId = document.getElementById('pbiTenantId').value.trim();
  const clientId = document.getElementById('pbiClientId').value.trim();
  const clientSecret = document.getElementById('pbiClientSecret').value.trim();
  const workspaceId = document.getElementById('pbiWorkspaceId').value.trim();
  const pushData = document.getElementById('pbiPushData').checked;

  if (!tenantId || !clientId || !clientSecret || !workspaceId) {
    showPushResult('error', 'All credential fields are required.');
    return;
  }

  const btn = document.getElementById('btnPush');
  btn.disabled = true;
  btn.innerHTML = '<div class="mini-spin" style="width:14px;height:14px;border:2px solid var(--border);border-top-color:#fff;border-radius:50%;animation:spin .6s linear infinite;"></div> Pushing...';
  setStatus('Pushing to Power BI...', true);

  const payload = {
    config: {
      tenant_id: tenantId,
      client_id: clientId,
      client_secret: clientSecret,
      workspace_id: workspaceId
    },
    model: collectState(),
    session_id: currentData?.session_id || null,
    push_data: pushData
  };

  try {
    const resp = await fetch(`${API}/push-to-powerbi`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload)
    });
    if (!resp.ok) {
      const e = await resp.json();
      throw new Error(e.detail || 'Push failed');
    }
    const result = await resp.json();
    let msg = `Dataset "${esc(result.dataset_name)}" created successfully!<br/><a href="${esc(result.url)}" target="_blank" rel="noopener">Open in Power BI</a>`;
    if (result.rows_pushed && Object.keys(result.rows_pushed).length > 0) {
      const rowsSummary = Object.entries(result.rows_pushed).map(([t, n]) => `${t}: ${n} rows`).join(', ');
      msg += `<br/>Data pushed: ${rowsSummary}`;
    }
    showPushResult('success', msg);
    setStatus('Ready');
  } catch (e) {
    showPushResult('error', e.message);
    setStatus('Error', false, true);
  } finally {
    btn.disabled = false;
    btn.innerHTML = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 10h-1.26A8 8 0 109 20h9a5 5 0 000-10z"/></svg> Push Dataset';
  }
}

function showPushResult(type, msg) {
  const el = document.getElementById('pushResult');
  el.className = 'push-result ' + type;
  el.innerHTML = msg;
  el.style.display = 'block';
}

async function deployToDesktop() {
  const mcpPath = document.getElementById('mcpExePath').value.trim();
  if (!mcpPath) {
    alert('Please enter the path to the Power BI Modeling MCP server executable.');
    document.getElementById('mcpExePath').focus();
    return;
  }
  const resultEl = document.getElementById('deployResult');
  resultEl.className = 'push-result';
  resultEl.style.display = 'none';
  setStatus('Deploying to PBI Desktop...', true);

  const payload = {
    model: collectState(),
    mcp_exe_path: mcpPath,
  };

  try {
    const resp = await fetch(`${API}/deploy-to-desktop`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload)
    });
    const result = await resp.json();
    if (!resp.ok) throw new Error(result.detail || 'Deploy failed');

    let msg = '';
    if (result.relationships?.length > 0) {
      msg += `<strong>${result.relationships.length} relationships</strong> created<br/>`;
    }
    if (result.tables_created?.length > 0) {
      msg += `<strong>${result.tables_created.length} tables</strong> created from source data<br/>`;
      result.tables_created.forEach(t => { msg += `<span style="font-size:12px;color:var(--text3);">&nbsp;&nbsp;${esc(t.name)} (${esc(t.type)}) — ${t.columns?.length || 0} columns</span><br/>`; });
    }
    if (result.measures?.length > 0) {
      msg += `<strong>${result.measures.length} DAX measures</strong> created<br/>`;
    }
    if (result.pbi_tables?.length > 0) {
      msg += `<br/><span style="font-size:11px;color:var(--text3);">Tables in PBI Desktop: ${result.pbi_tables.map(t => `<strong>${esc(t)}</strong>`).join(', ')}</span><br/>`;
    }
    if (result.verified_measures?.length > 0) {
      msg += `<span style="font-size:11px;color:var(--success);">Verified: ${result.verified_measures.length} measures in model</span><br/>`;
    }
    if (result.verified_relationships?.length > 0) {
      msg += `<span style="font-size:11px;color:var(--success);">Verified: ${result.verified_relationships.length} relationships in model</span><br/>`;
    }
    if (result.errors?.length > 0) {
      msg += `<br/><strong>${result.errors.length} error(s):</strong><br/>`;
      result.errors.forEach(e => { msg += `&bull; ${esc(e)}<br/>`; });
    }
    if (result.success) {
      resultEl.className = 'push-result success';
      resultEl.innerHTML = 'Deployed to Power BI Desktop!<br/>' + msg;
    } else {
      resultEl.className = 'push-result error';
      resultEl.innerHTML = 'Deploy failed.<br/>' + msg;
    }
    resultEl.style.display = 'block';
    setStatus(result.success ? 'Ready' : 'Error', false, !result.success);
  } catch (e) {
    resultEl.className = 'push-result error';
    resultEl.innerHTML = esc(e.message);
    resultEl.style.display = 'block';
    setStatus('Error', false, true);
  }
}
