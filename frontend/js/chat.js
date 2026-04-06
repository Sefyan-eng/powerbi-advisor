// ── AI Assistant / Chat ──

function addChatMessage(role, content) {
  const container = document.getElementById('chatMessages');
  // Remove hint on first message
  const hint = container.querySelector('.chat-hint');
  if (hint) hint.remove();

  const bubble = document.createElement('div');
  bubble.style.cssText = role === 'user'
    ? 'align-self:flex-end;background:var(--primary);color:#fff;padding:10px 16px;border-radius:var(--radius) var(--radius) 4px var(--radius);max-width:80%;font-size:14px;line-height:1.5;'
    : 'align-self:flex-start;background:var(--surface);border:1px solid var(--border);color:var(--text);padding:12px 16px;border-radius:var(--radius) var(--radius) var(--radius) 4px;max-width:90%;font-size:14px;line-height:1.6;';
  bubble.innerHTML = content;
  container.appendChild(bubble);
  container.scrollTop = container.scrollHeight;
}

function renderActions(actions) {
  if (!actions || actions.length === 0) return '';
  let html = '<div style="margin-top:10px;display:flex;flex-direction:column;gap:6px;">';
  for (const a of actions) {
    const icon = a.status === 'done' ? '&#10003;' : '&#10007;';
    const color = a.status === 'done' ? 'var(--success)' : 'var(--danger)';
    let label = '';
    if (a.type === 'create_measure') label = `<strong>+</strong> Measure <strong>${esc(a.name || '')}</strong> on ${esc(a.table || '')}`;
    else if (a.type === 'delete_measure') label = `<strong>&minus;</strong> Deleted measure <strong>${esc(a.name || '')}</strong>`;
    else if (a.type === 'update_measure') label = `<strong>&#8635;</strong> Updated measure <strong>${esc(a.name || '')}</strong>`;
    else if (a.type === 'create_relationship') label = `<strong>+</strong> Relationship ${esc(a.rel || '')}`;
    else if (a.type === 'delete_relationship') label = `<strong>&minus;</strong> Deleted relationship <strong>${esc(a.name || '')}</strong>`;
    else if (a.type === 'create_table') label = `<strong>+</strong> Table <strong>${esc(a.name || '')}</strong>`;
    else if (a.type === 'delete_table') label = `<strong>&minus;</strong> Deleted table <strong>${esc(a.name || '')}</strong>`;
    else if (a.type === 'execute_dax') label = `DAX Query` + (a.result ? `<br/><code style="font-size:11px;color:var(--text3);white-space:pre-wrap;">${esc(a.result).substring(0,500)}</code>` : '');
    else if (a.type === 'info') label = esc(a.message || '');
    else label = `${esc(a.type)}: ${a.error ? esc(a.error) : 'done'}`;

    html += `<div style="display:flex;align-items:flex-start;gap:8px;padding:6px 10px;background:rgba(0,0,0,.15);border-radius:6px;">
      <span style="color:${color};font-weight:700;font-size:14px;line-height:1;">${icon}</span>
      <span style="font-size:13px;font-family:var(--mono);">${label}</span>
    </div>`;
  }
  html += '</div>';
  return html;
}

async function sendPrompt() {
  const input = document.getElementById('promptInput');
  const prompt = input.value.trim();
  if (!prompt) return;

  const mcpPath = document.getElementById('mcpExePath').value.trim();
  if (!mcpPath) {
    alert('Set the MCP Server Path in the export section first.');
    return;
  }

  // Show user message
  addChatMessage('user', esc(prompt));
  input.value = '';
  input.style.height = 'auto';

  // Show typing indicator
  const typing = document.createElement('div');
  typing.id = 'typingIndicator';
  typing.style.cssText = 'align-self:flex-start;color:var(--text3);font-size:13px;padding:8px 16px;font-style:italic;';
  typing.innerHTML = 'Claude is thinking & executing...';
  document.getElementById('chatMessages').appendChild(typing);

  const btn = document.getElementById('promptSendBtn');
  btn.disabled = true;
  setStatus('Processing prompt...', true);

  try {
    const resp = await fetch(`${API}/prompt-model`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        prompt,
        mcp_exe_path: mcpPath,
        conversation: chatConversation,
      }),
    });
    const result = await resp.json();
    if (!resp.ok) throw new Error(result.detail || 'Request failed');

    // Remove typing indicator
    const ti = document.getElementById('typingIndicator');
    if (ti) ti.remove();

    // Build assistant reply
    let reply = esc(result.reply || 'Done.');
    reply += renderActions(result.actions);
    if (result.errors && result.errors.length > 0) {
      reply += `<div style="margin-top:8px;color:var(--danger);font-size:12px;font-family:var(--mono);">Errors: ${result.errors.map(e => esc(e)).join('<br/>')}</div>`;
    }
    addChatMessage('assistant', reply);

    // Update conversation history for multi-turn
    chatConversation.push({ role: 'user', content: prompt });
    chatConversation.push({ role: 'assistant', content: result.reply || '' });

    setStatus('Ready');
  } catch (e) {
    const ti = document.getElementById('typingIndicator');
    if (ti) ti.remove();
    addChatMessage('assistant', `<span style="color:var(--danger);">Error: ${esc(e.message)}</span>`);
    setStatus('Error', false, true);
  } finally {
    btn.disabled = false;
  }
}
