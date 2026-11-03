const state = {
  websocket: null,
  polling: null,
  actionNames: {
    'start-safe-mode': 'Starting safe watching',
    'public-check': 'Checking Binance',
    'connect-market': 'Connecting to live market',
    'stop-market': 'Stopping market watch',
    'account-check': 'Checking account safely',
    'run-research-tests': 'Checking robot homework',
    'kill-switch': 'Stopping everything',
    'start-engine': 'Starting the robot',
    'stop-engine': 'Pausing the robot',
    'start-account-events': 'Starting account updates',
  },
};

const $ = (id) => document.getElementById(id);
const toast = (message, duration = 3400) => {
  const element = $('toast');
  element.textContent = message;
  element.classList.add('show');
  window.clearTimeout(toast.timer);
  toast.timer = window.setTimeout(() => element.classList.remove('show'), duration);
};

const money = (value) => {
  if (value === null || value === undefined || value === '') return '—';
  const number = Number(value);
  return Number.isFinite(number) ? number.toLocaleString(undefined, { maximumFractionDigits: 2 }) : '—';
};

const signedMoney = (value) => {
  if (value === null || value === undefined || value === '') return 'Not checked';
  const number = Number(value);
  if (!Number.isFinite(number)) return 'Not checked';
  return `${number >= 0 ? '+' : ''}${number.toLocaleString(undefined, { maximumFractionDigits: 2 })} USDT`;
};

function setConnection(connected, label = connected ? 'Watching live' : 'Not watching') {
  $('connection-pill').classList.toggle('live', connected);
  $('connection-label').textContent = label;
}

function renderSafety(safety = {}) {
  const locked = !safety.live_order_path_ready;
  $('safety-badge').textContent = locked ? 'LOCKED' : 'READY';
  $('safety-badge').className = `status-badge ${locked ? 'danger' : 'good'}`;
  $('safety-title').textContent = locked ? 'Money is protected' : 'Safety doors are open';
  $('safety-subtitle').textContent = locked ? 'Real orders are locked while the robot learns.' : 'Every required safety check has passed.';
  const items = [
    [!safety.live_orders_enabled, safety.live_orders_enabled ? 'Live orders are switched on' : 'Live orders are off'],
    [safety.kill_switch_active, safety.kill_switch_active ? 'Emergency stop is active' : 'Human stop button is ready'],
    [safety.gate_decision === 'ALLOW_LIVE_TRADING', safety.gate_decision === 'ALLOW_LIVE_TRADING' ? 'Research gate says yes' : 'Research gate is not open'],
  ];
  $('safety-list').innerHTML = items.map(([positive, label]) => `<li><span class="check-dot" style="border-color:${positive ? '#86c99b' : '#efb0a5'}"></span><span>${label}</span></li>`).join('');
}

function renderMarket(market, publicData) {
  const book = market?.order_book || {};
  const hasBook = book.best_bid && book.best_ask;
  const price = market?.last_trade?.price || market?.last_kline?.c || publicData?.book_ticker?.askPrice;
  $('price-value').textContent = price ? `${money(price)} USDT` : '—';
  $('price-subtitle').textContent = hasBook ? `The robot is watching ${market.symbol}.` : 'Press Start watching to see BTC/USDT.';
  $('best-bid').textContent = book.best_bid ? money(book.best_bid) : '—';
  $('best-ask').textContent = book.best_ask ? money(book.best_ask) : '—';
  $('spread-value').textContent = book.spread ? money(book.spread) : '—';
  $('market-badge').textContent = hasBook && !market.stale ? 'LIVE' : 'WAITING';
  $('market-badge').className = `status-badge ${hasBook && !market.stale ? 'good' : 'neutral'}`;
  if (price) {
    $('profit-empty').classList.add('hidden');
    $('profit-details').classList.remove('hidden');
    $('live-price').textContent = money(price);
    $('robot-mode').textContent = market.stale ? 'Paused' : 'Watching only';
    $('last-update').textContent = market.freshness_ms == null ? '—' : `${Math.round(market.freshness_ms)} ms ago`;
  }
}

function renderResearch(research = {}) {
  const tests = research.baseline;
  const gate = research.deployment_gate;
  const warehouse = research.warehouse;
  $('research-tests').textContent = tests?.report_count ? `${tests.report_count}` : '—';
  $('research-gate').textContent = gate?.decision === 'ABSTAIN_NO_LIVE_TRADING' ? 'LOCKED' : (gate?.decision || '—');
  $('research-data').textContent = warehouse?.present ? 'READY' : '—';
}

function render(snapshot) {
  if (!snapshot) return;
  setConnection(snapshot.system?.mode === 'live-observation');
  renderSafety(snapshot.safety);
  if (snapshot.market) renderMarket(snapshot.market, snapshot.public);
  renderAccount(snapshot.account_summary, snapshot.account);
  renderEngine(snapshot.engine);
  renderResearch(snapshot.research);
  $('footer-time').textContent = `UTC ${new Date().toISOString().replace('T', ' ').slice(0, 19)}`;
}

function renderEngine(engine) {
  if (!engine) return;
  const running = Boolean(engine.running);
  $('engine-badge').textContent = running ? 'RUNNING' : (engine.risk?.halted ? 'HALTED' : 'STOPPED');
  $('engine-badge').className = `status-badge ${running && !engine.risk?.halted ? 'good' : 'danger'}`;
  $('engine-mode').textContent = `${engine.mode || 'PAPER'} MODE`;
  $('engine-subtitle').textContent = engine.risk?.halted ? `Paused: ${engine.risk.halt_reason || 'risk rule'}` : (running ? 'Watching, thinking, and waiting for a safe opportunity.' : 'The robot is not making decisions yet.');
  const decision = engine.decision || {};
  $('decision-action').textContent = decision.action || 'HOLD';
  $('decision-confidence').textContent = `${Math.round(Number(decision.confidence || 0) * 100)}% confidence`;
  $('decision-strategy').textContent = decision.strategy || 'No signal yet';
  $('decision-value').textContent = decision.expected_net_profit ? `${money(decision.expected_net_profit)} USDT` : '—';
  $('decision-position').textContent = engine.position ? `${engine.position.side} ${money(engine.position.quantity)} BTC` : 'Flat';
  $('decision-risk').textContent = engine.risk?.halted ? 'Paused' : 'Within limits';
  $('decision-reason').textContent = decision.reason || 'The robot will explain its last decision here.';
  const stats = engine.statistics || {};
  if (stats.realized_pnl_quote !== undefined) $('account-change').textContent = signedMoney(stats.realized_pnl_quote);
}

function renderAccount(summary, account) {
  if (!summary) return;
  $('account-value').textContent = summary.estimated_total_usdt == null ? '—' : `${money(summary.estimated_total_usdt)} USDT`;
  $('account-subtitle').textContent = summary.estimated_total_usdt == null ? 'Account value needs a current BTC price.' : 'Estimated BTC + USDT value at the current market price.';
  $('account-badge').textContent = account?.can_trade === true ? 'CONNECTED' : 'CHECKED';
  $('account-badge').className = `status-badge ${account?.can_trade === true ? 'good' : 'neutral'}`;
  $('account-change').textContent = signedMoney(summary.session_change_usdt);
  $('account-change').classList.toggle('muted-value', summary.session_change_usdt == null);
  $('account-note').textContent = summary.note || 'This is an estimate, not a promise of profit.';
}

async function fetchState() {
  try {
    const response = await fetch('/api/state', { cache: 'no-store' });
    if (!response.ok) throw new Error('state unavailable');
    render(await response.json());
  } catch (error) {
    setConnection(false, 'Dashboard offline');
  }
}

async function runAction(action, body = {}) {
  const button = document.querySelector(`[data-action="${action}"]`);
  if (button) button.disabled = true;
  toast(state.actionNames[action] || 'Working…');
  try {
    const response = await fetch(`/api/actions/${action}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const result = await response.json();
    if (!response.ok) throw new Error(result.detail || result.error || 'Action failed');
    if (result.status === 'failed') throw new Error(result.error || 'Action failed');
    toast(result.status === 'completed' ? 'Finished. The robot has an update.' : 'Started. The robot is working.');
    await fetchState();
    if (result.action_id) await watchAction(result.action_id);
  } catch (error) {
    toast(`Not completed: ${error.message}`);
  } finally {
    if (button) button.disabled = false;
  }
}

async function watchAction(actionId) {
  for (let attempt = 0; attempt < 90; attempt += 1) {
    await new Promise((resolve) => setTimeout(resolve, 500));
    try {
      const response = await fetch(`/api/action/${actionId}`, { cache: 'no-store' });
      const action = await response.json();
      if (action.status === 'completed') {
        if (action.result?.public) render({ public: action.result.public });
        await fetchState();
        return;
      }
      if (action.status === 'failed') {
        toast(`Not completed: ${action.error || 'the robot stopped'}`);
        return;
      }
    } catch (_) { return; }
  }
}

function connectWebsocket() {
  const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
  try {
    const socket = new WebSocket(`${protocol}://${window.location.host}/ws`);
    state.websocket = socket;
    socket.onmessage = (event) => render(JSON.parse(event.data));
    socket.onclose = () => { state.websocket = null; setConnection(false); };
  } catch (_) { /* polling remains active */ }
}

document.querySelectorAll('[data-action]').forEach((button) => {
  button.addEventListener('click', () => {
    const action = button.dataset.action;
    if (action === 'test-order') {
      if (!window.confirm('The robot will ask Binance to check a tiny practice order. It will not enter the matching engine. Continue?')) return;
      runAction(action, { symbol: 'BTCUSDT', side: 'BUY', order_type: 'MARKET', quote_order_qty: '10' });
      return;
    }
    if (action === 'kill-switch' && !window.confirm('Press OK to stop everything. This creates a sticky safety stop.')) return;
    if (action === 'start-safe-mode' || action === 'start-engine') {
      runAction(action);
      return;
    }
    runAction(action);
  });
});

$('tutorial-toggle').addEventListener('click', () => {
  const content = $('tutorial-content');
  const hidden = content.classList.toggle('hidden');
  $('tutorial-toggle').textContent = hidden ? 'Show' : 'Hide';
  $('tutorial-toggle').setAttribute('aria-expanded', String(!hidden));
});

fetchState();
connectWebsocket();
state.polling = window.setInterval(fetchState, 4000);
