const pipelineEl = document.getElementById('pipeline');
const agentsEl = document.getElementById('agents');
const sourcesEl = document.getElementById('sources');
const themesEl = document.getElementById('themes');
const watchlistEl = document.getElementById('watchlist');
const debateEl = document.getElementById('debate');
const tvPlanEl = document.getElementById('tv-plan');
const leanStatusEl = document.getElementById('lean-status');
const alpacaStatusEl = document.getElementById('alpaca-status');
const dataFeedsEl = document.getElementById('data-feeds');
const optionStreamEl = document.getElementById('option-stream');
const confirmationEl = document.getElementById('confirmation');
const decisionEl = document.getElementById('decision');
const logsEl = document.getElementById('logs');
const statusEl = document.getElementById('system-status');
const updatedEl = document.getElementById('last-updated');
const runScanBtn = document.getElementById('run-scan');
const simulateBtn = document.getElementById('simulate-signal');

const empty = '—';
let latestState = {};
let optionStreamState = null;
let optionStreamReconnectTimer = null;

function statusLabel(status) {
    const labels = {
        idle: 'idle',
        waiting: 'waiting',
        running: 'running',
        done: 'done',
        blocked: 'blocked',
        skipped: 'skipped',
        simulated: 'test',
        disabled: 'disabled',
        needs_setup: 'setup',
        needs_input: 'needs input',
        unavailable: 'unavailable',
        not_configured: 'missing key',
        degraded: 'degraded',
        ok: 'ok',
        error: 'error'
    };
    return labels[status] || status || 'idle';
}

function formatTime(value) {
    if (!value) return '--';
    return new Intl.DateTimeFormat(undefined, {
        hour: 'numeric',
        minute: '2-digit',
        second: '2-digit'
    }).format(new Date(value));
}

function formatPrice(value) {
    if (value === undefined || value === null || value === '') return empty;
    const number = Number(value);
    if (!Number.isFinite(number)) return String(value);
    return `$${number.toFixed(2)}`;
}

function setText(el, value) {
    el.textContent = value || empty;
}

function renderPipeline(stages) {
    pipelineEl.replaceChildren();
    stages.forEach((stage, index) => {
        const item = document.createElement('li');
        item.className = `pipe-step is-${stage.status}`;

        const indexEl = document.createElement('span');
        indexEl.className = 'step-index';
        indexEl.textContent = String(index + 1).padStart(2, '0');

        const copy = document.createElement('div');
        copy.className = 'step-copy';

        const title = document.createElement('strong');
        title.textContent = stage.label;

        const caption = document.createElement('span');
        caption.textContent = stage.caption;

        const detail = document.createElement('small');
        detail.textContent = stage.detail;

        const badge = document.createElement('em');
        badge.textContent = statusLabel(stage.status);

        copy.append(title, caption, detail);
        item.append(indexEl, copy, badge);
        pipelineEl.appendChild(item);
    });
}

function renderThemes(themes) {
    themesEl.replaceChildren();
    if (!themes?.length) {
        themesEl.appendChild(readoutLine('No themes ranked yet', empty));
        return;
    }

    themes.forEach((theme) => {
        const row = document.createElement('div');
        row.className = 'theme-row';

        const strength = document.createElement('span');
        strength.className = 'score-ring';
        strength.style.setProperty('--score', `${theme.strength}%`);
        strength.textContent = theme.strength;

        const copy = document.createElement('div');
        const name = document.createElement('strong');
        name.textContent = theme.name;
        const meta = document.createElement('span');
        meta.textContent = `${theme.direction} · ${theme.tickers.join(', ')}`;
        const why = document.createElement('small');
        why.textContent = theme.why;
        copy.append(name, meta, why);

        row.append(strength, copy);
        themesEl.appendChild(row);
    });
}

function renderAgents(agents) {
    agentsEl.replaceChildren();
    if (!agents?.length) {
        agentsEl.appendChild(readoutLine('Agents', 'No scan yet'));
        return;
    }

    agents.forEach((agent) => {
        const row = document.createElement('div');
        row.className = `agent-row is-${agent.status}`;

        const status = document.createElement('span');
        status.className = 'mini-led';

        const copy = document.createElement('div');
        const name = document.createElement('strong');
        name.textContent = agent.label;
        const detail = document.createElement('small');
        detail.textContent = agent.detail;
        copy.append(name, detail);

        const duration = document.createElement('em');
        duration.textContent = agent.duration_ms ? `${agent.duration_ms}ms` : 'rule';

        row.append(status, copy, duration);
        agentsEl.appendChild(row);
    });
}

function renderSources(sources) {
    sourcesEl.replaceChildren();
    if (!sources?.length) {
        sourcesEl.appendChild(readoutLine('Sources', 'No sources checked'));
        return;
    }

    sources.forEach((source) => {
        const row = document.createElement('div');
        row.className = `source-row is-${source.status}`;
        const name = document.createElement('strong');
        name.textContent = source.name;
        const status = document.createElement('span');
        status.textContent = statusLabel(source.status);
        const detail = document.createElement('small');
        detail.textContent = source.detail;
        row.append(name, status, detail);
        sourcesEl.appendChild(row);
    });
}

function renderWatchlist(items) {
    watchlistEl.replaceChildren();
    if (!items?.length) {
        const row = document.createElement('div');
        row.className = 'table-row muted-row';
        row.textContent = 'No candidates selected';
        watchlistEl.appendChild(row);
        return;
    }

    items.forEach((item) => {
        const row = document.createElement('div');
        row.className = 'table-row';

        const ticker = document.createElement('strong');
        ticker.textContent = `${item.ticker} (${item.bias.toUpperCase()})`;

        const theme = document.createElement('span');
        theme.textContent = item.theme;

        const marketScore = document.createElement('span');
        marketScore.className = 'metric-tag market-tag';
        marketScore.textContent = `${item.technical_score ?? 50}/100`;

        const sourcesScore = document.createElement('span');
        sourcesScore.className = 'metric-tag sources-tag';
        const news = item.news_score ?? 50;
        const sec = item.fundamentals?.fundamental_score ?? 50;
        sourcesScore.textContent = `News ${news} · SEC ${sec}`;

        const overall = document.createElement('strong');
        overall.className = 'overall-badge';
        overall.textContent = `${item.score}/100`;

        const contract = document.createElement('span');
        contract.className = 'contract-tag';
        contract.textContent = item.options?.selected_contract?.symbol || item.options?.status || '—';

        row.append(ticker, theme, marketScore, sourcesScore, overall, contract);
        const revGrowth = item.fundamentals?.revenue_yoy != null ? `${item.fundamentals.revenue_yoy}% YoY Rev` : 'No SEC YoY';
        const triggerTxt = item.trigger || 'Waiting for TV alert';
        row.title = `[Ticker: ${item.ticker}]\n[Trigger Rule] ${triggerTxt}\n[Market Data] Trend Score: ${item.technical_score}/100 | Price: $${item.price || '—'}\n[Sources Data] News Score: ${news}/100 | SEC Growth: ${revGrowth}\n[Options Data] ${item.options?.reason || 'No contract'}`;
        watchlistEl.appendChild(row);
    });
}

function renderDebate(debate) {
    debateEl.replaceChildren();
    if (!debate) {
        debateEl.appendChild(readoutLine('Debate', 'No research debate yet'));
        return;
    }
    const groups = [
        ['Bull', debate.bull || []],
        ['Bear', debate.bear || []],
        ['Risk', [
            `Regime: ${debate.risk?.market_regime || 'unknown'}`,
            `Max premium: $${debate.risk?.max_option_premium || '—'}`
        ]],
        ['Manager', [debate.manager]]
    ];
    groups.forEach(([label, points]) => {
        const block = document.createElement('div');
        block.className = 'debate-block';
        const title = document.createElement('strong');
        title.textContent = label;
        block.appendChild(title);
        points.filter(Boolean).slice(0, 3).forEach((point) => {
            const line = document.createElement('small');
            line.textContent = point;
            block.appendChild(line);
        });
        debateEl.appendChild(block);
    });
}

function renderTradingViewPlan(state) {
    tvPlanEl.replaceChildren();
    const first = state.research?.watchlist?.[0];
    if (!first) {
        tvPlanEl.appendChild(readoutLine('Plan', 'No option-validated ticker yet'));
        return;
    }
    const contract = first.options?.selected_contract || {};
    tvPlanEl.append(
        readoutLine('Top ticker', first.ticker),
        readoutLine('Bias', first.bias),
        readoutLine('Contract', contract.symbol || '—'),
        readoutLine('Timeframes', '1W + 1D'),
        readoutLine('Alert', first.trigger),
        readoutLine('Secret', state.tradingview?.secret_configured ? 'configured' : 'missing', state.tradingview?.secret_configured ? 'positive' : 'warn')
    );
}

function renderLeanStatus(state) {
    leanStatusEl.replaceChildren();
    const lean = state.lean || {};
    const docker = lean.docker || {};
    const imageInstalled = docker.image === 'installed';
    leanStatusEl.append(
        readoutLine('Image', imageInstalled ? 'installed' : docker.image || 'unknown', imageInstalled ? 'positive' : 'warn'),
        readoutLine('Docker', docker.docker || 'unknown', docker.docker === 'available' ? 'positive' : 'warn'),
        readoutLine('Candidates', String(lean.candidate_count ?? 0), lean.candidate_count ? 'positive' : 'warn'),
        readoutLine('Mode', lean.mode || 'dry-run'),
        readoutLine('Export', lean.watchlist_export_path || 'not written'),
        readoutLine('Dry check', 'bash scripts/run_lean_dry_check.sh')
    );
}

function renderAlpacaStatus(state) {
    alpacaStatusEl.replaceChildren();
    const alpaca = state.alpaca || {};
    const healthy = alpaca.status === 'ok';
    const ordersOff = !alpaca.trading_enabled;
    alpacaStatusEl.append(
        readoutLine('Auth', healthy ? 'ok' : alpaca.status || 'unknown', healthy ? 'positive' : 'warn'),
        readoutLine('Account', alpaca.account_status || alpaca.detail || 'not checked'),
        readoutLine('Options', alpaca.options_trading_level ? `level ${alpaca.options_trading_level}` : 'unknown', alpaca.options_trading_level ? 'positive' : 'warn'),
        readoutLine('Buying power', alpaca.buying_power ? `$${alpaca.buying_power}` : 'unknown'),
        readoutLine('Orders', ordersOff ? 'disabled' : 'enabled', ordersOff ? 'warn' : 'positive'),
        readoutLine('Paper', alpaca.paper ? 'yes' : 'no', alpaca.paper ? 'positive' : 'warn')
    );
}

function renderDataFeeds(state) {
    dataFeedsEl.replaceChildren();
    const feedState = state.data_feeds || {};
    const snapshot = feedState.snapshot || {};
    const firstTicker = snapshot.symbols?.[0];
    const firstContract = snapshot.contracts?.[0];
    const stockQuote = firstTicker ? snapshot.underlyings?.[firstTicker]?.selected : null;
    const optionQuote = firstContract ? snapshot.options?.[firstContract]?.selected : null;

    const summary = document.createElement('div');
    summary.className = 'compact-readout';
    summary.append(
        readoutLine('Status', feedState.status || 'unknown', feedState.status === 'ok' ? 'positive' : 'warn'),
        readoutLine('Stocks', stockQuote ? `${firstTicker} ${stockQuote.price || stockQuote.mid || '—'} via ${stockQuote.provider}` : 'no selected quote', stockQuote ? 'positive' : 'warn'),
        readoutLine('Option', optionQuote ? `${firstContract} ${optionQuote.mid || optionQuote.price || '—'} via ${optionQuote.provider}` : 'no selected quote', optionQuote ? 'positive' : 'warn'),
        readoutLine('Primary', `${snapshot.primary?.underlying || '—'} / ${snapshot.primary?.options || '—'}`)
    );
    dataFeedsEl.appendChild(summary);

    const providers = document.createElement('div');
    providers.className = 'provider-list';
    (snapshot.providers || []).forEach((provider) => {
        const row = document.createElement('div');
        row.className = `provider-row is-${provider.status}`;
        const name = document.createElement('strong');
        name.textContent = provider.label;
        const status = document.createElement('span');
        status.textContent = statusLabel(provider.status);
        const detail = document.createElement('small');
        detail.textContent = `${provider.detail} · ${provider.latency_ms || 0}ms`;
        row.append(name, status, detail);
        providers.appendChild(row);
    });
    dataFeedsEl.appendChild(providers);
}

function renderOptionStream(state) {
    optionStreamEl.replaceChildren();
    const stream = optionStreamState || state.option_stream || {};
    const quotes = Object.values(stream.quotes || {})
        .sort((a, b) => String(b.received_at || '').localeCompare(String(a.received_at || '')));
    const latest = quotes[0];
    const running = stream.status === 'running';

    const summary = document.createElement('div');
    summary.className = 'compact-readout';
    summary.append(
        readoutLine('Status', statusLabel(stream.status || 'unknown'), running ? 'positive' : 'warn'),
        readoutLine('Feed', stream.feed || 'indicative'),
        readoutLine('Subscribed', String(stream.subscribed_count ?? 0), stream.subscribed_count ? 'positive' : 'warn'),
        readoutLine('Quotes', `${stream.stream_quote_count || 0} live / ${stream.quote_count || 0} cached`),
        readoutLine('Latest', latest ? `${latest.symbol} ${formatPrice(latest.price || latest.mid)} ${latest.streamed ? 'stream' : latest.source}` : 'waiting for quote', latest ? 'positive' : 'warn')
    );
    optionStreamEl.appendChild(summary);

    const list = document.createElement('div');
    list.className = 'quote-list';
    if (!quotes.length) {
        list.appendChild(readoutLine('Stream', stream.detail || 'No option quotes yet', 'warn'));
    } else {
        quotes.slice(0, 5).forEach((quote) => {
            const row = document.createElement('div');
            row.className = `quote-row ${quote.streamed ? 'is-live' : 'is-seeded'}`;

            const symbol = document.createElement('strong');
            symbol.textContent = quote.symbol;

            const price = document.createElement('span');
            price.textContent = formatPrice(quote.price || quote.mid);

            const detail = document.createElement('small');
            detail.textContent = `bid ${formatPrice(quote.bid)} · ask ${formatPrice(quote.ask)} · ${quote.streamed ? 'stream' : quote.source || 'cached'}`;

            row.append(symbol, price, detail);
            list.appendChild(row);
        });
    }
    optionStreamEl.appendChild(list);
}

function readoutLine(label, value, tone = '') {
    const row = document.createElement('div');
    row.className = `readout-line ${tone}`.trim();

    const labelEl = document.createElement('span');
    labelEl.textContent = label;

    const valueEl = document.createElement('strong');
    valueEl.textContent = value || empty;

    row.append(labelEl, valueEl);
    return row;
}

function renderConfirmation(state) {
    confirmationEl.replaceChildren();
    const signal = state.last_signal;
    const confirmation = state.technical_confirmation;

    if (!signal) {
        confirmationEl.append(
            readoutLine('State', state.tradingview?.verified ? 'connected' : 'not connected', state.tradingview?.verified ? 'positive' : 'warn'),
            readoutLine('Waiting for', 'verified TradingView webhook'),
            readoutLine('Secret', state.tradingview?.secret_configured ? 'configured' : 'missing', state.tradingview?.secret_configured ? 'positive' : 'warn')
        );
        return;
    }

    confirmationEl.append(
        readoutLine('Ticker', signal.ticker),
        readoutLine('Action', signal.action),
        readoutLine('Source', signal.source),
        readoutLine('Verified', signal.verified ? 'yes' : 'no', signal.verified ? 'positive' : 'warn'),
        readoutLine('Status', confirmation?.status, confirmation?.status === 'confirmed' ? 'positive' : 'warn'),
        readoutLine('Reason', confirmation?.reason || confirmation?.matched_trigger)
    );
}

function renderDecision(state) {
    decisionEl.replaceChildren();
    const decision = state.last_decision;
    const execution = state.last_execution;

    if (!decision) {
        decisionEl.append(
            readoutLine('Decision', 'pending'),
            readoutLine('Execution', 'not armed')
        );
        return;
    }

    const approved = String(decision.decision || '').startsWith('approved') || decision.decision === 'test_plan_only';
    decisionEl.append(
        readoutLine('Decision', decision.decision, approved ? 'positive' : 'warn'),
        readoutLine('Reason', decision.reason),
        readoutLine('Confidence', decision.confidence ? `${Math.round(decision.confidence * 100)}%` : empty),
        readoutLine('Contract', decision.contract_plan?.symbol || 'needs explicit OCC symbol'),
        readoutLine('Execution', execution?.status || 'disabled')
    );
}

function renderLogs(events) {
    logsEl.replaceChildren();
    if (!events?.length) {
        const line = document.createElement('div');
        line.className = 'log-line';
        line.textContent = 'No events yet';
        logsEl.appendChild(line);
        return;
    }

    events.forEach((event) => {
        const line = document.createElement('div');
        line.className = `log-line log-${event.level}`;
        const time = document.createElement('span');
        time.textContent = formatTime(event.time);
        const message = document.createElement('p');
        message.textContent = event.message;
        line.append(time, message);
        logsEl.appendChild(line);
    });
}

function renderState(state) {
    latestState = state;
    setText(statusEl, state.status?.replaceAll('_', ' '));
    setText(updatedEl, `updated ${formatTime(state.updated_at)}`);
    renderPipeline(state.stages || []);
    renderAgents(state.research?.agent_runs || []);
    renderSources(state.research?.source_health || []);
    renderThemes(state.research?.themes || []);
    renderWatchlist(state.research?.watchlist || []);
    renderDebate(state.research?.debate);
    renderTradingViewPlan(state);
    renderLeanStatus(state);
    renderAlpacaStatus(state);
    renderDataFeeds(state);
    renderOptionStream(state);
    renderConfirmation(state);
    renderDecision(state);
    renderLogs(state.events || []);
}

async function getState() {
    const response = await fetch('/api/pipeline');
    if (!response.ok) throw new Error('Unable to load pipeline state');
    renderState(await response.json());
}

async function postAction(url) {
    const response = await fetch(url, { method: 'POST' });
    if (!response.ok) throw new Error(`Request failed: ${url}`);
    await getState();
}

function connectOptionStream() {
    if (optionStreamReconnectTimer) {
        clearTimeout(optionStreamReconnectTimer);
        optionStreamReconnectTimer = null;
    }
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const socket = new WebSocket(`${protocol}//${window.location.host}/ws/options`);

    socket.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            optionStreamState = data.stream || data.option_stream || data;
            renderOptionStream(latestState);
        } catch {
            // Ignore non-JSON messages.
        }
    };

    socket.onclose = () => {
        optionStreamReconnectTimer = setTimeout(connectOptionStream, 3000);
    };

    socket.onerror = () => {
        socket.close();
    };
}

runScanBtn.addEventListener('click', () => postAction('/api/research/run'));
simulateBtn.addEventListener('click', () => postAction('/api/simulate/signal'));

getState().catch((error) => {
    statusEl.textContent = 'offline';
    logsEl.textContent = error.message;
});

connectOptionStream();
setInterval(getState, 5000);
