/* ── Trading Pipeline Dashboard ── */
/* Minimal JS: poll state, render 4 phases, handle scan button */

const $ = (id) => document.getElementById(id);
const escapeHtml = (value) => String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');

// ── State polling ──────────────────────────────────────────────
let latestState = {};

async function fetchState() {
    try {
        const res = await fetch('/api/pipeline');
        if (!res.ok) return;
        const state = await res.json();
        latestState = state;
        render(state);
    } catch { /* offline */ }
}

function formatTime(v) {
    if (!v) return '--';
    return new Intl.DateTimeFormat(undefined, {
        hour: 'numeric', minute: '2-digit', second: '2-digit'
    }).format(new Date(v));
}

// ── Main render ────────────────────────────────────────────────
function render(s) {
    renderStatus(s);
    renderSources(s);
    renderResearch(s);
    renderTrader(s);
    renderExecution(s);
    renderEvents(s);
}

// ── Top bar ────────────────────────────────────────────────────
function renderStatus(s) {
    const pill = $('sys-status');
    pill.textContent = (s.status || 'idle').replaceAll('_', ' ');
    pill.className = 'pill' +
        (s.status === 'ready' ? ' is-active' : '') +
        (s.status === 'researching' ? ' is-running' : '');
    $('last-updated').textContent = s.updated_at ? `Updated ${formatTime(s.updated_at)}` : '--';
}

// ── Phase 1: Data Sources ──────────────────────────────────────
function renderSources(s) {
    const r = s.research || {};
    const disc = r.discovery || {};
    const raw = r.raw_inputs || {};
    const mkt = raw.market || {};
    const news = raw.news || {};
    const social = raw.social || {};
    const fundamentals = raw.fundamentals || {};
    const agents = r.agent_runs || [];

    // Discovery lane
    const topEtfs = (disc.etf_rankings || []).slice(0, 3).map(e => `${e.etf} (+${e.return_20d}%)`).join(', ');
    setLaneDetail(
        'lane-discovery',
        'disc-badge',
        disc.themes_active ? `Top ETFs: ${topEtfs || `${disc.themes_active} themes`}` : 'Waiting for scan',
        disc.themes_active ? `${disc.themes_active} themes · ${disc.sectors_active}/${disc.sectors_scanned} sectors` : '--',
        disc.themes_active ? 'ok' : null,
        `Sources used: ${(disc.sources_used || []).join(', ') || 'not run'}. Scanned ${disc.sectors_scanned || 0}/${disc.etfs_configured || 0} configured ETFs and selected ${disc.sectors_active || 0} sectors.`
    );

    // Market lane
    const wl = r.watchlist || [];
    const topTickers = wl.slice(0, 4).map(w => w.ticker).join(', ');
    setLaneDetail(
        'lane-market',
        'mkt-badge',
        mkt.symbols_loaded ? `Loaded ${mkt.symbols_loaded} symbols · Top: ${topTickers}` : 'Waiting for scan',
        mkt.symbols_loaded ? `${mkt.symbols_loaded} symbols` : '--',
        mkt.symbols_loaded ? 'ok' : null,
        `Source used: ${mkt.source || 'not run'}. Technical thresholds and scoring weights are shown in Config Sources → Market.`
    );

    // News lane
    const newsSent = typeof news.sentiment === 'number' ? news.sentiment : (news.sentiment?.score ?? 50);
    const newsLabel = newsSent > 60 ? 'positive' : newsSent < 40 ? 'negative' : 'neutral';
    setLaneDetail(
        'lane-news',
        'news-badge',
        `Sentiment: ${newsLabel} · Score ${newsSent}/100`,
        `Score ${newsSent}`,
        'ok',
        `Sources used: ${(news.sources_used || []).join(', ') || news.source || 'not run'}. Score ${newsSent}/100 uses the configured positive and negative keyword dictionaries.`
    );

    // Social lane
    const socSent = typeof social.sentiment === 'number' ? social.sentiment : (social.sentiment?.score ?? 50);
    const socLabel = socSent > 60 ? 'positive' : socSent < 40 ? 'negative' : 'neutral';
    setLaneDetail(
        'lane-social',
        'social-badge',
        `Sentiment: ${socLabel} · Score ${socSent}/100`,
        `Score ${socSent}`,
        'ok',
        `Sources used: ${(social.sources_used || []).join(', ') || social.source || 'not run'}. Score ${socSent}/100 uses the configured social keyword dictionaries.`
    );

    // SEC lane
    const secAgent = agents.find(a => a.id === 'fundamentals');
    setLaneDetail(
        'lane-sec',
        'sec-badge',
        secAgent ? `SEC XBRL company facts verified (${secAgent.duration_ms}ms)` : 'Waiting for scan',
        secAgent ? `${secAgent.duration_ms}ms` : '--',
        secAgent?.status === 'ok' ? 'ok' : secAgent?.status === 'degraded' ? 'warn' : null,
        `Source used: ${(fundamentals.sources_used || []).join(', ') || fundamentals.source || 'not run'}. Revenue and net-income tags, thresholds, and score weights are configurable.`
    );

    renderDrawers(s);
    renderSelectedUniverse(s);
}

function setLaneDetail(laneId, badgeId, subtext, badgeText, status, tooltip) {
    const lane = $(laneId);
    if (lane) {
        const small = lane.querySelector('small');
        if (small && subtext) small.textContent = subtext;
        lane.title = tooltip;
    }
    const badge = $(badgeId);
    if (badge) {
        badge.textContent = badgeText;
        badge.className = 'lane-badge' + (status ? ` is-${status}` : '');
    }
}

// ── Drawers (Expandable Scanned Items) ──────────────────────────
function renderDrawers(s) {
    const r = s.research || {};
    const disc = r.discovery || {};
    const raw = r.raw_inputs || {};

    function appendDrawerItem(container, {selected = true, name, source, metric, title}) {
        const item = document.createElement('div');
        item.className = 'drawer-item';
        item.title = title || '';

        const left = document.createElement('div');
        left.className = 'item-left';
        const check = document.createElement('span');
        check.className = `item-check ${selected ? 'is-selected' : 'is-skipped'}`;
        check.textContent = selected ? '✓' : '✗';
        const itemName = document.createElement('span');
        itemName.className = 'item-name';
        itemName.textContent = name;
        left.append(check, itemName);

        const sourceEl = document.createElement('span');
        sourceEl.className = 'item-source';
        sourceEl.textContent = source || 'Unknown source';
        const metricEl = document.createElement('span');
        metricEl.className = 'item-metric';
        metricEl.textContent = metric || '';
        item.append(left, sourceEl, metricEl);
        container.appendChild(item);
    }

    // 1. Discovery Drawer
    const discDrawer = $('drawer-disc');
    if (discDrawer) {
        discDrawer.replaceChildren();
        const rankings = disc.etf_rankings || [];
        if (!rankings.length) {
            discDrawer.innerHTML = '<span class="empty-hint">No ETF rankings yet</span>';
        } else {
            rankings.slice(0, 8).forEach((e, idx) => {
                const selected = idx < (disc.sectors_active || 5);
                appendDrawerItem(discDrawer, {
                    selected,
                    name: `${e.etf} (${e.sector})`,
                    source: (disc.sources_used || ['yahoo_chart']).join(', '),
                    metric: `${e.return_20d >= 0 ? '+' : ''}${e.return_20d}% 20D`,
                    title: selected
                        ? `Rank #${idx + 1}: momentum score ${e.momentum_score} in ${e.sector}.`
                        : `Momentum score ${e.momentum_score} was outside the selected sectors.`,
                });
            });
        }
    }

    // 2. Market Drawer
    const mktDrawer = $('drawer-mkt');
    if (mktDrawer) {
        mktDrawer.replaceChildren();
        const wl = r.watchlist || [];
        if (!wl.length) {
            mktDrawer.innerHTML = '<span class="empty-hint">No market bars scanned yet</span>';
        } else {
            wl.slice(0, 6).forEach(w => {
                appendDrawerItem(mktDrawer, {
                    name: `${w.ticker} (${w.theme})`,
                    source: raw.market?.source || 'Market bars',
                    metric: `Score ${w.technical_score}/100`,
                    title: `Technical score ${w.technical_score}/100 using the currently configured market periods, thresholds, and weights.`,
                });
            });
        }
    }

    // 3. News Drawer
    const newsDrawer = $('drawer-news');
    if (newsDrawer) {
        newsDrawer.replaceChildren();
        const newsItems = Object.entries(raw.news?.items_by_theme || {}).flatMap(([theme, items]) =>
            (items || []).map(item => ({...item, theme}))
        );
        if (!newsItems.length) {
            newsDrawer.innerHTML = '<span class="empty-hint">No news articles scanned yet</span>';
        } else {
            newsItems.slice(0, 8).forEach(item => {
                appendDrawerItem(newsDrawer, {
                    name: item.title,
                    source: item.source || item.publisher || raw.news?.source,
                    metric: item.theme,
                    title: `Headline retained for the ${item.theme} theme and included in configured sentiment scoring.`,
                });
            });
        }
    }

    // 4. Social Drawer
    const socialDrawer = $('drawer-social');
    if (socialDrawer) {
        socialDrawer.replaceChildren();
        const socialItems = Object.entries(raw.social?.items_by_theme || {}).flatMap(([theme, items]) =>
            (items || []).map(item => ({...item, theme}))
        );
        if (socialItems.length) {
            socialItems.slice(0, 8).forEach(item => {
                appendDrawerItem(socialDrawer, {
                    name: item.title,
                    source: item.source || raw.social?.source,
                    metric: item.theme,
                    title: `Discussion retained for the ${item.theme} theme and included in configured sentiment scoring.`,
                });
            });
        } else {
            (raw.social?.sources_used || []).forEach(source => {
                appendDrawerItem(socialDrawer, {
                    name: source,
                    source: 'Configured source',
                    metric: 'No retained posts',
                    title: 'This source was enabled for the scan but returned no retained discussion items.',
                });
            });
            if (!socialDrawer.children.length) {
                socialDrawer.innerHTML = '<span class="empty-hint">No social sources or posts in the latest scan</span>';
            }
        }
    }

    // 5. SEC Drawer
    const secDrawer = $('drawer-sec');
    if (secDrawer) {
        secDrawer.replaceChildren();
        const wl = r.watchlist || [];
        if (!wl.length) {
            secDrawer.innerHTML = '<span class="empty-hint">No SEC CIK facts queried yet</span>';
        } else {
            wl.slice(0, 5).forEach(w => {
                const rev = w.fundamentals?.revenue_yoy;
                const revStr = rev != null ? `+${rev.toFixed(1)}% YoY Rev` : 'SEC verified';
                appendDrawerItem(secDrawer, {
                    name: `${w.ticker} (CIK ${w.fundamentals?.cik || 'unknown'})`,
                    source: raw.fundamentals?.source || 'SEC company facts',
                    metric: revStr,
                    title: `SEC facts produced fundamental score ${w.fundamental_score}/100. Revenue benchmark passed: ${w.fundamentals?.meets_revenue_growth_benchmark ? 'yes' : 'no'}.`,
                });
            });
        }
    }
}

// ── Selected Universe Summary Card Renderer ───────────────────────
function renderSelectedUniverse(s) {
    const r = s.research || {};
    const wl = r.watchlist || [];
    const disc = r.discovery || {};
    const themes = r.themes || [];

    const grid = $('universe-grid');
    const countEl = $('universe-count');
    if (!grid) return;
    grid.replaceChildren();

    // Gather universe symbols from themes + watchlist
    const universe = [];
    const seen = new Set();

    wl.forEach(w => {
        if (!seen.has(w.ticker)) {
            seen.add(w.ticker);
            universe.push({
                ticker: w.ticker,
                source: w.theme || 'Discovery',
                reason: w.selection_reason || `Nominated via ${w.theme} sector momentum.`,
                score: w.score
            });
        }
    });

    themes.forEach(t => {
        (t.tickers || []).forEach(tk => {
            if (!seen.has(tk)) {
                seen.add(tk);
                universe.push({
                    ticker: tk,
                    source: t.name,
                    reason: `Nominated via active theme '${t.name}' (Strength ${t.strength}/100).`,
                    score: t.strength
                });
            }
        });
    });

    (disc.alpaca_extras || []).forEach(extra => {
        if (!seen.has(extra)) {
            seen.add(extra);
            universe.push({
                ticker: extra,
                source: 'Alpaca Mover',
                reason: `Nominated via Alpaca top market movers screener.`,
                score: 75
            });
        }
    });

    if (countEl) countEl.textContent = `${universe.length} Selected`;

    if (!universe.length) {
        grid.innerHTML = '<span class="empty-hint">Run scan to populate dynamic universe</span>';
        return;
    }

    universe.forEach(u => {
        const badge = document.createElement('span');
        badge.className = 'uni-badge';
        badge.style.cursor = 'pointer';
        badge.innerHTML = `✓ ${u.ticker} <span class="uni-source">(${u.source})</span> <span style="font-size:0.7rem;opacity:0.75;margin-left:2px;">🧠</span>`;
        badge.title = `[Ticker: ${u.ticker}]\n[Nominated By] ${u.source}\n[Why Selected] ${u.reason}\n\n👉 Click to inspect Intelligence for ${u.ticker}`;
        badge.onclick = (e) => {
            e.stopPropagation();
            openIntelDrawer(u.ticker);
        };
        grid.appendChild(badge);
    });
}

// ── Phase 2: Research Team ─────────────────────────────────────
function renderResearch(s) {
    const debate = s.research?.debate;

    // Bull points
    const bullEl = $('bull-points');
    bullEl.replaceChildren();
    const bullPts = debate?.bull || [];
    if (bullPts.length) {
        bullPts.slice(0, 3).forEach(p => {
            const li = document.createElement('li');
            li.textContent = p;
            li.title = `Why Selected: Key bullish catalyst identified by Research Agent.`;
            bullEl.appendChild(li);
        });
    } else {
        const li = document.createElement('li');
        li.className = 'placeholder';
        li.textContent = 'Waiting for scan...';
        bullEl.appendChild(li);
    }

    // Bear points
    const bearEl = $('bear-points');
    bearEl.replaceChildren();
    const bearPts = debate?.bear || [];
    if (bearPts.length) {
        bearPts.slice(0, 3).forEach(p => {
            const li = document.createElement('li');
            li.textContent = p;
            li.title = `Why Selected: Downside risk factor evaluated by Risk Management Agent.`;
            bearEl.appendChild(li);
        });
    } else {
        const li = document.createElement('li');
        li.className = 'placeholder';
        li.textContent = 'Waiting for scan...';
        bearEl.appendChild(li);
    }

    // Themes bar
    const themesBar = $('themes-bar');
    themesBar.replaceChildren();
    const themes = s.research?.themes || [];
    if (!themes.length) {
        const empty = document.createElement('span');
        empty.className = 'theme-pill';
        empty.textContent = 'No themes yet';
        themesBar.appendChild(empty);
        return;
    }
    themes.slice(0, 6).forEach(t => {
        const pill = document.createElement('span');
        pill.className = 'theme-pill';
        pill.innerHTML = `${t.name} <span class="strength">${t.strength}</span>`;
        pill.title = `Why Selected: ${t.why || 'High sector momentum + constituent selection'}. Strength: ${t.strength}/100.`;
        themesBar.appendChild(pill);
    });
}

// ── Phase 3: Trader ────────────────────────────────────────────
function renderTrader(s) {
    const wl = s.research?.watchlist || [];
    const wlEl = $('watchlist');
    wlEl.replaceChildren();

    if (!wl.length) {
        const empty = document.createElement('div');
        empty.className = 'wl-empty';
        empty.textContent = 'No candidates yet';
        wlEl.appendChild(empty);
    } else {
        wl.slice(0, 6).forEach(item => {
            const row = document.createElement('div');
            row.className = 'wl-row';

            const left = document.createElement('div');
            const ticker = document.createElement('span');
            ticker.className = 'wl-ticker';
            ticker.textContent = item.ticker;
            const theme = document.createElement('span');
            theme.className = 'wl-theme';
            theme.textContent = ` · ${item.theme}`;
            left.append(ticker, theme);

            const bias = document.createElement('span');
            bias.className = `wl-bias ${item.bias}`;
            bias.textContent = item.bias;

            const score = document.createElement('span');
            const sc = item.score || 0;
            score.className = `wl-score ${sc >= 70 ? 'high' : sc >= 50 ? 'med' : 'low'}`;
            score.textContent = sc;

            const contract = document.createElement('span');
            contract.className = 'wl-contract';
            const sym = item.options?.selected_contract?.symbol;
            contract.textContent = sym || item.options?.status || '—';

            const intelBtn = document.createElement('button');
            intelBtn.className = 'wl-intel-btn';
            intelBtn.type = 'button';
            intelBtn.textContent = '🧠 Intel';
            intelBtn.title = `Open ${item.ticker} Intelligence Drawer`;
            intelBtn.onclick = (e) => {
                e.stopPropagation();
                openIntelDrawer(item.ticker);
            };

            row.append(left, contract, bias, score, intelBtn);

            // Rich 1-line hover explanation
            const selReason = item.selection_reason || `Selected via ${item.theme} theme momentum.`;
            const scoreBreak = item.score_breakdown || `Score ${item.score}/100: Market + News + Options + SEC + Intelligence.`;
            const optInfo = sym
                ? `Contract: ${sym} (DTE: ${item.options.selected_contract.dte || '?'}d, OI: ${item.options.selected_contract.open_interest || '?'})`
                : `Options: ${item.options?.reason || 'No contract'}`;
            const intelInfo = item.intelligence
                ? `\n[Intelligence] ${item.intelligence.composite_signal?.toUpperCase() || 'NEUTRAL'} (${item.intelligence.intelligence_score ?? item.intelligence_score ?? 50}/100) · ${item.intelligence.insider_summary || 'SEC filings scanned'} · ${item.intelligence.seasonality_summary || ''}`
                : '';

            row.title = `[Why Selected] ${selReason}\n[Score Breakdown] ${scoreBreak}\n[Options Detail] ${optInfo}${intelInfo}\n\n👉 Click row or '🧠 Intel' button to open Intelligence Drawer`;

            wlEl.appendChild(row);
        });
    }

    // Risk regime
    const debate = s.research?.debate;
    const regimeEl = $('risk-regime');
    if (regimeEl) {
        const regime = debate?.risk?.market_regime || '--';
        regimeEl.querySelector('span').textContent = regime;
    }

    const optEl = $('risk-options');
    if (optEl) {
        const optCount = wl.filter(w => w.options?.selected_contract).length;
        optEl.querySelector('span').textContent =
            wl.length ? `${optCount}/${wl.length} contracts valid` : '--';
    }
}

// ── Phase 4: Execution ─────────────────────────────────────────
function renderExecution(s) {
    const alp = s.alpaca || {};
    const tv = s.tradingview || {};
    const dec = s.last_decision;
    const exec = s.last_execution;

    // Alpaca
    const alpEl = $('exec-alpaca');
    if (alpEl) {
        const sp = alpEl.querySelector('span');
        const ok = alp.status === 'ok';
        sp.textContent = ok
            ? `Paper · $${Number(alp.buying_power || 0).toLocaleString()} buying power`
            : (alp.detail || alp.status || '--');
        sp.className = ok ? 'ok' : 'warn';
    }

    // TradingView
    const tvEl = $('exec-tv');
    if (tvEl) {
        const sp = tvEl.querySelector('span');
        const configured = tv.secret_configured;
        sp.textContent = configured ? 'Webhook secret configured' : 'Secret not set';
        sp.className = configured ? 'ok' : 'warn';
    }

    // Last order
    const ordEl = $('exec-order');
    if (ordEl) {
        const sp = ordEl.querySelector('span');
        sp.textContent = exec?.status || 'No orders placed';
        sp.className = exec?.status === 'filled' ? 'ok' : '';
    }

    // Decision
    const decEl = $('exec-decision');
    if (decEl) {
        const sp = decEl.querySelector('span');
        if (dec) {
            const approved = String(dec.decision || '').includes('approved');
            sp.textContent = `${dec.decision} · ${Math.round((dec.confidence || 0) * 100)}%`;
            sp.className = approved ? 'ok' : 'warn';
        } else {
            sp.textContent = 'Pending';
            sp.className = '';
        }
    }
}

// ── Events ─────────────────────────────────────────────────────
function renderEvents(s) {
    const el = $('events');
    if (!el) return;
    el.replaceChildren();
    const evts = s.events || [];
    if (!evts.length) {
        const line = document.createElement('div');
        line.className = 'evt';
        line.textContent = 'No events yet';
        el.appendChild(line);
        return;
    }
    evts.slice(0, 12).forEach(e => {
        const line = document.createElement('div');
        line.className = `evt evt-${e.level || 'info'}`;
        const time = document.createElement('span');
        time.className = 'evt-time';
        time.textContent = formatTime(e.time);
        line.append(time, document.createTextNode(e.message));
        el.appendChild(line);
    });
}

// ── Actions ────────────────────────────────────────────────────
$('btn-scan').addEventListener('click', async () => {
    $('btn-scan').disabled = true;
    $('btn-scan').textContent = '⏳ Scanning...';
    try {
        await fetch('/api/research/run', { method: 'POST' });
        await fetchState();
    } finally {
        $('btn-scan').disabled = false;
        $('btn-scan').textContent = '▶ Run Scan';
    }
});

// ── Live Option Stream (WebSocket) ─────────────────────────────
let optionStreamData = {};
let optionStreamReconnectTimer = null;

function connectOptionStream() {
    if (optionStreamReconnectTimer) {
        clearTimeout(optionStreamReconnectTimer);
        optionStreamReconnectTimer = null;
    }
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const socket = new WebSocket(`${protocol}//${window.location.host}/ws/options`);

    socket.onopen = () => {
        const statusEl = $('stream-status');
        statusEl.textContent = 'connected';
        statusEl.className = 'stream-status is-connected';
    };

    socket.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            const snapshot = data.stream || data.option_stream || data;
            const quotes = snapshot.quotes || snapshot;
            if (snapshot.quotes && typeof snapshot.quotes === 'object') {
                // The server sends a complete symbol-keyed snapshot. Replacing the
                // map also removes contracts that were unsubscribed after a scan.
                const targetOrder = Array.isArray(snapshot.target_symbols)
                    ? snapshot.target_symbols
                    : Object.keys(snapshot.quotes);
                optionStreamData = {};
                targetOrder.forEach(symbol => {
                    if (snapshot.quotes[symbol]) optionStreamData[symbol] = snapshot.quotes[symbol];
                });
            } else if (Array.isArray(quotes)) {
                quotes.forEach(q => {
                    if (q.symbol) optionStreamData[q.symbol] = q;
                });
            } else if (quotes.symbol) {
                optionStreamData[quotes.symbol] = quotes;
            }
            const statusEl = $('stream-status');
            if (statusEl && snapshot.status === 'running') {
                const liveCount = snapshot.stream_quote_count ?? Object.keys(optionStreamData).length;
                statusEl.textContent = liveCount ? `live · ${liveCount}` : 'connected';
                statusEl.className = 'stream-status is-connected';
            }
            renderOptionStream();
        } catch { /* ignore non-JSON */ }
    };

    socket.onclose = () => {
        const statusEl = $('stream-status');
        statusEl.textContent = 'disconnected';
        statusEl.className = 'stream-status is-disconnected';
        optionStreamReconnectTimer = setTimeout(connectOptionStream, 3000);
    };

    socket.onerror = () => socket.close();
}

function renderOptionStream() {
    const el = $('option-stream');
    if (!el) return;
    el.replaceChildren();

    const entries = Object.values(optionStreamData);
    if (!entries.length) {
        const empty = document.createElement('div');
        empty.className = 'stream-empty';
        empty.textContent = 'Waiting for live quotes...';
        el.appendChild(empty);
        return;
    }

    entries.slice(0, 8).forEach(q => {
        const row = document.createElement('div');
        row.className = 'stream-row';

        const sym = document.createElement('span');
        sym.className = 'stream-symbol';
        sym.textContent = q.symbol;

        const bid = document.createElement('span');
        bid.className = 'stream-bid';
        bid.textContent = q.bid != null ? `B $${Number(q.bid).toFixed(2)}` : 'B --';

        const ask = document.createElement('span');
        ask.className = 'stream-ask';
        ask.textContent = q.ask != null ? `A $${Number(q.ask).toFixed(2)}` : 'A --';

        const last = document.createElement('span');
        last.className = 'stream-last';
        const livePrice = q.last ?? q.price ?? q.mid;
        last.textContent = livePrice != null ? `M $${Number(livePrice).toFixed(2)}` : 'M --';

        row.append(sym, bid, ask, last);
        el.appendChild(row);
    });
}

// ── Toggle buttons for drawers ──────────────────────────────────
['disc', 'mkt', 'news', 'social', 'sec'].forEach(key => {
    const btn = $(`toggle-${key}`);
    const drawer = $(`drawer-${key}`);
    if (btn && drawer) {
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            const isHidden = drawer.style.display === 'none';
            drawer.style.display = isHidden ? 'flex' : 'none';
            btn.textContent = isHidden ? '▲' : '▼';
        });
    }
});

// ── Data Sources Real-time Configuration Modal Logic ─────────────
let sourcesConfigData = null;
let sourcesConfigMeta = null;
let sourceContracts = {};
let activeConfigTab = 'tab-discovery';

const configSectionByTab = {
    'tab-discovery': 'discovery',
    'tab-market': 'market',
    'tab-news': 'news',
    'tab-social': 'social',
    'tab-fundamentals': 'fundamentals'
};

async function fetchSourcesConfig() {
    try {
        const res = await fetch('/api/sources/config');
        if (!res.ok) throw new Error(`Configuration request failed (${res.status})`);
        const data = await res.json();
        sourcesConfigData = data.config;
        sourcesConfigMeta = data.meta || null;
        sourceContracts = data.source_contracts || {};
        applyConfiguredSourceLabels();
        return true;
    } catch (err) {
        showConfigStatus(err.message || 'Unable to load configuration', 'err');
        return false;
    }
}

function openConfigModal() {
    const modal = $('config-modal');
    if (!modal) return;
    modal.style.display = 'flex';
    document.body.classList.add('modal-open');
    if (!sourcesConfigData) {
        const container = $('modal-form-container');
        if (container) container.innerHTML = '<span class="empty-hint">Loading current configuration…</span>';
        fetchSourcesConfig().then(ok => { if (ok) renderModalTab(activeConfigTab); });
    } else {
        renderModalTab(activeConfigTab);
    }
}

function closeConfigModal() {
    const modal = $('config-modal');
    if (modal) modal.style.display = 'none';
    document.body.classList.remove('modal-open');
}

function renderModalTab(tabId) {
    activeConfigTab = tabId;
    if ($('btn-save-cfg')) $('btn-save-cfg').disabled = false;
    if ($('btn-reset-cfg')) $('btn-reset-cfg').disabled = false;
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.toggle('is-active', btn.dataset.tab === tabId);
        btn.setAttribute('aria-selected', btn.dataset.tab === tabId ? 'true' : 'false');
    });

    const container = $('modal-form-container');
    if (!container || !sourcesConfigData) return;
    container.replaceChildren();
    container.scrollTop = 0;

    const sectionKey = configSectionByTab[tabId] || 'discovery';
    const section = sourcesConfigData[sectionKey];
    if (!section) return;

    // Header title & description
    const title = document.createElement('div');
    title.className = 'cfg-section-title';
    title.textContent = section.title || sectionKey.toUpperCase();

    const desc = document.createElement('div');
    desc.className = 'cfg-section-desc';
    desc.textContent = section.description || '';

    container.append(title, desc);

    const applyNote = document.createElement('div');
    applyNote.className = 'cfg-apply-note';
    const updated = sourcesConfigMeta?.updated_at ? formatTime(sourcesConfigMeta.updated_at) : '--';
    applyNote.textContent = `Currently active · last saved ${updated} · changes apply to the next scan or quote refresh`;
    container.appendChild(applyNote);

    // Exact provider and endpoint inventory currently in use.
    const inventoryHead = document.createElement('div');
    inventoryHead.className = 'cfg-row-head';
    const inventoryTitle = document.createElement('h3');
    inventoryTitle.className = 'cfg-subtitle';
    inventoryTitle.textContent = 'Configured sources right now';
    const addSourceButton = document.createElement('button');
    addSourceButton.type = 'button';
    addSourceButton.className = 'btn-secondary cfg-add-source';
    addSourceButton.textContent = '+ Add source';
    addSourceButton.addEventListener('click', () => renderSourceEditor(sectionKey));
    inventoryHead.append(inventoryTitle, addSourceButton);
    container.appendChild(inventoryHead);

    const sourceGrid = document.createElement('div');
    sourceGrid.className = 'cfg-source-grid';
    (section.source_inventory || []).forEach(source => {
        const card = document.createElement('div');
        card.className = `cfg-source-card ${source.enabled ? 'is-enabled' : 'is-disabled'}`;

        const head = document.createElement('div');
        head.className = 'cfg-source-head';
        const name = document.createElement('strong');
        name.textContent = source.label;
        const state = document.createElement('span');
        state.className = `cfg-source-state ${source.enabled ? 'is-enabled' : 'is-disabled'}`;
        state.textContent = source.enabled ? 'Enabled' : 'Disabled';
        head.append(name, state);

        const purpose = document.createElement('span');
        purpose.className = 'cfg-source-purpose';
        purpose.textContent = source.purpose || '';
        card.append(head, purpose);

        if (source.endpoint) {
            const endpoint = document.createElement('code');
            endpoint.className = 'cfg-source-endpoint';
            endpoint.textContent = source.endpoint;
            card.appendChild(endpoint);
        }

        const credential = document.createElement('span');
        credential.className = `cfg-credential is-${source.credential_status || 'public'}`;
        const envNames = (source.credential_env || []).join(', ');
        credential.textContent = source.credential_status === 'public'
            ? 'No API key required'
            : source.credential_status === 'configured'
                ? `Credentials configured (${envNames})`
                : `Credentials missing (${envNames})`;
        card.appendChild(credential);
        if (source.custom) {
            const actions = document.createElement('div');
            actions.className = 'cfg-card-actions';
            const editButton = document.createElement('button');
            editButton.type = 'button';
            editButton.textContent = 'Edit';
            editButton.addEventListener('click', () => {
                const definition = (section.custom_sources || []).find(item => item.id === source.id);
                renderSourceEditor(sectionKey, definition);
            });
            const testButton = document.createElement('button');
            testButton.type = 'button';
            testButton.textContent = 'Test';
            testButton.addEventListener('click', () => testSourceDefinition(sectionKey, source.id));
            const deleteButton = document.createElement('button');
            deleteButton.type = 'button';
            deleteButton.className = 'is-danger';
            deleteButton.textContent = 'Delete';
            deleteButton.addEventListener('click', () => deleteCustomSource(sectionKey, source.id));
            actions.append(editButton, testButton, deleteButton);
            card.appendChild(actions);
        }
        sourceGrid.appendChild(card);
    });
    if (!sourceGrid.children.length) {
        sourceGrid.innerHTML = '<span class="empty-hint">No sources implemented for this section.</span>';
    }
    container.appendChild(sourceGrid);

    renderScoringExplanation(container, sectionKey, section);

    const settingsTitle = document.createElement('h3');
    settingsTitle.className = 'cfg-subtitle';
    settingsTitle.textContent = 'Editable runtime settings';
    container.appendChild(settingsTitle);

    // Render only declared, validated fields. Every control shows its active value.
    const schema = section.field_schema || {};
    Object.entries(schema).forEach(([key, fieldMeta]) => {
        if (fieldMeta.hidden) return;
        const value = section[key];

        const group = document.createElement('div');
        group.className = 'cfg-group';

        const label = document.createElement('label');
        label.textContent = fieldMeta.label || key;

        let input;
        if (fieldMeta.type === 'list' || Array.isArray(value)) {
            input = document.createElement('textarea');
            input.rows = 2;
            input.value = Array.isArray(value) ? value.join(', ') : String(value || '');
            input.dataset.type = 'list';
        } else if (fieldMeta.type === 'number' || fieldMeta.type === 'integer' || typeof value === 'number') {
            input = document.createElement('input');
            input.type = 'number';
            input.step = fieldMeta.type === 'integer' ? '1' : 'any';
            input.value = value != null ? value : '';
            input.dataset.type = fieldMeta.type === 'integer' ? 'integer' : 'number';
            if (fieldMeta.min != null) input.min = String(fieldMeta.min);
            if (fieldMeta.max != null) input.max = String(fieldMeta.max);
        } else {
            input = document.createElement('input');
            input.type = fieldMeta.type === 'url' ? 'url' : 'text';
            input.value = value != null ? value : '';
            input.dataset.type = 'text';
        }

        if (fieldMeta.placeholder) input.placeholder = fieldMeta.placeholder;

        input.dataset.section = sectionKey;
        input.dataset.key = key;

        const help = document.createElement('span');
        help.className = 'cfg-help';
        const choices = fieldMeta.choices?.length ? ` Allowed values: ${fieldMeta.choices.join(', ')}.` : '';
        const origin = fieldMeta.origin ? ` Basis: ${fieldMeta.origin}` : '';
        help.textContent = `Expected: ${fieldMeta.help || 'Enter a valid value.'}${choices}${origin}`;

        group.append(label, input, help);
        container.appendChild(group);
    });
}

function renderScoringExplanation(container, sectionKey, section) {
    const formulas = {
        discovery: `score = (${section.weight_5d_return} × 5D return %) + (${section.weight_20d_return} × 20D return %) + (${section.weight_volume_expansion} × max(volume ratio − 1, 0))`,
        market: `score = clamp[0,100](50 + ${section.weight_5d_return} × 5D return % + ${section.weight_20d_return} × 20D return % + ${section.weight_60d_return} × 60D return % + ${section.sma_short_bonus} if above short SMA + ${section.sma_long_bonus} if above long SMA + min(${section.volume_bonus_cap}, ${section.weight_volume_expansion} × max(volume ratio − ${section.minimum_volume_ratio}, 0)) − ${section.overbought_penalty} if RSI > ${section.overbought_rsi})`,
        news: `sentiment = clamp[0,100](50 + ${section.sentiment_word_weight} × positive keyword matches − ${section.sentiment_word_weight} × negative keyword matches)`,
        social: `sentiment = clamp[0,100](50 + ${section.sentiment_word_weight} × positive keyword matches − ${section.sentiment_word_weight} × negative keyword matches)`,
        fundamentals: `score = clamp[0,100](${section.base_score} + clip(revenue YoY % × ${section.revenue_growth_weight}, ±${section.revenue_contribution_cap}) + clip(net-income YoY % × ${section.net_income_growth_weight}, ±${section.net_income_contribution_cap}))`,
    };
    if (!formulas[sectionKey]) return;

    const panel = document.createElement('section');
    panel.className = 'cfg-scoring-card';
    const head = document.createElement('div');
    head.className = 'cfg-scoring-head';
    const title = document.createElement('div');
    title.innerHTML = '<strong>How this score is calculated</strong><span>Active formula — values below are exactly what the running agents use.</span>';
    head.appendChild(title);

    const provenance = section.scoring_provenance || {};
    if (sectionKey === 'discovery' || sectionKey === 'market') {
        const status = document.createElement('span');
        status.className = `cfg-model-status is-${provenance.status || 'legacy_unvalidated'}`;
        status.textContent = provenance.status === 'calibrated'
            ? 'Calibrated'
            : provenance.status === 'rejected'
                ? 'Calibration rejected'
                : 'Unvalidated defaults';
        head.appendChild(status);
    }
    panel.appendChild(head);

    const formula = document.createElement('code');
    formula.className = 'cfg-formula';
    formula.textContent = formulas[sectionKey];
    panel.appendChild(formula);

    const basis = document.createElement('p');
    basis.className = 'cfg-model-basis';
    if (sectionKey === 'discovery' || sectionKey === 'market') {
        basis.textContent = provenance.origin || provenance.method || 'No coefficient provenance recorded.';
    } else {
        basis.textContent = 'This is an explicit configurable heuristic, not a statistically validated prediction model.';
    }
    panel.appendChild(basis);

    if (provenance.metrics && Object.keys(provenance.metrics).length) {
        const metrics = document.createElement('div');
        metrics.className = 'cfg-metrics';
        const metricLabels = {
            validation_correlation: 'Holdout correlation',
            validation_correlation_t_stat: 'Correlation t-stat',
            validation_directional_accuracy: 'Direction accuracy',
            validation_top_quartile_mean_return_pct: 'Top quartile return',
            validation_all_mean_return_pct: 'All observations return',
        };
        Object.entries(provenance.metrics).forEach(([key, value]) => {
            const item = document.createElement('span');
            item.innerHTML = `<small>${escapeHtml(metricLabels[key] || key)}</small><strong>${escapeHtml(String(value))}${key.includes('return_pct') ? '%' : ''}</strong>`;
            metrics.appendChild(item);
        });
        panel.appendChild(metrics);
    }

    if (sectionKey === 'discovery' || sectionKey === 'market') {
        const calibrationRow = document.createElement('div');
        calibrationRow.className = 'cfg-calibration-row';
        const explanation = document.createElement('span');
        explanation.textContent = 'Calibration fits ridge regression to forward returns and applies weights only when the chronological holdout has positive, statistically credible correlation (t ≥ 1.96) and top-quartile lift.';
        const calibrateButton = document.createElement('button');
        calibrateButton.type = 'button';
        calibrateButton.className = 'btn-secondary';
        calibrateButton.textContent = 'Calibrate & apply';
        calibrateButton.addEventListener('click', () => runScoreCalibration(sectionKey, calibrateButton));
        calibrationRow.append(explanation, calibrateButton);
        panel.appendChild(calibrationRow);
    }
    container.appendChild(panel);
}

function editorControl(id, labelText, helpText, control) {
    const group = document.createElement('div');
    group.className = 'cfg-group';
    const label = document.createElement('label');
    label.htmlFor = id;
    label.textContent = labelText;
    control.id = id;
    const help = document.createElement('span');
    help.className = 'cfg-help';
    help.textContent = helpText;
    group.append(label, control, help);
    return group;
}

function sourceEditorInput(type, value = '') {
    const input = document.createElement('input');
    input.type = type;
    if (type === 'checkbox') input.checked = Boolean(value);
    else input.value = value == null ? '' : String(value);
    return input;
}

function renderSourceEditor(sectionKey, existing = null) {
    const container = $('modal-form-container');
    if (!container) return;
    const contracts = sourceContracts[sectionKey] || [];
    if (!contracts.length) {
        showConfigStatus(`No custom source adapters are available for ${sectionKey}.`, 'err');
        return;
    }
    if ($('btn-save-cfg')) $('btn-save-cfg').disabled = true;
    if ($('btn-reset-cfg')) $('btn-reset-cfg').disabled = true;
    const initialContract = contracts.find(item => item.id === existing?.adapter) || contracts[0];
    container.replaceChildren();
    container.scrollTop = 0;

    const editor = document.createElement('section');
    editor.className = 'cfg-source-editor';
    const head = document.createElement('div');
    head.className = 'cfg-editor-head';
    const heading = document.createElement('div');
    heading.innerHTML = `<h3>${existing ? 'Edit' : 'Add'} ${escapeHtml(sectionKey)} source</h3><p>Configure a read-only JSON endpoint. API secrets are never entered here; provide only the name of an environment variable.</p>`;
    const cancel = document.createElement('button');
    cancel.type = 'button';
    cancel.className = 'modal-close';
    cancel.setAttribute('aria-label', 'Cancel source editor');
    cancel.textContent = '✕';
    cancel.addEventListener('click', () => renderModalTab(activeConfigTab));
    head.append(heading, cancel);
    editor.appendChild(head);

    const contractHelp = document.createElement('div');
    contractHelp.className = 'cfg-contract-help';
    editor.appendChild(contractHelp);
    const grid = document.createElement('div');
    grid.className = 'cfg-editor-grid';

    const adapter = document.createElement('select');
    contracts.forEach(contract => {
        const option = document.createElement('option');
        option.value = contract.id;
        option.textContent = contract.label;
        option.selected = contract.id === initialContract.id;
        adapter.appendChild(option);
    });
    grid.appendChild(editorControl('src-adapter', 'Data contract', 'Choose the shape this section expects from the endpoint.', adapter));

    const idInput = sourceEditorInput('text', existing?.id || '');
    idInput.placeholder = 'my_market_provider';
    idInput.disabled = Boolean(existing);
    grid.appendChild(editorControl('src-id', 'Source ID', 'Required: 3–40 lowercase letters, numbers, or underscores; must start with a letter.', idInput));
    grid.appendChild(editorControl('src-label', 'Display name', 'Required: a clear 2–80 character provider name.', sourceEditorInput('text', existing?.label || '')));
    grid.appendChild(editorControl('src-purpose', 'Purpose', 'Optional: explain what this endpoint contributes to the scan.', sourceEditorInput('text', existing?.purpose || '')));

    const endpoint = sourceEditorInput('url', existing?.endpoint || '');
    endpoint.placeholder = 'https://api.example.com/bars/{symbol}';
    grid.appendChild(editorControl('src-endpoint', 'GET endpoint', 'Required: full HTTPS/HTTP URL. Supported placeholders are listed above.', endpoint));

    const priority = sourceEditorInput('number', existing?.priority ?? 50);
    priority.min = '1'; priority.max = '999'; priority.step = '1';
    grid.appendChild(editorControl('src-priority', 'Priority', 'Required: 1–999; lower-numbered bar/quote/fundamental providers are attempted first.', priority));
    const timeout = sourceEditorInput('number', existing?.timeout_seconds ?? 12);
    timeout.min = '2'; timeout.max = '60'; timeout.step = '1';
    grid.appendChild(editorControl('src-timeout', 'Timeout seconds', 'Required: 2–60 seconds per request.', timeout));

    const auth = document.createElement('select');
    [['none', 'No authentication'], ['query', 'API key in query'], ['header', 'API key in header'], ['bearer', 'Bearer token']].forEach(([value, text]) => {
        const option = document.createElement('option'); option.value = value; option.textContent = text;
        option.selected = value === (existing?.auth_type || 'none'); auth.appendChild(option);
    });
    grid.appendChild(editorControl('src-auth-type', 'Authentication', 'Select how the endpoint receives its credential.', auth));
    const credential = sourceEditorInput('text', existing?.credential_env || '');
    credential.placeholder = 'MY_PROVIDER_API_KEY';
    grid.appendChild(editorControl('src-credential-env', 'Credential environment variable', 'For authenticated sources, enter the environment variable name—not the secret value.', credential));
    const authName = sourceEditorInput('text', existing?.auth_name || '');
    authName.placeholder = 'apikey or X-API-Key';
    grid.appendChild(editorControl('src-auth-name', 'Auth parameter/header name', 'Required only for query or header authentication; ignored for none/bearer.', authName));

    const enabled = sourceEditorInput('checkbox', existing?.enabled ?? true);
    grid.appendChild(editorControl('src-enabled', 'Enabled', 'When checked, the next scan can use this source immediately.', enabled));
    editor.appendChild(grid);

    const jsonGrid = document.createElement('div');
    jsonGrid.className = 'cfg-editor-json-grid';
    const query = document.createElement('textarea'); query.rows = 5;
    query.value = JSON.stringify(existing?.query_params || {}, null, 2);
    jsonGrid.appendChild(editorControl('src-query-params', 'Query parameters (JSON)', 'JSON object of parameter names to values. Values can contain the supported placeholders.', query));
    const headers = document.createElement('textarea'); headers.rows = 5;
    headers.value = JSON.stringify(existing?.static_headers || {}, null, 2);
    jsonGrid.appendChild(editorControl('src-static-headers', 'Static headers (JSON)', 'JSON object for non-secret headers such as Accept or API version.', headers));
    const mapping = document.createElement('textarea'); mapping.rows = 7;
    mapping.value = JSON.stringify(existing?.field_mapping || initialContract.default_mapping, null, 2);
    jsonGrid.appendChild(editorControl('src-field-mapping', 'Field mapping (JSON)', 'Keys are the required output names shown above. Values are dot paths inside each returned row.', mapping));
    const root = sourceEditorInput('text', existing?.root_path || '');
    root.placeholder = 'data.results';
    jsonGrid.appendChild(editorControl('src-root-path', 'Array/root JSON path', 'Dot path from the response to the rows. Leave blank when the response root is already the array/object.', root));
    editor.appendChild(jsonGrid);

    const result = document.createElement('pre');
    result.className = 'cfg-test-result';
    result.textContent = 'Test preview will appear here.';
    editor.appendChild(result);
    const actions = document.createElement('div');
    actions.className = 'cfg-editor-actions';
    const test = document.createElement('button'); test.type = 'button'; test.className = 'btn-secondary'; test.textContent = 'Test connection';
    const save = document.createElement('button'); save.type = 'button'; save.className = 'btn-primary'; save.textContent = 'Save source';
    test.addEventListener('click', async () => {
        try {
            const definition = collectSourceEditor();
            await testCustomSource(sectionKey, definition, result, test);
        } catch (err) {
            result.textContent = `Error: ${err.message}`;
            result.className = 'cfg-test-result is-error';
        }
    });
    save.addEventListener('click', () => saveCustomSource(sectionKey, save, result));
    actions.append(test, save);
    editor.appendChild(actions);
    container.appendChild(editor);

    function updateContractHelp(resetMapping = false) {
        const contract = contracts.find(item => item.id === adapter.value) || contracts[0];
        contractHelp.replaceChildren();
        const summary = document.createElement('strong'); summary.textContent = contract.description;
        const details = document.createElement('span');
        const placeholders = contract.context_variables.length ? contract.context_variables.map(name => `{${name}}`).join(', ') : 'none';
        details.textContent = `Required mappings: ${contract.required_mapping.join(', ')}. Optional: ${contract.optional_mapping.join(', ') || 'none'}. Supported placeholders: ${placeholders}. Test context: ${JSON.stringify(contract.sample_context)}.`;
        contractHelp.append(summary, details);
        if (resetMapping) mapping.value = JSON.stringify(contract.default_mapping, null, 2);
    }
    adapter.addEventListener('change', () => updateContractHelp(true));
    updateContractHelp(false);
}

function parseJsonObject(id, label) {
    const raw = $(id).value.trim();
    try {
        const value = raw ? JSON.parse(raw) : {};
        if (!value || Array.isArray(value) || typeof value !== 'object') throw new Error();
        return value;
    } catch {
        throw new Error(`${label} must be a valid JSON object`);
    }
}

function collectSourceEditor() {
    return {
        id: $('src-id').value.trim(),
        label: $('src-label').value.trim(),
        adapter: $('src-adapter').value,
        purpose: $('src-purpose').value.trim(),
        enabled: $('src-enabled').checked,
        priority: Number($('src-priority').value),
        method: 'GET',
        endpoint: $('src-endpoint').value.trim(),
        auth_type: $('src-auth-type').value,
        credential_env: $('src-credential-env').value.trim(),
        auth_name: $('src-auth-name').value.trim(),
        query_params: parseJsonObject('src-query-params', 'Query parameters'),
        static_headers: parseJsonObject('src-static-headers', 'Static headers'),
        root_path: $('src-root-path').value.trim(),
        field_mapping: parseJsonObject('src-field-mapping', 'Field mapping'),
        timeout_seconds: Number($('src-timeout').value),
    };
}

async function testCustomSource(sectionKey, definitionOrId, resultElement = null, button = null) {
    const definition = typeof definitionOrId === 'string'
        ? (sourcesConfigData[sectionKey].custom_sources || []).find(item => item.id === definitionOrId)
        : definitionOrId;
    if (!definition) throw new Error('Source definition was not found');
    if (button) button.disabled = true;
    showConfigStatus(`Testing ${definition.label || definition.id}…`);
    try {
        const res = await fetch('/api/sources/config/source/test', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ section: sectionKey, source: definition }),
        });
        if (!res.ok) throw new Error(await readApiError(res, 'Source test failed'));
        const data = await res.json();
        if (resultElement) {
            resultElement.textContent = JSON.stringify(data, null, 2);
            resultElement.className = 'cfg-test-result is-ok';
        }
        showConfigStatus(`✓ ${definition.label || definition.id}: ${data.records} valid record(s).`, 'ok');
        return data;
    } catch (err) {
        if (resultElement) {
            resultElement.textContent = `Error: ${err.message}`;
            resultElement.className = 'cfg-test-result is-error';
        }
        showConfigStatus(`✕ ${err.message}`, 'err');
        throw err;
    } finally {
        if (button) button.disabled = false;
    }
}

async function testSourceDefinition(sectionKey, sourceId) {
    try { await testCustomSource(sectionKey, sourceId); } catch { /* status is shown in the footer */ }
}

async function saveCustomSource(sectionKey, button, resultElement) {
    button.disabled = true;
    try {
        const source = collectSourceEditor();
        const res = await fetch('/api/sources/config/source', {
            method: 'PUT', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ section: sectionKey, source }),
        });
        if (!res.ok) throw new Error(await readApiError(res, 'Source save failed'));
        const data = await res.json();
        sourcesConfigData = data.config;
        sourcesConfigMeta = data.meta || sourcesConfigMeta;
        applyConfiguredSourceLabels();
        renderModalTab(activeConfigTab);
        showConfigStatus(`✓ ${source.label} saved and available to the next scan.`, 'ok');
    } catch (err) {
        resultElement.textContent = `Error: ${err.message}`;
        resultElement.className = 'cfg-test-result is-error';
        showConfigStatus(`✕ ${err.message}`, 'err');
    } finally {
        button.disabled = false;
    }
}

async function deleteCustomSource(sectionKey, sourceId) {
    if (!window.confirm(`Delete custom source “${sourceId}”?`)) return;
    try {
        const res = await fetch(`/api/sources/config/source/${encodeURIComponent(sectionKey)}/${encodeURIComponent(sourceId)}`, { method: 'DELETE' });
        if (!res.ok) throw new Error(await readApiError(res, 'Delete failed'));
        const data = await res.json();
        sourcesConfigData = data.config;
        sourcesConfigMeta = data.meta || sourcesConfigMeta;
        applyConfiguredSourceLabels();
        renderModalTab(activeConfigTab);
        showConfigStatus(`✓ ${sourceId} deleted.`, 'ok');
    } catch (err) {
        showConfigStatus(`✕ ${err.message}`, 'err');
    }
}

async function runScoreCalibration(sectionKey, button) {
    button.disabled = true;
    button.textContent = 'Saving settings…';
    try {
        const saved = await saveConfig(true);
        if (!saved) return;
        button.textContent = 'Calibrating…';
        showConfigStatus(`Fetching historical bars and validating ${sectionKey} weights…`);
        const res = await fetch(`/api/sources/config/calibrate/${encodeURIComponent(sectionKey)}`, { method: 'POST' });
        if (!res.ok) throw new Error(await readApiError(res, 'Calibration failed'));
        const data = await res.json();
        sourcesConfigData = data.config;
        sourcesConfigMeta = data.meta || sourcesConfigMeta;
        renderModalTab(activeConfigTab);
        showConfigStatus(data.applied
            ? '✓ Holdout checks passed; calibrated weights were applied.'
            : 'Calibration finished, but holdout checks failed; previous weights were kept.', data.applied ? 'ok' : 'err');
    } catch (err) {
        showConfigStatus(`✕ ${err.message}`, 'err');
    } finally {
        button.disabled = false;
        button.textContent = 'Calibrate & apply';
    }
}

function showConfigStatus(message, kind = '') {
    const statusEl = $('cfg-save-status');
    if (!statusEl) return;
    statusEl.textContent = message || '';
    statusEl.className = `save-status ${kind}`.trim();
}

async function readApiError(res, fallback) {
    try {
        const data = await res.json();
        return data.detail || fallback;
    } catch {
        return fallback;
    }
}

async function saveConfig(silent = false) {
    if (!silent) showConfigStatus('Saving configuration…');
    const saveButton = $('btn-save-cfg');
    if (saveButton) saveButton.disabled = true;

    try {
        const updatedPayload = {};
        document.querySelectorAll('#modal-form-container [data-section]').forEach(input => {
            const sec = input.dataset.section;
            const k = input.dataset.key;
            const type = input.dataset.type;
            if (!updatedPayload[sec]) updatedPayload[sec] = {};

            let val = input.value;
            if (type === 'list') {
                val = val.split(',').map(s => s.trim()).filter(Boolean);
            } else if (type === 'number' || type === 'integer') {
                if (val.trim() === '') throw new Error(`${k} requires a number`);
                val = Number(val);
                if (!Number.isFinite(val)) throw new Error(`${k} requires a valid number`);
                if (type === 'integer' && !Number.isInteger(val)) throw new Error(`${k} requires a whole number`);
            } else {
                val = val.trim();
            }
            updatedPayload[sec][k] = val;
        });

        const res = await fetch('/api/sources/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(updatedPayload)
        });
        if (!res.ok) throw new Error(await readApiError(res, 'Save failed'));
        const data = await res.json();
        sourcesConfigData = data.config;
        sourcesConfigMeta = data.meta || sourcesConfigMeta;
        applyConfiguredSourceLabels();
        renderModalTab(activeConfigTab);
        if (!silent) {
            showConfigStatus('✓ Saved. The next scan will use these values.', 'ok');
            setTimeout(() => {
                showConfigStatus('');
            }, 3000);
        }
        fetchState();
        return true;
    } catch (err) {
        showConfigStatus(`✕ ${err.message || 'Error saving configuration'}`, 'err');
        return false;
    } finally {
        if (saveButton) saveButton.disabled = false;
    }
}

async function resetCurrentConfigSection() {
    const section = configSectionByTab[activeConfigTab] || 'discovery';
    showConfigStatus(`Resetting ${section}…`);
    const button = $('btn-reset-cfg');
    if (button) button.disabled = true;
    try {
        const res = await fetch('/api/sources/config/reset', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ section })
        });
        if (!res.ok) throw new Error(await readApiError(res, 'Reset failed'));
        const data = await res.json();
        sourcesConfigData = data.config;
        sourcesConfigMeta = data.meta || sourcesConfigMeta;
        applyConfiguredSourceLabels();
        renderModalTab(activeConfigTab);
        showConfigStatus(`✓ ${section} reset to defaults.`, 'ok');
    } catch (err) {
        showConfigStatus(`✕ ${err.message || 'Reset failed'}`, 'err');
    } finally {
        if (button) button.disabled = false;
    }
}

function applyConfiguredSourceLabels() {
    if (!sourcesConfigData) return;
    const laneMap = {
        discovery: 'lane-discovery', market: 'lane-market', news: 'lane-news',
        social: 'lane-social', fundamentals: 'lane-sec'
    };
    Object.entries(laneMap).forEach(([sectionKey, laneId]) => {
        const section = sourcesConfigData[sectionKey];
        const lane = $(laneId);
        if (!section || !lane) return;
        const labels = (section.source_inventory || [])
            .filter(source => source.enabled)
            .map(source => source.label);
        const small = lane.querySelector('small');
        if (small) small.textContent = labels.join(' · ') || 'All sources disabled';
    });
}

// Modal Event Listeners
const btnConfig = $('btn-config');
if (btnConfig) btnConfig.addEventListener('click', openConfigModal);

const btnClose = $('modal-close');
if (btnClose) btnClose.addEventListener('click', closeConfigModal);

document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', (e) => {
        renderModalTab(e.target.dataset.tab);
    });
});

const btnSaveCfg = $('btn-save-cfg');
if (btnSaveCfg) btnSaveCfg.addEventListener('click', () => saveConfig());

const btnResetCfg = $('btn-reset-cfg');
if (btnResetCfg) btnResetCfg.addEventListener('click', resetCurrentConfigSection);

const configModal = $('config-modal');
if (configModal) {
    configModal.addEventListener('click', event => {
        if (event.target === configModal) closeConfigModal();
    });
}

document.addEventListener('keydown', event => {
    if (event.key === 'Escape') {
        if (configModal && configModal.style.display !== 'none') closeConfigModal();
        const drawer = $('intel-drawer');
        if (drawer && (drawer.style.display !== 'none' || drawer.classList.contains('is-open'))) {
            closeIntelDrawer();
        }
    }
});

// ── Intelligence Drawer ─────────────────────────────────────────

let _intelCurrentSymbol = null;
let _intelData = null;

function openIntelDrawer(symbol) {
    _intelCurrentSymbol = symbol;
    const drawer  = $('intel-drawer');
    const overlay = $('intel-overlay');
    if (!drawer || !overlay) return;

    $('intel-symbol').textContent = symbol;
    _setIntelSignal('neutral');
    $('intel-fetch-status').textContent = 'Fetching intelligence data…';
    $('intel-sec-link').href = `https://www.sec.gov/cgi-bin/browse-edgar?company=${encodeURIComponent(symbol)}&CIK=&type=4&dateb=&owner=include&count=40&search_text=&action=getcompany`;

    // Show loading in all panels
    ['insider', 'congress', 'actions', 'ownership', 'seasonal', 'valuation'].forEach(tab => {
        const panelId = 'itab-' + tab;
        const panel = $(panelId);
        if (panel) {
            panel.innerHTML = `<div class="intel-loading"><div class="intel-spinner"></div>Loading ${tab} data…</div>`;
        }
    });

    overlay.style.display = 'block';
    drawer.style.display  = 'flex';
    overlay.classList.add('is-open');
    drawer.classList.add('is-open');
    overlay.style.animation = 'none';
    drawer.style.animation  = 'none';
    requestAnimationFrame(() => {
        overlay.style.animation = 'fadeIn 0.2s ease';
        drawer.style.animation  = 'slideInRight 0.28s cubic-bezier(0.16, 1, 0.3, 1)';
    });

    // Re-initialise tab state
    document.querySelectorAll('.intel-tab').forEach(t => t.classList.remove('is-active'));
    const firstTab = document.querySelector('.intel-tab[data-itab="insider"]');
    if (firstTab) firstTab.classList.add('is-active');

    fetchIntelligence(symbol);
}

function closeIntelDrawer() {
    const drawer  = $('intel-drawer');
    const overlay = $('intel-overlay');
    if (drawer) {
        drawer.style.display = 'none';
        drawer.classList.remove('is-open');
    }
    if (overlay) {
        overlay.style.display = 'none';
        overlay.classList.remove('is-open');
    }
    _intelCurrentSymbol = null;
    _intelData = null;
}

function _setIntelSignal(signal) {
    const badge = $('intel-signal');
    if (!badge) return;
    badge.textContent = signal;
    badge.className = 'intel-signal-badge ' + (signal || 'neutral');
}

async function fetchIntelligence(symbol) {
    try {
        const res = await fetch(`/api/intelligence/${encodeURIComponent(symbol)}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        _intelData = data;
        renderIntelligence(data);
        $('intel-fetch-status').textContent = `Last fetched: ${new Date().toLocaleTimeString()}`;
        if (data.insider?.source_url) {
            $('intel-sec-link').href = data.insider.source_url;
        }
    } catch (err) {
        $('intel-fetch-status').textContent = `Error: ${err.message}`;
        ['insider', 'congress', 'actions', 'ownership', 'seasonal', 'valuation'].forEach(tab => {
            const panel = $('itab-' + tab);
            if (panel) panel.innerHTML = `<div class="intel-empty">⚠ Failed to load data: ${escapeHtml(err.message)}</div>`;
        });
    }
}

function renderIntelligence(data) {
    // Composite signal badge
    _setIntelSignal(data.composite_signal || 'neutral');

    // Rebuild panels with actual content
    renderInsiderPanel(data.insider);
    renderCongressPanel(data.congress);
    renderActionsPanel(data.corporate_actions);
    renderOwnershipPanel(data.ownership);
    renderSeasonalPanel(data.seasonality);
    renderValuationPanel(data.valuation);
}

// ── Insider Trades Panel ────────────────────────────────────────

function renderInsiderPanel(d) {
    const panel = $('itab-insider');
    if (!panel) return;
    panel.innerHTML = '';

    if (!d || d.error) {
        panel.innerHTML = `<div class="intel-empty">⚠ ${escapeHtml(d?.error || 'No insider data')}</div>`;
        return;
    }

    // Source bar
    const srcBar = document.createElement('div');
    srcBar.className = 'intel-source-bar';
    srcBar.innerHTML = `📡 Source: <strong>${escapeHtml(d.source)}</strong> · <a href="${escapeHtml(d.source_url || '#')}" target="_blank" rel="noopener">View filings →</a>`;
    panel.appendChild(srcBar);

    // Summary
    const summary = document.createElement('div');
    summary.className = 'intel-summary';
    summary.textContent = d.summary || 'No summary available.';
    panel.appendChild(summary);

    // Table
    const tableWrap = document.createElement('div');
    tableWrap.className = 'intel-table-wrap';
    if (!d.trades || !d.trades.length) {
        tableWrap.innerHTML = '<div class="intel-empty">No Form 4 insider trades found in the last 180 days.</div>';
    } else {
        const table = document.createElement('table');
        table.className = 'intel-table';
        table.innerHTML = `<thead><tr><th>Person</th><th>Title</th><th>Type</th><th>Shares</th><th>Price</th><th>Value</th><th>Date</th></tr></thead>`;
        const tbody = document.createElement('tbody');
        d.trades.forEach(t => {
            const tr = document.createElement('tr');
            tr.title = t.reason || '';
            const txClass = { Buy: 'tx-buy', Sell: 'tx-sell', Award: 'tx-award' }[t.transaction_type] || 'tx-other';
            tr.innerHTML = `
                <td>${escapeHtml(t.person)}</td>
                <td style="color:var(--muted);font-size:0.72rem">${escapeHtml(t.title || '—')}</td>
                <td><span class="${txClass}">${escapeHtml(t.transaction_type)}</span></td>
                <td>${escapeHtml(String(t.shares))}</td>
                <td>${t.price && t.price !== '?' ? '$' + escapeHtml(String(t.price)) : '—'}</td>
                <td>${escapeHtml(t.total_value || '—')}</td>
                <td style="white-space:nowrap">${escapeHtml(t.date)}</td>
            `;
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);
        tableWrap.appendChild(table);
    }
    panel.appendChild(tableWrap);
}

// ── Congress Trades Panel ────────────────────────────────────────

function renderCongressPanel(d) {
    const panel = $('itab-congress');
    if (!panel) return;
    panel.innerHTML = '';

    if (!d || d.error) {
        panel.innerHTML = `<div class="intel-empty">⚠ ${escapeHtml(d?.error || 'No congress data')}</div>`;
        return;
    }

    const srcBar = document.createElement('div');
    srcBar.className = 'intel-source-bar';
    srcBar.innerHTML = `📡 Source: <strong>${escapeHtml(d.source)}</strong> · <a href="${escapeHtml(d.source_url || '#')}" target="_blank" rel="noopener">View on Capitol Trades →</a>`;
    panel.appendChild(srcBar);

    if (d.note) {
        const note = document.createElement('div');
        note.className = 'intel-note';
        note.textContent = '⏱ ' + d.note;
        panel.appendChild(note);
    }

    const summary = document.createElement('div');
    summary.className = 'intel-summary';
    summary.textContent = d.summary || '';
    panel.appendChild(summary);

    const tableWrap = document.createElement('div');
    tableWrap.className = 'intel-table-wrap';
    if (!d.trades || !d.trades.length) {
        tableWrap.innerHTML = '<div class="intel-empty">No congressional trades recorded for this ticker in the STOCK Act disclosure database.</div>';
    } else {
        const table = document.createElement('table');
        table.className = 'intel-table';
        table.innerHTML = `<thead><tr><th>Member / Filer</th><th>Party/State</th><th>Type</th><th>Amount Range</th><th>Traded</th><th>Disclosed</th></tr></thead>`;
        const tbody = document.createElement('tbody');
        d.trades.forEach(t => {
            const tr = document.createElement('tr');
            tr.title = t.reason || '';
            const isBuy = /buy|purchase/i.test(t.transaction_type);
            const txClass = isBuy ? 'tx-buy' : 'tx-sell';
            const memberHtml = t.doc_url
                ? `<a href="${escapeHtml(t.doc_url)}" target="_blank" rel="noopener" style="color:var(--text);text-decoration:none;font-weight:600">${escapeHtml(t.person)} <span style="font-size:0.65rem;color:var(--blue)">↗</span></a>`
                : `<strong>${escapeHtml(t.person)}</strong>`;

            tr.innerHTML = `
                <td>${memberHtml}</td>
                <td style="color:var(--muted);font-weight:500">${escapeHtml(t.party_state)}</td>
                <td><span class="${txClass}">${escapeHtml(t.transaction_type)}</span></td>
                <td style="font-weight:600">${escapeHtml(t.amount_range)}</td>
                <td style="white-space:nowrap">${escapeHtml(t.traded_on)}</td>
                <td style="white-space:nowrap;color:var(--dim)">${escapeHtml(t.disclosed_on)}</td>
            `;
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);
        tableWrap.appendChild(table);
    }
    panel.appendChild(tableWrap);
}

// ── Corporate Actions Panel ─────────────────────────────────────

function renderActionsPanel(d) {
    const panel = $('itab-actions');
    if (!panel) return;
    panel.innerHTML = '';

    if (!d || d.error) {
        panel.innerHTML = `<div class="intel-empty">⚠ ${escapeHtml(d?.error || 'No actions data')}</div>`;
        return;
    }

    const srcBar = document.createElement('div');
    srcBar.className = 'intel-source-bar';
    srcBar.innerHTML = `📡 Source: <strong>${escapeHtml(d.source)}</strong> · <a href="${escapeHtml(d.source_url || '#')}" target="_blank" rel="noopener">View SEC Filings →</a>`;
    panel.appendChild(srcBar);

    const summary = document.createElement('div');
    summary.className = 'intel-summary';
    summary.textContent = d.summary || '';
    panel.appendChild(summary);

    const timeline = document.createElement('div');
    timeline.className = 'intel-timeline';

    if (!d.actions || !d.actions.length) {
        timeline.innerHTML = '<div class="intel-empty">No material corporate actions recorded in the last 12 months.</div>';
    } else {
        const dotClassMap = {
            'Earnings Results': 'dot-earnings',
            'M&A / Material Agreement': 'dot-ma',
            'Dividend': 'dot-dividend',
            'Stock Split': 'dot-split',
            'Executive Changes': 'dot-exec',
        };
        d.actions.forEach(a => {
            const item = document.createElement('div');
            item.className = 'timeline-item';
            item.title = a.reason || '';
            const dotClass = dotClassMap[a.type] || 'dot-other';
            const titleHtml = a.url
                ? `<a href="${escapeHtml(a.url)}" target="_blank" rel="noopener" style="color:var(--text);text-decoration:none">${escapeHtml(a.type)} <span style="font-size:0.65rem;color:var(--blue)">↗</span></a>`
                : escapeHtml(a.type);

            item.innerHTML = `
                <div class="timeline-dot ${dotClass}"></div>
                <div class="timeline-content">
                    <div class="timeline-type">${titleHtml}</div>
                    <div class="timeline-date">${escapeHtml(a.date)}</div>
                    ${a.items ? `<div class="timeline-items-label">${escapeHtml(a.items)}</div>` : ''}
                </div>
                <span class="timeline-source-badge">${escapeHtml(a.source)}</span>
            `;
            timeline.appendChild(item);
        });
    }
    panel.appendChild(timeline);
}

// ── Ownership Panel ─────────────────────────────────────────────

function renderOwnershipPanel(d) {
    const panel = $('itab-ownership');
    if (!panel) return;
    panel.innerHTML = '';

    if (!d || d.error) {
        panel.innerHTML = `<div class="intel-empty">⚠ ${escapeHtml(d?.error || 'No ownership data')}</div>`;
        return;
    }

    const srcBar = document.createElement('div');
    srcBar.className = 'intel-source-bar';
    const instStr = d.institutional_ownership_pct ? `Institutional: <strong>${escapeHtml(d.institutional_ownership_pct)}</strong>` : `Institutional: <strong>${(d.total_institutional_pct || 0).toFixed(1)}%</strong>`;
    const insStr = d.insider_ownership_pct ? ` · Insider: <strong>${escapeHtml(d.insider_ownership_pct)}</strong>` : '';
    srcBar.innerHTML = `📡 Source: <strong>${escapeHtml(d.source)}</strong> · ${instStr}${insStr}`;
    panel.appendChild(srcBar);

    const summary = document.createElement('div');
    summary.className = 'intel-summary';
    summary.textContent = d.summary || '';
    panel.appendChild(summary);

    const tableWrap = document.createElement('div');
    tableWrap.className = 'intel-table-wrap';

    if (!d.owners || !d.owners.length) {
        tableWrap.innerHTML = '<div class="intel-empty">No institutional ownership filings found for this ticker.</div>';
    } else {
        const table = document.createElement('table');
        table.className = 'intel-table';
        table.innerHTML = `<thead><tr><th>Institutional Holder / Fund</th><th>Filing Type</th><th>Filing Date</th><th>Period</th></tr></thead>`;
        const tbody = document.createElement('tbody');
        d.owners.forEach(o => {
            const tr = document.createElement('tr');
            tr.title = o.reason || '';
            const ftype = o.percent && o.percent > 0 ? `${o.percent.toFixed(2)}%` : (o.filing_type || '13F-HR');
            const fdate = o.filing_date || 'Recent';
            const fperiod = o.period_ending || '—';

            tr.innerHTML = `
                <td><strong>${escapeHtml(o.name)}</strong></td>
                <td><span style="color:var(--blue);font-weight:600">${escapeHtml(ftype)}</span></td>
                <td style="color:var(--text);font-size:0.75rem">${escapeHtml(fdate)}</td>
                <td style="color:var(--muted);font-size:0.75rem">${escapeHtml(fperiod)}</td>
            `;
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);
        tableWrap.appendChild(table);
    }
    panel.appendChild(tableWrap);
}

// ── Seasonality Panel ───────────────────────────────────────────

function renderSeasonalPanel(d) {
    const panel = $('itab-seasonal');
    if (!panel) return;
    panel.innerHTML = '';

    if (!d || d.error) {
        panel.innerHTML = `<div class="intel-empty">⚠ ${escapeHtml(d?.error || 'No seasonality data')}</div>`;
        return;
    }

    const srcBar = document.createElement('div');
    srcBar.className = 'intel-source-bar';
    srcBar.innerHTML = `📡 Source: <strong>${escapeHtml(d.source)}</strong>`;
    panel.appendChild(srcBar);

    const summary = document.createElement('div');
    summary.className = 'intel-summary';
    summary.textContent = d.summary || '';
    panel.appendChild(summary);

    const wrap = document.createElement('div');
    wrap.className = 'intel-season-wrap';

    const barsEl = document.createElement('div');
    barsEl.className = 'season-bars';

    const months = d.months || [];
    if (!months.length) {
        wrap.innerHTML = '<div class="intel-empty">No monthly data available.</div>';
    } else {
        const maxAbs = Math.max(...months.map(m => Math.abs(m.avg_return_pct || 0)), 0.1);
        const chartH = 180; // usable pixel height per direction
        months.forEach(m => {
            const col = document.createElement('div');
            col.className = 'season-bar-col';
            col.title = m.reason || m.month;

            const val = m.avg_return_pct || 0;
            const bar = document.createElement('div');
            const heightPx = Math.max(3, Math.round((Math.abs(val) / maxAbs) * chartH));
            bar.className = 'season-bar ' + (val >= 0 ? 'pos' : 'neg');
            bar.style.height = heightPx + 'px';

            const valLabel = document.createElement('span');
            valLabel.className = 'season-val';
            valLabel.textContent = (val >= 0 ? '+' : '') + val.toFixed(1) + '%';

            const label = document.createElement('span');
            label.className = 'season-label';
            label.textContent = m.month;

            col.append(valLabel, bar, label);
            barsEl.appendChild(col);
        });
        wrap.appendChild(barsEl);

        const legend = document.createElement('div');
        legend.className = 'season-legend';
        legend.innerHTML = `<span class="leg-pos">▲ Positive avg return</span><span class="leg-neg">▼ Negative avg return</span>`;
        wrap.appendChild(legend);

        // Best / Worst callouts
        if (d.best_month || d.worst_month) {
            const callouts = document.createElement('div');
            callouts.style.cssText = 'display:flex;gap:10px;margin-top:4px;';
            if (d.best_month) {
                callouts.innerHTML += `<div style="flex:1;padding:8px 10px;border-radius:8px;background:var(--green-bg);border:1px solid var(--green);font-size:0.75rem"><strong style="color:var(--green)">Best: ${escapeHtml(d.best_month.month)}</strong><br>${escapeHtml((d.best_month.avg_return_pct >= 0 ? '+' : '') + d.best_month.avg_return_pct.toFixed(2))}% avg</div>`;
            }
            if (d.worst_month) {
                callouts.innerHTML += `<div style="flex:1;padding:8px 10px;border-radius:8px;background:var(--red-bg);border:1px solid var(--red);font-size:0.75rem"><strong style="color:var(--red)">Worst: ${escapeHtml(d.worst_month.month)}</strong><br>${escapeHtml((d.worst_month.avg_return_pct >= 0 ? '+' : '') + d.worst_month.avg_return_pct.toFixed(2))}% avg</div>`;
            }
            wrap.appendChild(callouts);
        }
    }

    panel.appendChild(wrap);
}

// ── Valuation & Analyst Targets Panel ───────────────────────────

function renderValuationPanel(d) {
    const panel = $('itab-valuation');
    if (!panel) return;
    panel.innerHTML = '';

    if (!d || d.error) {
        panel.innerHTML = `<div class="intel-empty">⚠ ${escapeHtml(d?.error || 'No valuation data available')}</div>`;
        return;
    }

    // Source bar
    const srcBar = document.createElement('div');
    srcBar.className = 'intel-source-bar';
    srcBar.innerHTML = `📡 Source: <strong>${escapeHtml(d.source)}</strong> · <a href="${escapeHtml(d.source_url || '#')}" target="_blank" rel="noopener">Open Finviz Profile →</a>`;
    panel.appendChild(srcBar);

    // Summary
    const summary = document.createElement('div');
    summary.className = 'intel-summary';
    summary.textContent = d.summary || '';
    panel.appendChild(summary);

    // Hero: Target Price & Upside
    const hero = document.createElement('div');
    hero.className = 'intel-target-hero';

    const curPriceStr = d.price != null ? `$${Number(d.price).toFixed(2)}` : '—';
    const targetPriceStr = d.target_price != null ? `$${Number(d.target_price).toFixed(2)}` : '—';
    const upside = d.target_upside_pct;
    const upsideBadgeClass = upside != null ? (upside > 0 ? 'pos' : (upside < 0 ? 'neg' : 'neutral')) : 'neutral';
    const upsideText = upside != null ? `${upside >= 0 ? '+' : ''}${upside.toFixed(1)}% upside` : 'No Target';

    hero.innerHTML = `
        <div class="target-hero-left">
            <span class="target-hero-title">Current Price vs Mean Analyst Target</span>
            <div class="target-hero-prices">
                <span class="target-hero-cur">${escapeHtml(curPriceStr)}</span>
                <span style="color:var(--muted);font-size:0.9rem">➔</span>
                <span class="target-hero-target">${escapeHtml(targetPriceStr)}</span>
            </div>
        </div>
        <div class="target-hero-right">
            <span class="target-upside-badge ${upsideBadgeClass}">${escapeHtml(upsideText)}</span>
            <span class="recom-pill">Consensus: ${escapeHtml(d.recommendation_label || 'Buy')}</span>
        </div>
    `;
    panel.appendChild(hero);

    // ── Earnings Date Alert Banner ──────────────────────────────
    if (d.earnings_date) {
        const daysAway = d.earnings_days_away;
        const timing = d.earnings_timing || '';
        const timingLabel = timing === 'AMC' ? 'After Market Close' : timing === 'BMO' ? 'Before Market Open' : '';
        let alertClass = 'earnings-alert-safe';
        let alertIcon = '📅';
        let alertMsg = `Next Earnings: ${escapeHtml(d.earnings_date)}`;
        if (timingLabel) alertMsg += ` (${timingLabel})`;

        if (daysAway != null && daysAway >= 0 && daysAway <= 7) {
            alertClass = 'earnings-alert-danger';
            alertIcon = '⚠️';
            alertMsg += ` — <strong>${daysAway}d away!</strong> IV Crush risk: option premiums may collapse 40–60% post-earnings`;
        } else if (daysAway != null && daysAway >= 8 && daysAway <= 14) {
            alertClass = 'earnings-alert-warn';
            alertIcon = '🔶';
            alertMsg += ` — <strong>${daysAway}d away</strong> — monitor IV levels before entry`;
        } else if (daysAway != null) {
            alertMsg += ` — ${daysAway}d away (safe window)`;
        }

        const earningsEl = document.createElement('div');
        earningsEl.className = `earnings-alert ${alertClass}`;
        earningsEl.innerHTML = `<span class="earnings-alert-icon">${alertIcon}</span> ${alertMsg}`;
        panel.appendChild(earningsEl);
    }

    // Multiples & Short Float Grid
    const grid = document.createElement('div');
    grid.className = 'intel-val-grid';

    const cards = [
        { label: 'Short Float %', val: d.short_float_pct || '—', sub: d.squeeze_risk || 'Normal' },
        { label: 'Short Ratio (DTC)', val: d.short_ratio != null ? `${d.short_ratio}d` : '—', sub: 'Days to cover' },
        { label: 'Forward P/E', val: d.forward_pe != null ? String(d.forward_pe) : '—', sub: 'Next 12M' },
        { label: 'Trailing P/E', val: d.pe != null ? String(d.pe) : '—', sub: 'Historical' },
        { label: 'PEG Ratio', val: d.peg != null ? String(d.peg) : '—', sub: 'Growth-adjusted' },
        { label: 'Profit Margin', val: d.profit_margin || '—', sub: 'Net margin' },
        { label: 'Debt / Equity', val: d.debt_to_equity || '—', sub: 'Leverage' },
        { label: 'Market Cap', val: d.market_cap || '—', sub: 'Total size' },
    ];

    cards.forEach(c => {
        const card = document.createElement('div');
        card.className = 'val-card';
        card.innerHTML = `
            <span class="val-card-label">${escapeHtml(c.label)}</span>
            <span class="val-card-value">${escapeHtml(c.val)}</span>
            <span class="val-card-sub">${escapeHtml(c.sub)}</span>
        `;
        grid.appendChild(card);
    });
    panel.appendChild(grid);

    // ── Analyst Actions Ledger ──────────────────────────────────
    const actions = d.analyst_actions || [];
    if (actions.length > 0) {
        const actionsWrap = document.createElement('div');
        actionsWrap.className = 'analyst-actions-section';

        const upgrades = d.recent_upgrades || 0;
        const downgrades = d.recent_downgrades || 0;
        let momentumBadge = '';
        if (upgrades > downgrades && upgrades >= 2) {
            momentumBadge = `<span class="analyst-momentum-badge bullish">🟢 ${upgrades} Upgrades</span>`;
        } else if (downgrades > upgrades && downgrades >= 2) {
            momentumBadge = `<span class="analyst-momentum-badge bearish">🔴 ${downgrades} Downgrades</span>`;
        }

        let rows = '';
        actions.slice(0, 6).forEach(a => {
            const actionClass = (a.action === 'Upgrade' || a.action === 'Initiated') ? 'action-upgrade' :
                                a.action === 'Downgrade' ? 'action-downgrade' : 'action-neutral';
            rows += `<tr>
                <td>${escapeHtml(a.date)}</td>
                <td><span class="analyst-action-badge ${actionClass}">${escapeHtml(a.action)}</span></td>
                <td>${escapeHtml(a.firm)}</td>
                <td>${escapeHtml(a.rating_change)}</td>
                <td>${escapeHtml(a.target || '—')}</td>
            </tr>`;
        });

        actionsWrap.innerHTML = `
            <div class="analyst-actions-header">
                <strong>📊 Wall Street Analyst Actions</strong>
                ${momentumBadge}
            </div>
            <table class="analyst-actions-table">
                <thead><tr><th>Date</th><th>Action</th><th>Firm</th><th>Rating</th><th>Target</th></tr></thead>
                <tbody>${rows}</tbody>
            </table>
        `;
        panel.appendChild(actionsWrap);
    }

    // Finviz Interactive Chart Box
    const chartBox = document.createElement('div');
    chartBox.className = 'intel-chart-box';
    chartBox.innerHTML = `
        <div class="chart-box-info">
            <strong>Finviz Technical Chart</strong><br>
            <span>Automated trendlines, support & resistance levels for ${escapeHtml(d.symbol)}</span>
        </div>
        <a href="${escapeHtml(d.source_url || '#')}" target="_blank" rel="noopener" class="btn-finviz-chart">
            📈 View Chart & Levels ↗
        </a>
    `;
    panel.appendChild(chartBox);
}

// ── Intel Tab Switching ─────────────────────────────────────────

document.querySelectorAll('.intel-tab').forEach(tab => {
    tab.addEventListener('click', () => {
        const targetTab = tab.dataset.itab;
        document.querySelectorAll('.intel-tab').forEach(t => t.classList.remove('is-active'));
        tab.classList.add('is-active');
        document.querySelectorAll('.intel-panel').forEach(p => p.classList.remove('is-active'));
        const targetPanel = $('itab-' + targetTab);
        if (targetPanel) targetPanel.classList.add('is-active');
    });
});

// ── Close handlers ──────────────────────────────────────────────

const intelClose   = $('intel-close');
const intelOverlay = $('intel-overlay');
if (intelClose)   intelClose.addEventListener('click', closeIntelDrawer);
if (intelOverlay) intelOverlay.addEventListener('click', closeIntelDrawer);

// ── Wire up watchlist row clicks (called after render) ──────────
// Patched into renderTrader via event delegation on #watchlist
const wlEl = $('watchlist');
if (wlEl) {
    wlEl.addEventListener('click', event => {
        const row = event.target.closest('.wl-row');
        if (!row) return;
        const tickerEl = row.querySelector('.wl-ticker');
        if (tickerEl) openIntelDrawer(tickerEl.textContent.trim());
    });
}

// ── Boot ───────────────────────────────────────────────────────
fetchState();
fetchSourcesConfig();
connectOptionStream();
setInterval(fetchState, 4000);

