/**
 * AGY-SOC // Notification Priority Reducer [Enterprise SOC Frontend Client]
 * 
 * Features:
 * 1. Java NIO RFC 6455 WebSocket client with auto-reconnection and low-latency streaming
 * 2. Real-Time 60-Second Scrolling Sparkline Area Canvas (Double-buffered high-DPI rendering)
 * 3. Waterfall Terminal Log Stream with filtering and auto-scroll pinning
 * 4. Priority Matrix with CSS FLIP (First-Last-Invert-Play) smooth row glide animations
 * 5. 500ms Magenta Border Pulse on O(1) deduplication frequency increments
 * 6. Synthesized Web Audio API tactical incident beeps (zero external audio files)
 */

(() => {
  'use strict';

  // State Management
  const state = {
    ws: null,
    isConnected: false,
    autoScroll: true,
    soundEnabled: true,
    filterQuery: '',
    historyData: [], // 60 seconds rolling metrics buffer: { ts, rawRate, reducedRate }
    previousAlertsMap: new Map(), // key -> { frequency, rank }
    audioCtx: null
  };

  // DOM Elements
  const el = {
    wsStatus: document.getElementById('ws-status'),
    wsStatusText: document.getElementById('ws-status-text'),
    clock: document.getElementById('soc-clock'),
    rateSlider: document.getElementById('rate-slider'),
    rateVal: document.getElementById('rate-val'),
    btnP0: document.getElementById('btn-p0'),
    btnWhatsapp: document.getElementById('btn-whatsapp'),
    btnPause: document.getElementById('btn-pause'),
    btnClear: document.getElementById('btn-clear'),
    btnSound: document.getElementById('btn-sound'),
    btnAutoScroll: document.getElementById('btn-autoscroll'),
    btnClearLogs: document.getElementById('btn-clear-logs'),
    filterInput: document.getElementById('filter-input'),
    hudRawRate: document.getElementById('hud-raw-rate'),
    hudDedupRatio: document.getElementById('hud-dedup-ratio'),
    hudReducedRate: document.getElementById('hud-reduced-rate'),
    hudHeapSize: document.getElementById('hud-heap-size'),
    statTotalRaw: document.getElementById('stat-total-raw'),
    statTotalDedup: document.getElementById('stat-total-dedup'),
    statActiveNodes: document.getElementById('stat-active-nodes'),
    statHeapDepth: document.getElementById('stat-heap-depth'),
    statVirtualThreads: document.getElementById('stat-virtual-threads'),
    waterfall: document.getElementById('waterfall-container'),
    matrixContainer: document.getElementById('matrix-container'),
    canvas: document.getElementById('telemetry-canvas'),
    latencyText: document.getElementById('ws-latency')
  };

  // ==========================================================================
  // Web Audio Tactical Beep Synthesizer (P0 Critical Alert Acoustic Notice)
  // ==========================================================================
  function initAudio() {
    if (!state.audioCtx) {
      const AudioContext = window.AudioContext || window.webkitAudioContext;
      if (AudioContext) state.audioCtx = new AudioContext();
    }
  }

  function playTacticalAlertSound(freq = 880, duration = 0.08, type = 'sawtooth') {
    if (!state.soundEnabled || !state.audioCtx) return;
    try {
      const osc = state.audioCtx.createOscillator();
      const gain = state.audioCtx.createGain();
      osc.type = type;
      osc.frequency.setValueAtTime(freq, state.audioCtx.currentTime);
      gain.gain.setValueAtTime(0.04, state.audioCtx.currentTime);
      gain.gain.exponentialRampToValueAtTime(0.0001, state.audioCtx.currentTime + duration);
      osc.connect(gain);
      gain.connect(state.audioCtx.destination);
      osc.start();
      osc.stop(state.audioCtx.currentTime + duration);
    } catch (_) {}
  }

  // ==========================================================================
  // WebSocket Connection Management
  // ==========================================================================
  function connectWebSocket() {
    const loc = window.location;
    let wsHost = loc.host;
    if (!wsHost || wsHost.includes('null') || loc.protocol === 'file:') {
      wsHost = 'localhost:8080';
    }
    const wsUrl = `ws://${wsHost}/ws`;

    el.wsStatusText.textContent = `CONNECTING TO ${wsUrl}...`;
    el.wsStatus.querySelector('.status-indicator').className = 'status-indicator offline';

    try {
      state.ws = new WebSocket(wsUrl);

      state.ws.onopen = () => {
        state.isConnected = true;
        el.wsStatusText.textContent = 'STREAM ONLINE (50Hz NIO)';
        el.wsStatus.querySelector('.status-indicator').className = 'status-indicator online';
      };

      state.ws.onmessage = (event) => {
        handleIncomingTelemetry(event.data);
      };

      state.ws.onclose = () => {
        state.isConnected = false;
        el.wsStatusText.textContent = 'OFFLINE: RETRYING IN 2s...';
        el.wsStatus.querySelector('.status-indicator').className = 'status-indicator offline';
        setTimeout(connectWebSocket, 2000);
      };

      state.ws.onerror = () => {
        if (state.ws) state.ws.close();
      };
    } catch (e) {
      setTimeout(connectWebSocket, 2000);
    }
  }

  function sendCommand(cmdObj) {
    if (state.ws && state.ws.readyState === WebSocket.OPEN) {
      state.ws.send(JSON.stringify(cmdObj));
    }
  }

  // ==========================================================================
  // Telemetry Dispatcher & Data Processing
  // ==========================================================================
  let lastTickTime = performance.now();

  function handleIncomingTelemetry(rawText) {
    let packet;
    try {
      packet = JSON.parse(rawText);
    } catch (err) {
      return;
    }

    if (packet.type !== 'TELEMETRY_TICK') return;

    const now = performance.now();
    const latency = Math.max(1, Math.round(now - lastTickTime));
    lastTickTime = now;
    el.latencyText.textContent = `NIO INTERVAL: ${latency} ms`;

    const m = packet.metrics;

    // 1. Update HUD Digital Metric Cards
    el.hudRawRate.innerHTML = `${formatNumber(m.rawRate)} <span class="unit">msg/s</span>`;
    el.hudDedupRatio.innerHTML = `${m.dedupEfficiency} <span class="unit">%</span>`;
    el.hudReducedRate.innerHTML = `${formatNumber(m.reducedRate)} <span class="unit">msg/s</span>`;
    el.hudHeapSize.innerHTML = `${m.heapDepth} <span class="unit">alerts</span>`;

    // 2. Update Strip Statistics
    el.statTotalRaw.textContent = formatNumber(m.totalRaw);
    el.statTotalDedup.textContent = formatNumber(m.totalDuplicates);
    el.statActiveNodes.textContent = `${m.activeNodes} nodes`;
    el.statHeapDepth.textContent = `${m.heapDepth} nodes`;
    el.statVirtualThreads.textContent = `${m.virtualThreads} Loom tasks`;

    // Pause button state indicator
    if (m.isPaused) {
      el.btnPause.innerHTML = '<span class="btn-icon">▶</span> RESUME';
      el.btnPause.classList.add('btn-crimson');
    } else {
      el.btnPause.innerHTML = '<span class="btn-icon">⏸</span> PAUSE';
      el.btnPause.classList.remove('btn-crimson');
    }

    // 3. Append to Sparkline 60-Second Buffer
    state.historyData.push({
      ts: packet.timestamp,
      rawRate: m.rawRate,
      reducedRate: m.reducedRate
    });

    // Retain past 60 data points (assuming 1 per second or smooth sample)
    const cutoff = packet.timestamp - 60000;
    while (state.historyData.length > 0 && state.historyData[0].ts < cutoff) {
      state.historyData.shift();
    }

    // 4. Render Waterfall Log Entries
    if (packet.recentRawLogs && packet.recentRawLogs.length > 0) {
      renderWaterfallLogs(packet.recentRawLogs);
    }

    // 5. Render Reduced Priority Matrix with CSS FLIP Animations
    if (packet.topAlerts) {
      renderPriorityMatrixWithFLIP(packet.topAlerts);
    }
  }

  // ==========================================================================
  // Waterfall Terminal Feed
  // ==========================================================================
  function renderWaterfallLogs(logs) {
    const fragment = document.createDocumentFragment();
    const query = state.filterQuery.toLowerCase();

    // logs are newest first
    for (let i = 0; i < logs.length; i++) {
      const log = logs[i];
      if (query && !matchesFilter(log, query)) continue;

      const row = document.createElement('div');
      row.className = 'log-entry';

      const d = new Date(log.timestamp);
      const timeStr = `${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}.${pad3(d.getMilliseconds())}`;

      let rClass = 'routing-low';
      if (log.routingKey.startsWith('sec.') || log.routingKey.startsWith('sys.kernel.')) rClass = 'routing-crit';
      else if (log.routingKey.startsWith('infra.') || log.routingKey.startsWith('sys.disk.')) rClass = 'routing-high';
      else if (log.routingKey.startsWith('comm.slack.') || log.routingKey.startsWith('sys.cpu.')) rClass = 'routing-med';

      row.innerHTML = `
        <span class="log-ts">${timeStr}</span>
        <span class="log-source">${escapeHtml(log.source)}</span>
        <span class="log-routing ${rClass}">${escapeHtml(log.routingKey)}</span>
        <span class="log-payload">${escapeHtml(log.title)} // ${escapeHtml(log.payload)}</span>
      `;
      fragment.appendChild(row);
    }

    el.waterfall.innerHTML = '';
    el.waterfall.appendChild(fragment);

    if (state.autoScroll) {
      el.waterfall.scrollTop = 0;
    }
  }

  function matchesFilter(log, q) {
    return (log.routingKey && log.routingKey.toLowerCase().includes(q)) ||
           (log.source && log.source.toLowerCase().includes(q)) ||
           (log.title && log.title.toLowerCase().includes(q)) ||
           (log.payload && log.payload.toLowerCase().includes(q));
  }

  // ==========================================================================
  // Priority Matrix: CSS FLIP (First-Last-Invert-Play) Animation Engine
  // ==========================================================================
  function renderPriorityMatrixWithFLIP(alerts) {
    const container = el.matrixContainer;

    // STEP 1: FIRST
    // Record top bounding coordinate of all existing alert elements
    const firstPositions = new Map();
    const existingElements = container.querySelectorAll('.alert-row');
    existingElements.forEach(item => {
      const id = item.dataset.dedupKey;
      if (id) {
        firstPositions.set(id, item.getBoundingClientRect().top);
      }
    });

    // STEP 2: BUILD / REORDER DOM
    const currentAlertKeySet = new Set();
    const fragment = document.createDocumentFragment();

    alerts.forEach((alert, index) => {
      currentAlertKeySet.add(alert.dedupKey);

      let row = container.querySelector(`[data-dedup-key="${CSS.escape(alert.dedupKey)}"]`);
      const isNew = !row;

      if (isNew) {
        row = document.createElement('div');
        row.className = 'alert-row';
        row.dataset.dedupKey = alert.dedupKey;
      }

      // Check deduplication frequency increment for 500ms Border Flash
      const prevData = state.previousAlertsMap.get(alert.dedupKey);
      const freqIncreased = prevData && alert.frequency > prevData.frequency;

      if (alert.justUpdated || freqIncreased) {
        row.classList.remove('dedup-flash');
        // Force reflow to re-trigger CSS keyframe animation
        void row.offsetWidth;
        row.classList.add('dedup-flash');

        if (alert.severity === 'P0_CRITICAL') {
          playTacticalAlertSound(1046, 0.06, 'sawtooth');
        } else {
          playTacticalAlertSound(440, 0.03, 'sine');
        }
      }

      // Rank styling
      let rankClass = '';
      if (index === 0) rankClass = 'rank-top1';
      else if (index === 1) rankClass = 'rank-top2';
      else if (index === 2) rankClass = 'rank-top3';

      // Severity styling
      let sevClass = 'sev-low';
      let meterClass = 'meter-low';
      if (alert.severity === 'P0_CRITICAL') { sevClass = 'sev-crit'; meterClass = 'meter-crit'; }
      else if (alert.severity === 'P1_HIGH') { sevClass = 'sev-high'; meterClass = 'meter-high'; }
      else if (alert.severity === 'P2_MEDIUM') { sevClass = 'sev-med'; meterClass = 'meter-med'; }

      const scorePercent = Math.min(100, Math.max(0, alert.score));
      const elapsedSec = Math.max(0, Math.round((Date.now() - alert.lastSeen) / 1000));
      const timeDisplay = elapsedSec === 0 ? 'now' : `${elapsedSec}s ago`;

      row.innerHTML = `
        <div class="col-rank ${rankClass}">#${alert.rank}</div>
        <div class="col-sev">
          <span class="severity-pill ${sevClass}">${alert.severity.replace('_', ' ')}</span>
        </div>
        <div class="col-dedup">
          <span class="dedup-freq-badge">
            <span class="freq-number">x${formatNumber(alert.frequency)}</span>
          </span>
        </div>
        <div class="col-score score-cell">
          <span class="score-value">${alert.score}</span>
          <div class="score-meter">
            <div class="score-meter-fill ${meterClass}" style="width: ${scorePercent}%"></div>
          </div>
        </div>
        <div class="col-source source-cell">
          <span class="source-topic" title="${escapeHtml(alert.routingKey)}">${escapeHtml(alert.routingKey)}</span>
          <span class="source-host" title="${escapeHtml(alert.source)}">${escapeHtml(alert.source)}</span>
        </div>
        <div class="col-desc desc-cell">
          <span class="desc-title" title="${escapeHtml(alert.title)}">${escapeHtml(alert.title)}</span>
          <span class="desc-payload" title="${escapeHtml(alert.payload)}">${escapeHtml(alert.payload)}</span>
        </div>
        <div class="col-time time-cell">${timeDisplay}</div>
      `;

      fragment.appendChild(row);

      // Track alert frequency & rank
      state.previousAlertsMap.set(alert.dedupKey, {
        frequency: alert.frequency,
        rank: alert.rank
      });
    });

    // Remove any rows no longer in top alerts
    existingElements.forEach(item => {
      if (!currentAlertKeySet.has(item.dataset.dedupKey)) {
        item.remove();
      }
    });

    // Splicing all rows into container in their new order
    container.appendChild(fragment);

    // STEP 3: LAST & INVERT & PLAY
    requestAnimationFrame(() => {
      const updatedElements = container.querySelectorAll('.alert-row');
      updatedElements.forEach(item => {
        const key = item.dataset.dedupKey;
        const firstTop = firstPositions.get(key);

        if (firstTop !== undefined) {
          const lastTop = item.getBoundingClientRect().top;
          const deltaY = firstTop - lastTop;

          if (deltaY !== 0) {
            // INVERT: Immediately invert position back to where it was
            item.style.transform = `translateY(${deltaY}px)`;
            item.style.transition = 'none';

            // PLAY: On next frame, animate to final position (translateY = 0)
            requestAnimationFrame(() => {
              item.style.transition = 'transform 0.28s cubic-bezier(0.2, 0.9, 0.3, 1), box-shadow 0.2s ease';
              item.style.transform = '';
            });
          }
        }
      });
    });
  }

  // ==========================================================================
  // Real-Time 60-Second Scrolling Sparkline Area Chart (HTML5 Canvas)
  // ==========================================================================
  function setupCanvas() {
    const canvas = el.canvas;
    const ctx = canvas.getContext('2d');

    function resize() {
      const rect = canvas.parentElement.getBoundingClientRect();
      const dpr = window.devicePixelRatio || 1;
      canvas.width = rect.width * dpr;
      canvas.height = rect.height * dpr;
      ctx.scale(dpr, dpr);
    }

    window.addEventListener('resize', resize);
    resize();

    // 60 FPS Render Loop
    function renderLoop() {
      drawTelemetryChart(ctx, canvas);
      requestAnimationFrame(renderLoop);
    }
    requestAnimationFrame(renderLoop);
  }

  function drawTelemetryChart(ctx, canvas) {
    const dpr = window.devicePixelRatio || 1;
    const w = canvas.width / dpr;
    const h = canvas.height / dpr;

    ctx.clearRect(0, 0, w, h);

    if (w <= 0 || h <= 0) return;

    // Background Tactical Grid Lines
    ctx.strokeStyle = '#0e1824';
    ctx.lineWidth = 1;
    ctx.beginPath();
    const rows = 4;
    for (let r = 1; r < rows; r++) {
      const y = (h / rows) * r;
      ctx.moveTo(0, y);
      ctx.lineTo(w, y);
    }
    const cols = 8;
    for (let c = 1; c < cols; c++) {
      const x = (w / cols) * c;
      ctx.moveTo(x, 0);
      ctx.lineTo(x, h);
    }
    ctx.stroke();

    const data = state.historyData;
    if (data.length < 2) {
      // Empty state prompt
      ctx.fillStyle = '#484f58';
      ctx.font = '10px "Fira Code", monospace';
      ctx.fillText('AWAITING TELEMETRY STREAM SAMPLES (PROJECT LOOM DISPATCH)...', 15, h / 2);
      return;
    }

    // Determine Y-scale max
    let maxVal = 100;
    for (let i = 0; i < data.length; i++) {
      if (data[i].rawRate > maxVal) maxVal = data[i].rawRate;
      if (data[i].reducedRate > maxVal) maxVal = data[i].reducedRate;
    }
    maxVal = Math.ceil(maxVal * 1.15); // 15% headroom

    const minTs = data[0].ts;
    const maxTs = data[data.length - 1].ts;
    const timeSpan = Math.max(1000, maxTs - minTs);

    function getX(ts) {
      return ((ts - minTs) / timeSpan) * w;
    }
    function getY(val) {
      return h - (val / maxVal) * (h - 20) - 10;
    }

    // 1. Draw Raw Ingestion Area (Cyan)
    ctx.beginPath();
    ctx.moveTo(getX(data[0].ts), h);
    for (let i = 0; i < data.length; i++) {
      ctx.lineTo(getX(data[i].ts), getY(data[i].rawRate));
    }
    ctx.lineTo(getX(data[data.length - 1].ts), h);
    ctx.closePath();

    const cyanGrad = ctx.createLinearGradient(0, 0, 0, h);
    cyanGrad.addColorStop(0, 'rgba(0, 255, 255, 0.35)');
    cyanGrad.addColorStop(1, 'rgba(0, 255, 255, 0.01)');
    ctx.fillStyle = cyanGrad;
    ctx.fill();

    // Raw Ingestion Stroke Line
    ctx.beginPath();
    for (let i = 0; i < data.length; i++) {
      const x = getX(data[i].ts);
      const y = getY(data[i].rawRate);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.strokeStyle = '#00ffff';
    ctx.lineWidth = 2;
    ctx.shadowColor = '#00ffff';
    ctx.shadowBlur = 8;
    ctx.stroke();
    ctx.shadowBlur = 0; // reset glow

    // 2. Draw Reduced Output Stroke Line (Magenta)
    ctx.beginPath();
    for (let i = 0; i < data.length; i++) {
      const x = getX(data[i].ts);
      const y = getY(data[i].reducedRate);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.strokeStyle = '#ff00ff';
    ctx.lineWidth = 2;
    ctx.shadowColor = '#ff00ff';
    ctx.shadowBlur = 8;
    ctx.stroke();
    ctx.shadowBlur = 0;

    // Draw Y-Axis Labels
    ctx.fillStyle = '#6e7681';
    ctx.font = '9px "Fira Code", monospace';
    ctx.fillText(`${formatNumber(maxVal)} msg/s`, 8, 14);
    ctx.fillText(`${formatNumber(Math.round(maxVal / 2))} msg/s`, 8, h / 2);
    ctx.fillText('0 msg/s', 8, h - 4);
  }

  // ==========================================================================
  // Tactical Clock & User Interaction Handlers
  // ==========================================================================
  function startClock() {
    function tick() {
      const now = new Date();
      el.clock.textContent = `${pad2(now.getUTCHours())}:${pad2(now.getUTCMinutes())}:${pad2(now.getUTCSeconds())} UTC`;
    }
    setInterval(tick, 1000);
    tick();
  }

  function setupEventListeners() {
    // Inject P0 Critical Attack
    el.btnP0.addEventListener('click', () => {
      initAudio();
      sendCommand({ action: 'INJECT_P0' });
      playTacticalAlertSound(1200, 0.12, 'sawtooth');
    });

    // Inject WhatsApp Spam Flood
    el.btnWhatsapp.addEventListener('click', () => {
      initAudio();
      sendCommand({ action: 'WHATSAPP_SPAM' });
      playTacticalAlertSound(600, 0.08, 'sine');
    });

    // Rate Slider
    el.rateSlider.addEventListener('input', (e) => {
      const val = parseInt(e.target.value, 10);
      el.rateVal.textContent = `${formatNumber(val)} msg/s`;
      sendCommand({ action: 'SET_RATE', rate: val });
    });

    // Pause / Resume
    el.btnPause.addEventListener('click', () => {
      sendCommand({ action: 'TOGGLE_PAUSE' });
    });

    // Purge Buffers
    el.btnClear.addEventListener('click', () => {
      sendCommand({ action: 'CLEAR_BUFFERS' });
      state.previousAlertsMap.clear();
      el.matrixContainer.innerHTML = '';
      el.waterfall.innerHTML = '';
      state.historyData = [];
    });

    // Sound Toggle
    el.btnSound.addEventListener('click', () => {
      initAudio();
      state.soundEnabled = !state.soundEnabled;
      el.btnSound.innerHTML = state.soundEnabled ? '<span class="btn-icon">🔊</span> AUDIO: ON' : '<span class="btn-icon">🔇</span> AUDIO: OFF';
      el.btnSound.classList.toggle('btn-dim', state.soundEnabled);
    });

    // Autoscroll Toggle
    el.btnAutoScroll.addEventListener('click', () => {
      state.autoScroll = !state.autoScroll;
      el.btnAutoScroll.classList.toggle('active', state.autoScroll);
      el.btnAutoScroll.textContent = state.autoScroll ? 'SCROLL: ON' : 'SCROLL: OFF';
    });

    // Clear Logs
    el.btnClearLogs.addEventListener('click', () => {
      el.waterfall.innerHTML = '';
    });

    // Waterfall Filter Search Input
    el.filterInput.addEventListener('input', (e) => {
      state.filterQuery = e.target.value;
    });

    // Window user interaction to unlock Web Audio API
    document.addEventListener('click', () => initAudio(), { once: true });
  }

  // ==========================================================================
  // Utility Formatting Helpers
  // ==========================================================================
  function pad2(n) { return n < 10 ? '0' + n : n; }
  function pad3(n) { return n < 10 ? '00' + n : (n < 100 ? '0' + n : n); }
  function formatNumber(num) {
    if (num === null || num === undefined) return '0';
    return Number(num).toLocaleString('en-US');
  }
  function escapeHtml(str) {
    if (!str) return '';
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  // ==========================================================================
  // Initialization Lifecycle
  // ==========================================================================
  function init() {
    startClock();
    setupCanvas();
    setupEventListeners();
    connectWebSocket();
  }

  document.addEventListener('DOMContentLoaded', init);
})();
