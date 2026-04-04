// ═══════════════════════════════════════════════════
// LUMA ANALYTICS DASHBOARD — Interactive Engine
// ═══════════════════════════════════════════════════

const COUNTRY_FLAGS = {
    US: '🇺🇸', GB: '🇬🇧', DE: '🇩🇪', FR: '🇫🇷', JP: '🇯🇵', CA: '🇨🇦',
    AU: '🇦🇺', IN: '🇮🇳', SG: '🇸🇬', NL: '🇳🇱', AE: '🇦🇪', BR: '🇧🇷',
    SE: '🇸🇪', PT: '🇵🇹', KR: '🇰🇷', IL: '🇮🇱', KE: '🇰🇪', NG: '🇳🇬',
};

let map, geoMarkers = [], pageviewsChart = null, referrersChart = null, funnelChartObj = null;

// ─── Greeting ───
function setGreeting() {
    const h = new Date().getHours();
    let greeting = 'Good evening 👋';
    if (h < 12) greeting = 'Good morning ☀️';
    else if (h < 17) greeting = 'Good afternoon 👋';
    document.getElementById('welcomeTitle').textContent = greeting;
}

// ─── Count-up animation ───
function animateValue(el, end, suffix = '') {
    const duration = 1200;
    const start = 0;
    const startTime = performance.now();
    function update(now) {
        const elapsed = now - startTime;
        const progress = Math.min(elapsed / duration, 1);
        const eased = 1 - Math.pow(1 - progress, 3);
        const current = Math.round(start + (end - start) * eased);
        el.textContent = current.toLocaleString() + suffix;
        if (progress < 1) requestAnimationFrame(update);
    }
    requestAnimationFrame(update);
}

// ─── Scroll reveal ───
function setupScrollReveal() {
    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                entry.target.classList.add('animate-in');
                entry.target.style.transition = 'all 0.6s var(--ease-out)';
            }
        });
    }, { threshold: 0.1 });

    document.querySelectorAll('.stat-card, .card, .map-section').forEach(el => {
        observer.observe(el);
    });
}

// ─── Stagger stat cards ───
function staggerCards() {
    const cards = document.querySelectorAll('.stat-card');
    cards.forEach((card, i) => {
        setTimeout(() => {
            card.classList.add('animate-in');
            card.style.transition = `all 0.5s var(--ease-out) ${i * 0.08}s`;
        }, 200 + i * 80);
    });
}

// ─── Chart.js defaults ───
Chart.defaults.color = '#8b92a8';
Chart.defaults.borderColor = 'rgba(255,255,255,0.04)';
Chart.defaults.font.family = "'Inter', sans-serif";
Chart.defaults.font.size = 12;
Chart.defaults.plugins.legend.labels.usePointStyle = true;
Chart.defaults.plugins.legend.labels.pointStyle = 'circle';
Chart.defaults.animation.duration = 1000;
Chart.defaults.animation.easing = 'easeOutQuart';

// ─── Load Summary Stats ───
async function loadSummary() {
    try {
        const res = await fetch('/summary');
        const d = await res.json();
        animateValue(document.getElementById('valPageviews'), d.total_pageviews);
        animateValue(document.getElementById('valVisitors'), d.unique_visitors);
        animateValue(document.getElementById('valClicks'), d.total_clicks);
        animateValue(document.getElementById('valLive'), d.live_visitors);
        animateValue(document.getElementById('valBounce'), d.bounce_rate, '%');
        document.getElementById('liveCount').textContent = d.live_visitors;

        if (d.top_city) {
            const flag = COUNTRY_FLAGS[d.top_city.country] || '🌐';
            document.getElementById('valTopCity').textContent = flag + ' ' + d.top_city.city;
            document.getElementById('tagTopCity').textContent = d.top_city.count + ' visitors';
        }
    } catch (e) { console.error('Summary error:', e); }
}

// ─── Pageviews Chart ───
async function loadPageviews() {
    try {
        const res = await fetch('/stats');
        const data = await res.json();
        const labels = data.map(d => {
            const dt = new Date(d.hour);
            return dt.getHours() + ':00';
        });
        const views = data.map(d => d.pageviews);
        const visitors = data.map(d => d.unique_visitors);
        const clicks = data.map(d => d.clicks || 0);

        const ctx = document.getElementById('pageviewsChart').getContext('2d');

        const gradientPV = ctx.createLinearGradient(0, 0, 0, 240);
        gradientPV.addColorStop(0, 'rgba(139, 92, 246, 0.25)');
        gradientPV.addColorStop(1, 'rgba(139, 92, 246, 0)');

        const gradientUV = ctx.createLinearGradient(0, 0, 0, 240);
        gradientUV.addColorStop(0, 'rgba(6, 182, 212, 0.2)');
        gradientUV.addColorStop(1, 'rgba(6, 182, 212, 0)');

        if (pageviewsChart) pageviewsChart.destroy();

        pageviewsChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels,
                datasets: [
                    {
                        label: 'Pageviews',
                        data: views,
                        borderColor: '#8b5cf6',
                        backgroundColor: gradientPV,
                        fill: true,
                        tension: 0.4,
                        pointRadius: 0,
                        pointHoverRadius: 6,
                        pointHoverBackgroundColor: '#8b5cf6',
                        pointHoverBorderColor: '#fff',
                        pointHoverBorderWidth: 2,
                        borderWidth: 2.5
                    },
                    {
                        label: 'Unique Visitors',
                        data: visitors,
                        borderColor: '#06b6d4',
                        backgroundColor: gradientUV,
                        fill: true,
                        tension: 0.4,
                        pointRadius: 0,
                        pointHoverRadius: 6,
                        pointHoverBackgroundColor: '#06b6d4',
                        pointHoverBorderColor: '#fff',
                        pointHoverBorderWidth: 2,
                        borderWidth: 2
                    }
                ]
            },
            options: {
                responsive: true,
                interaction: { mode: 'index', intersect: false },
                scales: {
                    y: {
                        beginAtZero: true,
                        grid: { color: 'rgba(255,255,255,0.03)' },
                        ticks: { padding: 8 }
                    },
                    x: {
                        grid: { display: false },
                        ticks: { maxTicksLimit: 12 }
                    }
                },
                plugins: {
                    tooltip: {
                        backgroundColor: '#1a2035',
                        borderColor: 'rgba(139,92,246,0.3)',
                        borderWidth: 1,
                        padding: 12,
                        titleFont: { weight: '600' },
                        cornerRadius: 10
                    }
                }
            }
        });
    } catch (e) { console.error('Pageviews error:', e); }
}

// ─── Referrers Chart ───
async function loadReferrers() {
    try {
        const res = await fetch('/referrers');
        const data = await res.json();

        const colors = ['#8b5cf6', '#3b82f6', '#06b6d4', '#14b8a6', '#10b981', '#f59e0b', '#ec4899', '#ef4444', '#6366f1', '#8b92a8'];

        const ctx = document.getElementById('referrersChart').getContext('2d');
        if (referrersChart) referrersChart.destroy();

        referrersChart = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: data.map(d => {
                    try { return new URL(d.referrer).hostname; } catch { return d.referrer || 'Direct'; }
                }),
                datasets: [{
                    data: data.map(d => d.count),
                    backgroundColor: colors.slice(0, data.length),
                    borderWidth: 0,
                    hoverOffset: 8
                }]
            },
            options: {
                responsive: true,
                cutout: '65%',
                plugins: {
                    legend: {
                        position: 'right',
                        labels: { padding: 14, font: { size: 11 } }
                    },
                    tooltip: {
                        backgroundColor: '#1a2035',
                        borderColor: 'rgba(139,92,246,0.3)',
                        borderWidth: 1,
                        padding: 12,
                        cornerRadius: 10
                    }
                }
            }
        });
    } catch (e) { console.error('Referrers error:', e); }
}

// ─── Funnel ───
function setFunnelPreset(val) {
    document.getElementById('funnelSteps').value = val;
    loadFunnel();
}

async function loadFunnelDemo() {
    try {
        const res = await fetch('/funnel/demo');
        const data = await res.json();
        document.getElementById('funnelBadge').textContent = 'Demo Active';
        renderFunnel(data);
    } catch (e) { console.error('Funnel demo error:', e); }
}

async function loadFunnel() {
    const steps = document.getElementById('funnelSteps').value;
    if (!steps) { loadFunnelDemo(); return; }
    try {
        const res = await fetch('/funnel?steps=' + encodeURIComponent(steps));
        const data = await res.json();
        document.getElementById('funnelBadge').textContent = 'Custom';
        renderFunnel(data);
    } catch (e) { console.error('Funnel error:', e); }
}

function renderFunnel(data) {
    const ctx = document.getElementById('funnelChart').getContext('2d');
    if (funnelChartObj) funnelChartObj.destroy();

    funnelChartObj = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: data.map(d => d.step),
            datasets: [{
                label: 'Visitors',
                data: data.map(d => d.visitors),
                backgroundColor: data.map((_, i) => {
                    const hue = 260 - (i / Math.max(data.length - 1, 1)) * 80;
                    return `hsla(${hue}, 65%, 58%, 0.85)`;
                }),
                borderRadius: 8,
                borderSkipped: false,
                barPercentage: 0.65
            }]
        },
        options: {
            responsive: true,
            indexAxis: 'y',
            scales: {
                x: { beginAtZero: true, grid: { color: 'rgba(255,255,255,0.03)' } },
                y: { grid: { display: false }, ticks: { font: { size: 12 } } }
            },
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: '#1a2035', borderColor: 'rgba(139,92,246,0.3)',
                    borderWidth: 1, padding: 12, cornerRadius: 10,
                    callbacks: {
                        afterLabel: function (ctx) {
                            const i = ctx.dataIndex;
                            if (i === 0) return '';
                            const prev = data[i - 1].visitors;
                            const curr = data[i].visitors;
                            if (prev === 0) return 'Drop-off: N/A';
                            return '↓ Drop-off: ' + ((1 - curr / prev) * 100).toFixed(1) + '%';
                        }
                    }
                }
            }
        }
    });

    // Render drop-off labels below chart
    const dropoffEl = document.getElementById('funnelDropoff');
    dropoffEl.innerHTML = data.map((d, i) => {
        if (i === 0) return `<span class="dropoff-step"><strong>${d.visitors}</strong> ${d.step}</span>`;
        const prev = data[i - 1].visitors;
        const pct = prev ? ((1 - d.visitors / prev) * 100).toFixed(0) : '0';
        return `<span class="dropoff-arrow">→ -${pct}%</span><span class="dropoff-step"><strong>${d.visitors}</strong> ${d.step}</span>`;
    }).join('');
}

// ─── Heatmap with website screenshot background ───
async function loadHeatmap() {
    try {
        const res = await fetch('/heatmap?page=' + encodeURIComponent('https://lu.ma/sf-ai-meetup'));
        const data = await res.json();
        const canvas = document.getElementById('heatmapCanvas');
        const img = new Image();
        img.src = "/static/heatmap-bg.png";
        const bgImg = document.getElementById('heatmapBg');

        // Wait for background image to load to get proper dimensions
        function drawHeatmap() {
            const rect = bgImg.getBoundingClientRect();
            canvas.width = rect.width;
            canvas.height = rect.height;
            canvas.style.height = rect.height + 'px';
            const ctx = canvas.getContext('2d');
            ctx.clearRect(0, 0, canvas.width, canvas.height);

            if (!data.length) return;

            const maxCount = Math.max(...data.map(d => d.count));

            // Draw heatmap blobs
            data.forEach(click => {
                const x = (click.x / 1920) * canvas.width;
                const y = (click.y / 1080) * canvas.height;
                const intensity = click.count / maxCount;
                const radius = 12 + intensity * 20;

                // Outer glow
                const glow = ctx.createRadialGradient(x, y, 0, x, y, radius * 1.8);
                glow.addColorStop(0, `rgba(236, 72, 153, ${intensity * 0.35})`);
                glow.addColorStop(0.5, `rgba(139, 92, 246, ${intensity * 0.15})`);
                glow.addColorStop(1, 'rgba(139, 92, 246, 0)');
                ctx.beginPath();
                ctx.fillStyle = glow;
                ctx.arc(x, y, radius * 1.8, 0, Math.PI * 2);
                ctx.fill();

                // Inner hot dot
                const dot = ctx.createRadialGradient(x, y, 0, x, y, radius * 0.5);
                dot.addColorStop(0, `rgba(255, 100, 150, ${0.6 + intensity * 0.4})`);
                dot.addColorStop(0.6, `rgba(236, 72, 153, ${intensity * 0.5})`);
                dot.addColorStop(1, 'rgba(139, 92, 246, 0)');
                ctx.beginPath();
                ctx.fillStyle = dot;
                ctx.arc(x, y, radius * 0.5, 0, Math.PI * 2);
                ctx.fill();
            });
        }

        if (bgImg.complete) {
            drawHeatmap();
        } else {
            bgImg.onload = drawHeatmap;
        }
    } catch (e) { console.error('Heatmap error:', e); }
}

// ─── Map ───
function initMap() {
    map = L.map('map', {
        zoomControl: false,
        attributionControl: false
    }).setView([25, 10], 2);

    L.control.zoom({ position: 'bottomright' }).addTo(map);

    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
        maxZoom: 19,
        subdomains: 'abcd'
    }).addTo(map);

    loadGeoData();
}

async function loadGeoData() {
    try {
        const res = await fetch('/geo');
        const data = await res.json();

        // Clear existing markers
        geoMarkers.forEach(m => map.removeLayer(m));
        geoMarkers = [];

        const maxCount = Math.max(...data.map(d => d.count), 1);

        data.forEach(geo => {
            const intensity = geo.count / maxCount;
            const radius = Math.max(5, Math.min(3 + Math.sqrt(geo.count) * 2.5, 22));
            const flag = COUNTRY_FLAGS[geo.country] || '🌐';
            const hue = 220 + (1 - intensity) * 40;

            const marker = L.circleMarker([geo.lat, geo.lng], {
                radius: radius,
                color: `hsla(${hue}, 70%, 65%, 0.9)`,
                fillColor: `hsla(${hue}, 60%, 50%, 0.55)`,
                fillOpacity: 0.55,
                weight: 1.5,
            }).addTo(map);

            marker.bindTooltip(`${flag} ${geo.city} · ${geo.count}`, {
                direction: 'top', offset: [0, -radius], className: 'map-tooltip',
                permanent: geo.count > maxCount * 0.4
            });

            marker.bindPopup(`
                <div class="popup-city">${flag} ${geo.city}</div>
                <div class="popup-count"><strong>${geo.count}</strong> visitors · ${geo.region}, ${geo.country}</div>
            `, { className: 'custom-popup' });

            marker.on('click', function () {
                map.flyTo([geo.lat, geo.lng], 6, { duration: 1.2 });
            });

            geoMarkers.push(marker);
        });
    } catch (e) { console.error('Geo error:', e); }
}

// ─── Top Cities ───
async function loadCities() {
    try {
        const res = await fetch('/geo/cities');
        const data = await res.json();
        const list = document.getElementById('citiesList');
        const maxVisitors = data.length ? data[0].visitors : 1;
        document.getElementById('citiesCount').textContent = data.length + ' cities';

        list.innerHTML = data.map((c, i) => {
            const flag = COUNTRY_FLAGS[c.country] || '🌐';
            const pct = (c.visitors / maxVisitors * 100).toFixed(0);
            return `
                <li>
                    <div class="city-item" onclick="flyToCity(${c.lat}, ${c.lng})">
                        <div class="city-info">
                            <div class="city-rank">${i + 1}</div>
                            <div>
                                <div class="city-name">${flag} ${c.city}</div>
                                <div class="city-region">${c.region}, ${c.country}</div>
                            </div>
                        </div>
                        <div class="city-count">${c.visitors}</div>
                    </div>
                    <div class="city-bar-bg">
                        <div class="city-bar-fill" style="width: ${pct}%"></div>
                    </div>
                </li>
            `;
        }).join('');
    } catch (e) { console.error('Cities error:', e); }
}

function flyToCity(lat, lng) {
    if (map) map.flyTo([lat, lng], 8, { duration: 1.5 });
}

// ─── Top Pages ───
async function loadPages() {
    try {
        const res = await fetch('/pages');
        const data = await res.json();
        const tbody = document.getElementById('pagesBody');
        tbody.innerHTML = data.map(p => {
            const shortUrl = p.page.replace('https://', '');
            return `
                <tr>
                    <td><a class="page-url" href="${p.page}" target="_blank">${shortUrl}</a></td>
                    <td>${p.views.toLocaleString()}</td>
                    <td>${p.visitors.toLocaleString()}</td>
                </tr>
            `;
        }).join('');
    } catch (e) { console.error('Pages error:', e); }
}

// ─── Live Ticker ───
async function loadLiveTicker() {
    try {
        const res = await fetch('/geo/live');
        const data = await res.json();
        const container = document.getElementById('tickerItems');

        if (!data.length) return;

        const items = data.map(e => {
            const flag = COUNTRY_FLAGS[e.country] || '🌐';
            const ago = timeAgo(new Date(e.timestamp));
            const shortPage = e.page.replace('https://lu.ma/', '');
            return `
                <div class="ticker-item">
                    <span class="flag">${flag}</span>
                    <span class="ticker-city">${e.city}</span>
                    visited
                    <span class="ticker-page">${shortPage}</span>
                    <span class="ticker-time">${ago}</span>
                </div>
            `;
        }).join('');

        // Duplicate for seamless scrolling
        container.innerHTML = items + items;
    } catch (e) { console.error('Ticker error:', e); }
}

function timeAgo(date) {
    const seconds = Math.floor((new Date() - date) / 1000);
    if (seconds < 60) return seconds + 's ago';
    if (seconds < 3600) return Math.floor(seconds / 60) + 'm ago';
    return Math.floor(seconds / 3600) + 'h ago';
}

// ─── SSE Live Events ───
function initLiveStream() {
    const eventSource = new EventSource('/live');
    let retryCount = 0;
    const MAX_RETRIES = 5;

    eventSource.onopen = function () {
        retryCount = 0;
    };

    eventSource.onerror = function (error) {
        retryCount++;
        if (retryCount >= MAX_RETRIES) {
            console.error('SSE connection failed too many times. Closing connection.');
            eventSource.close();
        }
    };

    eventSource.onmessage = function (event) {
        try {
            const data = JSON.parse(event.data);
            if (data && data.lat && data.lng) {
                addLiveMarker(data);
                const liveEl = document.getElementById('valLive');
                const current = parseInt(liveEl.textContent.replace(/,/g, '')) || 0;
                liveEl.textContent = (current + 1).toLocaleString();
            }
        } catch (e) { }
    };
}

function addLiveMarker(data) {
    if (!map) return;
    const flag = COUNTRY_FLAGS[data.country] || '🌐';

    const pulseMarker = L.circleMarker([data.lat, data.lng], {
        radius: 14,
        color: '#10b981',
        fillColor: '#10b981',
        fillOpacity: 0.3,
        weight: 2,
    }).addTo(map);

    pulseMarker.bindPopup(`
        <div class="popup-city">${flag} ${data.city || 'Unknown'}</div>
        <div class="popup-count">Just now · <strong>Live</strong></div>
    `);

    // Pulse and fade
    let opacity = 0.6;
    let radius = 14;
    const pulse = setInterval(() => {
        opacity -= 0.04;
        radius += 0.5;
        pulseMarker.setStyle({ fillOpacity: opacity, radius: radius });
        if (opacity <= 0) {
            clearInterval(pulse);
            map.removeLayer(pulseMarker);
        }
    }, 200);
}

// ─── Time selector ───
document.querySelectorAll('.time-btn').forEach(btn => {
    btn.addEventListener('click', function () {
        document.querySelectorAll('.time-btn').forEach(b => b.classList.remove('active'));
        this.classList.add('active');
        // TODO: reload data with new time range
        loadPageviews();
        loadSummary();
    });
});

// ─── Auto-refresh ───
function startAutoRefresh() {
    setInterval(() => {
        loadSummary();
        loadGeoData();
        loadLiveTicker();
    }, 30000); // Every 30s
}

// ─── Bot Detection ───
async function loadBots() {
    try {
        const res = await fetch('/bots');
        const d = await res.json();
        document.getElementById('botFilterCount').textContent = d.filters_active + ' filters';
        document.getElementById('botTotal').textContent = d.total_requests.toLocaleString();
        document.getElementById('botBlocked').textContent = d.bot_requests.toLocaleString();
        document.getElementById('botHuman').textContent = d.human_requests.toLocaleString();
        document.getElementById('botShieldDesc').textContent =
            `${d.bot_percentage}% bot traffic detected and filtered · ${d.filters_active} patterns active`;

        const tbody = document.getElementById('botLogBody');
        if (d.recent_bots.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--text-muted);padding:20px">No bots detected in last 24h — all clear! ✅</td></tr>';
        } else {
            tbody.innerHTML = d.recent_bots.map(b => {
                const ago = timeAgo(new Date(b.timestamp));
                const shortPage = b.page.replace('https://lu.ma/', '');
                return `<tr>
                    <td><span class="bot-tag">🤖 BOT</span></td>
                    <td title="${b.user_agent}">${b.user_agent}</td>
                    <td>${b.ip}</td>
                    <td>${shortPage}</td>
                    <td>${ago}</td>
                </tr>`;
            }).join('');
        }
    } catch (e) { console.error('Bots error:', e); }
}

// ─── Map Tooltip Style ───
const tooltipStyle = document.createElement('style');
tooltipStyle.textContent = `
    .map-tooltip {
        background: rgba(17,24,39,0.9) !important;
        color: #f0f0f5 !important;
        border: 1px solid rgba(255,255,255,0.1) !important;
        border-radius: 6px !important;
        padding: 4px 10px !important;
        font-family: 'Inter', sans-serif !important;
        font-size: 11px !important;
        font-weight: 500 !important;
        box-shadow: 0 4px 12px rgba(0,0,0,0.4) !important;
    }
    .map-tooltip::before { border-top-color: rgba(17,24,39,0.9) !important; }
`;
document.head.appendChild(tooltipStyle);

// ─── Initialize Everything ───
window.addEventListener('DOMContentLoaded', () => {
    setGreeting();
    staggerCards();
    setupScrollReveal();

    loadSummary();
    loadPageviews();
    loadReferrers();
    loadHeatmap();
    loadCities();
    loadPages();
    loadLiveTicker();
    loadFunnelDemo();
    loadBots();

    initMap();
    initLiveStream();
    startAutoRefresh();
});