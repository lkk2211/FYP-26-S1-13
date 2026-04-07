// View Router
function showView(viewId) {
    const target = document.getElementById('view-' + viewId);
    if (!target) return;
    localStorage.setItem('currentView', viewId);

    if (viewId === 'admin') {
        const isAdmin = currentUser && currentUser.role === 'admin';
        if (!isAdmin) {
            alert('Access denied. Admins only.');
            showView(currentUser ? 'home' : 'signin');
            return;
        }
    }

    if (viewId === 'setting' && !currentUser) {
        alert('Please sign in first.');
        showView('signin');
        return;
    }

    document.querySelectorAll('.view-section').forEach(s => s.classList.remove('active'));
    target.classList.add('active');

    document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
    const activeLink = document.getElementById('nav-' + viewId);
    if (activeLink) activeLink.classList.add('active');

    if (viewId === 'trend') {
        setTimeout(() => { loadMarketWatch(); initHDBTrend(); renderTrendNews(currentNeighbourhood); runABSDSimulation(); }, 100);
    }
    if (viewId === 'map') {
        if (lastMapPostal) {
            document.getElementById('map-postal-input').value = lastMapPostal;
            setTimeout(() => initMapForPostal(lastMapPostal), 100);
        } else {
            setTimeout(() => initDefaultSingaporeMap(), 150);
        }
    }

    if (viewId === 'admin') {
        fetchAdminStats();
        setTimeout(initAdminTypeChart, 100);
    }

    if (viewId === 'setting') {
        loadProfileForm();
        loadProfilePhoto();
        loadTheme();
        loadPreferencesForm();
        loadNotificationsForm();
    }

    window.scrollTo({ top: 0, behavior: 'smooth' });
    lucide.createIcons();
    updateDarkModeNavIcon();
}

// Predict Logic
function togglePredictView(mode) {
    const input = document.getElementById('predict-input');
    const output = document.getElementById('predict-output');
    
    if (mode === 'output') {
        input.classList.add('hidden');
        output.classList.remove('hidden');
    } else {
        output.classList.add('hidden');
        input.classList.remove('hidden');
    }
}

function updateSlider(id) {
    const range = document.getElementById('range-' + id);
    const display = document.getElementById('val-' + id);
    if (range && display) {
        display.innerText = id === 'area' ? parseInt(range.value).toLocaleString() : range.value;
    }
}

async function handlePostalSearch() {
    const raw = document.getElementById('input-postal').value.trim();
    const errorEl = document.getElementById('postal-error');
    if (!raw) {
        errorEl.textContent = 'Please enter a postal code, building name, or area.';
        errorEl.classList.remove('hidden');
        return;
    }
    errorEl.classList.add('hidden');
    closeAllDropdowns();

    const searchVal = raw.replace(/\s+/g, ' ').trim();
    try {
        const geo = await fetch(`https://www.onemap.gov.sg/api/common/elastic/search?searchVal=${encodeURIComponent(searchVal)}&returnGeom=Y&getAddrDetails=Y&pageNum=1`);
        const geoData = await geo.json();
        const result = geoData.results?.[0];

        if (!result) {
            errorEl.textContent = 'No results found. Try a different postal code, building name, or area.';
            errorEl.classList.remove('hidden');
            return;
        }

        const address = result.ADDRESS || (result.BLK_NO + ' ' + result.ROAD_NAME);
        const building = result.BUILDING && result.BUILDING !== 'NIL' ? result.BUILDING : address;
        const postal = result.POSTAL && result.POSTAL !== 'NIL' ? result.POSTAL : searchVal;

        document.getElementById('display-address').innerText = address;
        document.getElementById('display-building').innerText = building;
        document.getElementById('input-postal').value = postal;

        const placeholder = document.getElementById('postal-placeholder');
        const details = document.getElementById('postal-details');
        placeholder.classList.add('hidden');
        details.classList.remove('hidden');
    } catch {
        errorEl.textContent = 'Unable to search. Please try again.';
        errorEl.classList.remove('hidden');
    }

    lucide.createIcons();
}

async function handlePredict() {
    const btn = document.querySelector('#postal-details button');
    const originalHtml = btn.innerHTML;
    btn.disabled = true;
    btn.innerHTML = '<i data-lucide="loader-2" class="w-6 h-6 animate-spin"></i> Calculating...';
    lucide.createIcons();
    
    const postal = document.getElementById('input-postal').value;
    lastMapPostal = postal;
    const area = document.getElementById('range-area').value;
    const bedrooms = document.getElementById('range-bedrooms').value;
    const floor = document.getElementById('range-floor').value;

    try {
        const response = await fetch('/api/predict', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
                postal: postal,
                area: area,
                bedrooms: bedrooms,
                floor: floor
            })
        });
        const data = await response.json();

        document.getElementById('output-price').innerText = `S$${data.estimated_value.toLocaleString()}`;
        document.getElementById('output-confidence').innerText = `${data.confidence}%`;

        const trendEl    = document.getElementById('output-trend');
        const trendBadge = document.getElementById('output-trend-badge');
        const trendIcon  = document.getElementById('output-trend-icon');
        if (trendEl) trendEl.innerText = data.market_trend || '+2.4%';
        if (trendBadge && trendIcon) {
            const isUp = (data.trend_direction || 'up') === 'up';
            trendBadge.className = `flex items-center gap-1 px-3 py-1 rounded-lg text-sm font-bold ${isUp ? 'bg-emerald-50 text-emerald-600' : 'bg-rose-50 text-rose-600'}`;
            trendIcon.setAttribute('data-lucide', isUp ? 'trending-up' : 'trending-down');
        }

        const mktEl = document.getElementById('output-market-state');
        if (mktEl) mktEl.innerText = data.market_state || 'Active';
        const dateEl = document.getElementById('output-valuation-date');
        if (dateEl) {
            const now = new Date();
            dateEl.innerText = now.toLocaleString('en-SG', { month: 'short', year: 'numeric' });
        }

        const insightEl = document.getElementById('output-insight');
        if (insightEl) insightEl.innerText = data.insight || '';
        const recEl = document.getElementById('output-recommendation');
        if (recEl) recEl.innerText = data.recommendation || '';

        const mapTitle = document.getElementById('map-title');
        if (mapTitle) mapTitle.innerText = document.getElementById('display-address').innerText;

        const list = document.getElementById('factors-list');
        list.innerHTML = data.factors.map(f => `
            <div class="space-y-3">
                <div class="flex items-center justify-between">
                    <div>
                        <span class="font-bold text-slate-700">${f.name}</span>
                        ${f.desc ? `<p class="text-xs text-slate-400 mt-0.5">${f.desc}</p>` : ''}
                    </div>
                    <span class="text-blue-600 font-bold shrink-0 ml-4">${f.label || ''} (${f.score}%)</span>
                </div>
                <div class="w-full h-3 bg-slate-100 rounded-full overflow-hidden">
                    <div class="h-full bg-blue-600 rounded-full transition-all duration-1000" style="width: 0%" id="bar-${f.name.replace(/\s+/g, '')}"></div>
                </div>
            </div>
        `).join('');

        togglePredictView('output');
        renderPredictNews(postal);

        lucide.createIcons();
        setTimeout(() => {
            data.factors.forEach(f => {
                const bar = document.getElementById(`bar-${f.name.replace(/\s+/g, '')}`);
                if (bar) bar.style.width = f.score + '%';
            });
            lucide.createIcons();
        }, 100);

    } catch (e) {
        console.error(e);
        alert('Local backend not detected. Showing demo data.');
        showDemoResult();
    } finally {
        btn.disabled = false;
        btn.innerHTML = originalHtml;
        lucide.createIcons();
    }
}

function showDemoResult() {
    document.getElementById('output-price').innerText = "S$465,000";
    document.getElementById('output-confidence').innerText = "92%";
    togglePredictView('output');
}

// Tabs Logic
function showTab(tabId) {
    const content = document.getElementById('tab-' + tabId);
    const btn = document.getElementById('tab-btn-' + tabId);
    if (!content || !btn) return;

    localStorage.setItem('currentSettingsTab', tabId);

    document.querySelectorAll('.tab-content').forEach(c => {
        c.classList.remove('active');
    });
    content.classList.add('active');
    
    document.querySelectorAll('.setting-tab').forEach(t => {
        t.classList.remove(
            'text-slate-900',
            'dark:text-white',
            'border-slate-900',
            'dark:border-white',
            'border-b-2'
        );
        t.classList.add('text-slate-400', 'dark:text-slate-400');
    });
    
    btn.classList.remove('text-slate-400', 'dark:text-slate-400');
    btn.classList.add(
        'text-slate-900',
        'dark:text-white',
        'border-slate-900',
        'dark:border-white',
        'border-b-2'
    );

    lucide.createIcons();
}

function showAdminTab(tabId) {
    const content = document.getElementById('admin-tab-' + tabId);
    const btn = document.getElementById('admin-tab-btn-' + tabId);
    if (!content || !btn) return;

    localStorage.setItem('currentAdminTab', tabId);

    document.querySelectorAll('.admin-tab-content').forEach(c => {
        c.classList.add('hidden');
        c.classList.remove('active');
    });
    content.classList.remove('hidden');
    content.classList.add('active');
    
    document.querySelectorAll('.admin-tab').forEach(t => t.classList.remove('text-slate-900', 'border-slate-900'));
    document.querySelectorAll('.admin-tab').forEach(t => t.classList.add('text-slate-400'));
    
    btn.classList.remove('text-slate-400');
    btn.classList.add('text-slate-900', 'border-slate-900');

    if (tabId === 'user') {
        loadAdminUsers();
    }

    lucide.createIcons();
}

// ── Global state ────────────────────────────────────────────
let lastMapPostal = '';
let currentNeighbourhood = 'Clementi';

// ── Map ──────────────────────────────────────────────────────
let mapInstance = null;
let mapLayers   = [];
let amenityMarkersByCategory = {};  // { mrt: [markers], school: [markers], ... }
let amenityFilterState = { all: true, mrt: true, bus: true, school: true, health: true, park: true, community: true, hawker: true };

async function loadMap() {
    const raw = document.getElementById('map-postal-input').value.trim();
    if (!raw) return;
    closeAllDropdowns();

    // If 6-digit number → treat as postal code directly
    if (/^\d{6}$/.test(raw.replace(/\s/g,''))) {
        lastMapPostal = raw.replace(/\s/g,'');
        initMapForPostal(lastMapPostal);
        return;
    }

    // Otherwise → smart search via OneMap elastic search
    const placeholder = document.getElementById('map-placeholder');
    const mapDiv = document.getElementById('leaflet-map');
    placeholder.classList.remove('hidden');
    placeholder.innerHTML = `<div class="flex flex-col items-center gap-3 text-slate-400">
        <svg class="w-8 h-8 animate-spin" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"/><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z"/></svg>
        <p class="text-sm font-medium">Searching for "${raw}"…</p></div>`;
    mapDiv.classList.add('hidden');

    try {
        const geo = await fetch(`https://www.onemap.gov.sg/api/common/elastic/search?searchVal=${encodeURIComponent(raw)}&returnGeom=Y&getAddrDetails=Y&pageNum=1`);
        const geoData = await geo.json();
        const result = geoData.results?.[0];
        if (!result) throw new Error('Not found');

        const postal = result.POSTAL && result.POSTAL !== 'NIL' ? result.POSTAL : null;
        document.getElementById('map-postal-input').value = postal || raw;
        if (postal) lastMapPostal = postal;
        initMapFromResult(result);
    } catch {
        placeholder.innerHTML = `<div class="text-center space-y-2 text-slate-400">
            <p class="font-semibold text-sm">No results for "${raw}"</p>
            <p class="text-xs">Try a postal code, MRT station name, or area (e.g. "Bishan", "560123")</p>
        </div>`;
        placeholder.classList.remove('hidden');
        mapDiv.classList.add('hidden');
    }
}

async function initMapForPostal(postal) {
    const placeholder = document.getElementById('map-placeholder');
    const mapDiv      = document.getElementById('leaflet-map');
    const addrBar     = document.getElementById('map-address-bar');

    placeholder.innerHTML = `<div class="flex flex-col items-center gap-3 text-slate-400">
        <svg class="w-8 h-8 animate-spin" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"/><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z"/></svg>
        <p class="text-sm font-medium">Locating postal code…</p></div>`;
    placeholder.classList.remove('hidden');
    mapDiv.classList.add('hidden');

    try {
        const geo = await fetch(`https://www.onemap.gov.sg/api/common/elastic/search?searchVal=${postal}&returnGeom=Y&getAddrDetails=Y&pageNum=1`);
        const geoData = await geo.json();

        if (!geoData.results || !geoData.results.length) throw new Error('Not found');

        const result = geoData.results[0];
        initMapFromResult(result, postal);

    } catch (err) {
        placeholder.innerHTML = `<div class="text-center text-slate-400 text-sm space-y-2">
            <p class="font-semibold">Could not locate postal code</p>
            <p class="text-xs">Try a different postal code or search term</p></div>`;
        placeholder.classList.remove('hidden');
        mapDiv.classList.add('hidden');
    }
}

async function initMapFromResult(result, postalHint) {
    const placeholder = document.getElementById('map-placeholder');
    const mapDiv      = document.getElementById('leaflet-map');
    const addrBar     = document.getElementById('map-address-bar');

    const lat = parseFloat(result.LATITUDE);
    const lng = parseFloat(result.LONGITUDE);
    const postal = (result.POSTAL && result.POSTAL !== 'NIL') ? result.POSTAL : (postalHint || '');
    const displayName = result.ADDRESS || result.BUILDING || postal;

    document.getElementById('map-address-text').innerText = displayName;
    document.getElementById('map-district-text').innerText = postal ? `Postal Code: ${postal} · Singapore` : 'Singapore';
    addrBar.classList.remove('hidden');
    addrBar.classList.add('flex');

    const predictBtn = document.getElementById('map-predict-btn');
    if (predictBtn && postal) {
        predictBtn.classList.remove('hidden');
        predictBtn.classList.add('flex');
        predictBtn._postal = postal;
    }

    placeholder.classList.add('hidden');
    mapDiv.classList.remove('hidden');

    await new Promise(r => setTimeout(r, 300));

    if (!mapInstance) {
        const isDark = document.documentElement.classList.contains('dark');
        mapInstance = L.map('leaflet-map', { zoomControl: true });
        const tileUrl = isDark
            ? 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png'
            : 'https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png';
        L.tileLayer(tileUrl, {
            attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> © <a href="https://carto.com/">CARTO</a>',
            maxZoom: 19, subdomains: 'abcd'
        }).addTo(mapInstance);
        attachMapClickHandler();
    } else {
        mapLayers.forEach(l => mapInstance.removeLayer(l));
        mapLayers = [];
        amenityMarkersByCategory = {};
    }

    mapInstance.invalidateSize();
    mapInstance.setView([lat, lng], 16);

    const propIcon = L.divIcon({
        html: `<div style="width:22px;height:22px;background:linear-gradient(135deg,#2563eb,#7c3aed);border:3px solid white;border-radius:50%;box-shadow:0 2px 10px rgba(37,99,235,0.55)"></div>`,
        iconSize: [22, 22], iconAnchor: [11, 11], className: ''
    });
    const propMarker = L.marker([lat, lng], { icon: propIcon })
        .bindPopup(`<b>📍 Subject Property</b><br>${displayName}`)
        .addTo(mapInstance);
    mapLayers.push(propMarker);

    showMapWithPin(lat, lng);
    await loadAmenities(lat, lng, postal);
}

async function loadAmenities(lat, lng, postal) {
    const amenityCards = document.getElementById('amenity-cards');
    amenityCards.innerHTML = `<div class="bg-white dark:bg-slate-800/80 rounded-2xl border border-slate-200 dark:border-slate-700 p-6 text-center text-slate-400 text-sm shadow-sm">
        <div class="flex items-center justify-center gap-2"><svg class="w-4 h-4 animate-spin" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"/><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z"/></svg>Loading amenities…</div></div>`;

    try {
        const params = new URLSearchParams({ lat, lng });
        if (postal) params.append('postal', postal);
        const res = await fetch(`/api/amenities?${params}`);
        if (!res.ok) throw new Error('API error');
        const data = await res.json();
        const cats = data.categories;

        // Clear old amenity markers from map before adding new ones
        const oldAmenityMarkers = Object.values(amenityMarkersByCategory).flat();
        oldAmenityMarkers.forEach(m => { try { mapInstance.removeLayer(m); } catch(_) {} });
        mapLayers = mapLayers.filter(l => !oldAmenityMarkers.includes(l));

        // Store markers by category for filter toggling
        amenityMarkersByCategory = {};
        Object.entries(cats).forEach(([key, cat]) => {
            amenityMarkersByCategory[key] = [];
            cat.items.forEach(item => {
                const marker = L.circleMarker([item.lat, item.lng], {
                    radius: 7, fillColor: cat.color, color: '#fff',
                    weight: 2, opacity: 1, fillOpacity: 0.9
                }).bindPopup(`<b>${cat.icon} ${item.name}</b><br>${item.dist} km · ${item.travel}`);
                // Only add if filter is active
                if (amenityFilterState[key] !== false) {
                    marker.addTo(mapInstance);
                }
                amenityMarkersByCategory[key].push(marker);
                mapLayers.push(marker);
            });
        });

        const hasAny = Object.values(cats).some(c => c.items.length > 0);
        if (!hasAny) {
            amenityCards.innerHTML = `<div class="bg-white dark:bg-slate-800/80 rounded-2xl border border-slate-200 dark:border-slate-700 p-6 text-center shadow-sm">
                <p class="text-slate-400 text-sm">No amenities found nearby</p></div>`;
            return;
        }

        amenityCards.innerHTML = Object.entries(cats).filter(([,c]) => c.items.length > 0).map(([key, cat]) => `
            <div class="bg-white dark:bg-slate-800/80 rounded-2xl border border-slate-200 dark:border-slate-700 p-5 shadow-sm amenity-cat-card" data-cat="${key}">
                <div class="flex items-center justify-between gap-3 mb-4">
                    <div class="flex items-center gap-3">
                        <div class="w-8 h-8 rounded-xl flex items-center justify-center" style="background:${cat.color}22">
                            <i data-lucide="${cat.lucide}" class="w-4 h-4" style="color:${cat.color}"></i>
                        </div>
                        <h3 class="text-sm font-bold">${cat.label}</h3>
                    </div>
                    <span class="text-[10px] font-bold text-slate-400">${cat.items.length}</span>
                </div>
                <div class="space-y-2.5">
                    ${cat.items.slice(0,4).map(item => `
                        <div class="flex justify-between items-start gap-2">
                            <div>
                                <p class="text-xs font-semibold leading-snug">${item.name}</p>
                                <p class="text-[10px] text-slate-400 mt-0.5">${item.travel}</p>
                            </div>
                            <span class="text-[10px] font-bold px-1.5 py-0.5 rounded-md" style="background:${cat.color}18;color:${cat.color}">${item.dist} km</span>
                        </div>
                    `).join('')}
                    ${cat.items.length > 4 ? `<p class="text-[10px] text-slate-400 font-medium">+${cat.items.length - 4} more on map</p>` : ''}
                </div>
            </div>
        `).join('');

        lucide.createIcons();
    } catch (err) {
        amenityCards.innerHTML = `<div class="bg-white dark:bg-slate-800/80 rounded-2xl border border-slate-200 dark:border-slate-700 p-6 text-center shadow-sm">
            <p class="text-slate-400 text-sm">Could not load amenities. Check backend connection.</p></div>`;
    }
}

function getDistKm(lat1, lng1, lat2, lng2) {
    const R = 6371;
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLng = (lng2 - lng1) * Math.PI / 180;
    const a = Math.sin(dLat/2)**2 + Math.cos(lat1*Math.PI/180)*Math.cos(lat2*Math.PI/180)*Math.sin(dLng/2)**2;
    return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
}

// ── Market Watch ─────────────────────────────────────────────
async function loadMarketWatch() {
    try {
        const res = await fetch('/api/market-watch');
        if (!res.ok) throw new Error();
        const data = await res.json();

        const periodEl = document.getElementById('market-watch-period');
        if (periodEl) periodEl.innerText = `${data.period.current} vs ${data.period.previous} · Source: URA / HDB`;

        const liveEl = document.getElementById('market-watch-live');
        if (liveEl && data.live_hdb) {
            liveEl.classList.remove('hidden');
            liveEl.classList.add('flex');
        }

        const container = document.getElementById('market-watch-cards');
        if (!container) return;

        container.innerHTML = data.segments.map(seg => {
            const pUp = seg.price_change >= 0;
            const vUp = seg.volume_change >= 0;
            const pColor  = pUp ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-500 dark:text-red-400';
            const vColor  = vUp ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-500 dark:text-red-400';
            const pArrow  = pUp ? '↑' : '↓';
            const vArrow  = vUp ? '↑' : '↓';
            const pSign   = pUp ? '+' : '';
            const vSign   = vUp ? '+' : '';
            return `
            <div class="bg-white dark:bg-slate-800/80 rounded-2xl border border-slate-200 dark:border-slate-700 p-5 shadow-sm text-center card-shadow">
                <p class="font-bold text-slate-800 dark:text-white text-sm mb-1">${seg.label}</p>
                <p class="text-[11px] font-semibold text-slate-400 mb-4">${data.period.current} vs ${data.period.previous}</p>
                <div class="flex justify-center gap-6 mb-4">
                    <div>
                        <p class="text-2xl font-bold ${pColor}">${pSign}${seg.price_change}% <span class="text-base">${pArrow}</span></p>
                        <p class="text-xs text-slate-400 mt-0.5 font-medium">Price</p>
                    </div>
                    <div>
                        <p class="text-2xl font-bold ${vColor}">${vSign}${seg.volume_change}% <span class="text-base">${vArrow}</span></p>
                        <p class="text-xs text-slate-400 mt-0.5 font-medium">Volume</p>
                    </div>
                </div>
                <p class="text-[10px] text-slate-300 dark:text-slate-600 flex items-center justify-center gap-1">
                    <i data-lucide="clock" class="w-3 h-3"></i> Last updated: ${data.last_updated}
                </p>
            </div>`;
        }).join('');
        lucide.createIcons();
    } catch {
        const container = document.getElementById('market-watch-cards');
        if (container) container.innerHTML = '<p class="text-xs text-slate-400 col-span-2 text-center py-4">Could not load market data.</p>';
    }
}

// ── Default Singapore Map ────────────────────────────────────
function initDefaultSingaporeMap() {
    const placeholder = document.getElementById('map-placeholder');
    const mapDiv = document.getElementById('leaflet-map');
    if (!placeholder || !mapDiv) return;

    placeholder.classList.add('hidden');
    mapDiv.classList.remove('hidden');

    if (!mapInstance) {
        const isDark = document.documentElement.classList.contains('dark');
        mapInstance = L.map('leaflet-map', { zoomControl: true });
        const tileUrl = isDark
            ? 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png'
            : 'https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png';
        L.tileLayer(tileUrl, {
            attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> © <a href="https://carto.com/">CARTO</a>',
            maxZoom: 19, subdomains: 'abcd'
        }).addTo(mapInstance);
        attachMapClickHandler();
    }
    mapInstance.invalidateSize();
    mapInstance.setView([1.3521, 103.8198], 12);
    showMapWithPin(1.3521, 103.8198);
}

// ── Map Click → Reverse Geocode ──────────────────────────────
let _draggablePin = null;

function attachMapClickHandler() {
    if (!mapInstance) return;
    mapInstance.doubleClickZoom.disable();
    mapInstance.on('dblclick', async (e) => {
        e.originalEvent.preventDefault();
        const { lat, lng } = e.latlng;
        await reverseGeocodeAndShow(lat, lng);
    });
}

async function reverseGeocodeAndShow(lat, lng) {
    try {
        const res = await fetch(
            `https://nominatim.openstreetmap.org/reverse?format=json&lat=${lat}&lon=${lng}&addressdetails=1`,
            { headers: { 'Accept-Language': 'en' } }
        );
        const data = await res.json();
        const postal = data.address?.postcode;
        const displayName = data.display_name || `${lat.toFixed(5)}, ${lng.toFixed(5)}`;
        const shortAddr = displayName.split(',').slice(0, 3).join(', ');

        addDraggablePin(lat, lng);
        const validPostal = (postal && /^\d{6}$/.test(postal)) ? postal : null;
        showPinResultBar(validPostal, shortAddr, lat, lng);
    } catch {
        addDraggablePin(lat, lng);
        showPinResultBar(null, `${lat.toFixed(5)}, ${lng.toFixed(5)}`, lat, lng);
    }
}

function showPinResultBar(postal, address, lat, lng) {
    const bar = document.getElementById('pin-result-bar');
    const postalEl = document.getElementById('pin-postal-label');
    const addrEl = document.getElementById('pin-address-label');
    const predictBtn = document.getElementById('pin-predict-btn');
    const exploreBtn = document.getElementById('pin-explore-btn');
    if (!bar) return;
    if (postalEl) postalEl.innerText = postal ? `📮 Postal Code: ${postal}` : '📍 Location';
    if (addrEl) addrEl.innerText = address;
    if (predictBtn) {
        if (postal) {
            predictBtn.classList.remove('hidden');
            predictBtn.onclick = () => usePostalFromMap(postal);
        } else {
            predictBtn.classList.add('hidden');
        }
    }
    if (exploreBtn) {
        exploreBtn.onclick = () => {
            bar.classList.add('hidden');
            loadAmenities(lat, lng, postal || '');
        };
    }
    bar.classList.remove('hidden');
    lucide.createIcons();
}

function addDraggablePin(centerLat, centerLng) {
    if (_draggablePin) {
        mapInstance.removeLayer(_draggablePin);
    }
    const pinIcon = L.divIcon({
        html: `<div style="
            width:28px;height:28px;
            background:linear-gradient(135deg,#f97316,#fb923c);
            border:3px solid white;border-radius:50% 50% 50% 0;
            transform:rotate(-45deg);
            box-shadow:0 3px 12px rgba(249,115,22,0.5);
            cursor:grab">
        </div>`,
        iconSize: [28, 28], iconAnchor: [14, 28], className: ''
    });
    _draggablePin = L.marker([centerLat, centerLng], { icon: pinIcon, draggable: true, zIndexOffset: 1000 })
        .addTo(mapInstance)
        .bindTooltip('Drag me to explore', { permanent: false, direction: 'top', offset: [0, -30] });

    _draggablePin.on('dragend', async (e) => {
        const { lat, lng } = e.target.getLatLng();
        await reverseGeocodeAndShow(lat, lng);
    });

    mapLayers.push(_draggablePin);
}

function showMapDragHint() {
    const hint = document.getElementById('map-drag-hint');
    if (!hint) return;
    hint.classList.remove('hidden');
    setTimeout(() => hint.classList.add('hidden'), 6000);
}

function showMapWithPin(lat, lng) {
    showMapDragHint();
    addDraggablePin(lat, lng);
}

function usePostalFromMap(postal) {
    mapInstance && mapInstance.closePopup();
    document.getElementById('input-postal').value = postal;
    showView('predict');
    setTimeout(() => handlePostalSearch(), 200);
}

function loadMapFromPostal(postal) {
    mapInstance && mapInstance.closePopup();
    document.getElementById('map-postal-input').value = postal;
    lastMapPostal = postal;
    initMapForPostal(postal);
}

function useMapAddressForPredict() {
    const btn = document.getElementById('map-predict-btn');
    const postal = btn && btn._postal;
    if (!postal) return;
    document.getElementById('input-postal').value = postal;
    showView('predict');
    setTimeout(() => handlePostalSearch(), 200);
}

// ── Amenity Filter Toggles ───────────────────────────────────
function toggleAmenityFilter(category, btn) {
    if (category === 'all') {
        // Toggle all on/off
        const allOn = amenityFilterState.all;
        const newState = !allOn;
        Object.keys(amenityFilterState).forEach(k => amenityFilterState[k] = newState);
        // Update all pill buttons
        document.querySelectorAll('.filter-pill').forEach(b => {
            b.classList.toggle('active', newState);
        });
        // Show/hide all markers
        Object.values(amenityMarkersByCategory).forEach(markers => {
            markers.forEach(m => {
                if (newState) { if (!mapInstance.hasLayer(m)) m.addTo(mapInstance); }
                else { if (mapInstance.hasLayer(m)) mapInstance.removeLayer(m); }
            });
        });
        return;
    }

    // Toggle single category
    const wasActive = amenityFilterState[category];
    amenityFilterState[category] = !wasActive;
    btn.classList.toggle('active', !wasActive);

    const markers = amenityMarkersByCategory[category] || [];
    markers.forEach(m => {
        if (!wasActive) { if (!mapInstance.hasLayer(m)) m.addTo(mapInstance); }
        else { if (mapInstance.hasLayer(m)) mapInstance.removeLayer(m); }
    });

    // Update "All" pill state
    const anyOff = Object.entries(amenityFilterState).some(([k,v]) => k !== 'all' && !v);
    const allOn  = !anyOff;
    amenityFilterState.all = allOn;
    const allBtn = document.querySelector('.filter-pill.filter-all');
    if (allBtn) allBtn.classList.toggle('active', allOn);
}

// ── Smart Search Autocomplete ────────────────────────────────
let _autocompleteDebounce = null;

async function fetchSearchSuggestions(query) {
    if (!query || query.length < 2) return [];
    try {
        const res = await fetch(`https://www.onemap.gov.sg/api/common/elastic/search?searchVal=${encodeURIComponent(query)}&returnGeom=N&getAddrDetails=Y&pageNum=1`);
        const data = await res.json();
        return (data.results || []).slice(0, 6);
    } catch { return []; }
}

function renderAutocomplete(containerId, results, onSelect) {
    const el = document.getElementById(containerId);
    if (!el) return;
    if (!results.length) { el.classList.remove('visible'); return; }

    el.innerHTML = results.map((r, i) => {
        const name = r.BUILDING && r.BUILDING !== 'NIL' ? r.BUILDING : r.ADDRESS;
        const sub  = r.POSTAL && r.POSTAL !== 'NIL' ? `Postal: ${r.POSTAL}` : (r.ROAD_NAME || '');
        const icon = r.POSTAL && r.POSTAL !== 'NIL' ? '📍' : '🗺️';
        return `<div class="autocomplete-item" data-idx="${i}">
            <div class="autocomplete-item-icon">${icon}</div>
            <div>
                <p class="autocomplete-item-name">${name || r.ADDRESS}</p>
                <p class="autocomplete-item-sub">${sub}</p>
            </div>
        </div>`;
    }).join('');

    el.querySelectorAll('.autocomplete-item').forEach((item, i) => {
        item.addEventListener('mousedown', (e) => {
            e.preventDefault();
            onSelect(results[i]);
            el.classList.remove('visible');
        });
    });
    el.classList.add('visible');
}

function closeAllDropdowns() {
    document.querySelectorAll('.autocomplete-dropdown').forEach(d => d.classList.remove('visible'));
}

document.addEventListener('click', (e) => {
    if (!e.target.closest('.autocomplete-wrap') && !e.target.closest('.hero-search')) {
        closeAllDropdowns();
    }
});

// Hero search autocomplete
function onHeroSearchInput(val) {
    clearTimeout(_autocompleteDebounce);
    if (!val.trim()) { closeAllDropdowns(); return; }
    _autocompleteDebounce = setTimeout(async () => {
        const results = await fetchSearchSuggestions(val);
        renderAutocomplete('hero-autocomplete', results, (r) => {
            const postal = r.POSTAL && r.POSTAL !== 'NIL' ? r.POSTAL : null;
            document.getElementById('hero-search-input').value = postal || r.ADDRESS;
            heroSearch();
        });
    }, 280);
}

function heroSearch() {
    closeAllDropdowns();
    const val = document.getElementById('hero-search-input').value.trim();
    if (!val) return;
    document.getElementById('input-postal').value = val;
    showView('predict');
    setTimeout(() => handlePostalSearch(), 200);
}

// Map search autocomplete
function onMapSearchInput(val) {
    clearTimeout(_autocompleteDebounce);
    if (!val.trim()) { closeAllDropdowns(); return; }
    _autocompleteDebounce = setTimeout(async () => {
        const results = await fetchSearchSuggestions(val);
        renderAutocomplete('map-autocomplete', results, (r) => {
            const postal = r.POSTAL && r.POSTAL !== 'NIL' ? r.POSTAL : null;
            document.getElementById('map-postal-input').value = postal || r.ADDRESS;
            loadMap();
        });
    }, 280);
}

// Predict search autocomplete
function onPredictSearchInput(val) {
    clearTimeout(_autocompleteDebounce);
    if (!val.trim()) { closeAllDropdowns(); return; }
    _autocompleteDebounce = setTimeout(async () => {
        const results = await fetchSearchSuggestions(val);
        renderAutocomplete('predict-autocomplete', results, (r) => {
            const postal = r.POSTAL && r.POSTAL !== 'NIL' ? r.POSTAL : null;
            document.getElementById('input-postal').value = postal || r.ADDRESS;
            handlePostalSearch();
        });
    }, 280);
}

// ── Dark Mode Nav Toggle ─────────────────────────────────────
function toggleDarkModeNav() {
    const html = document.documentElement;
    const isDark = html.classList.toggle('dark');
    localStorage.setItem('theme', isDark ? 'dark' : 'light');
    updateDarkModeNavIcon();
    // Also sync settings toggle if it exists
    const settingsToggle = document.getElementById('darkModeToggle');
    if (settingsToggle) settingsToggle.checked = isDark;
}

function updateDarkModeNavIcon() {
    const isDark = document.documentElement.classList.contains('dark');
    document.querySelectorAll('.dark-icon-sun').forEach(el => el.classList.toggle('hidden', !isDark));
    document.querySelectorAll('.dark-icon-moon').forEach(el => el.classList.toggle('hidden', isDark));
}

// ── Trend / Neighbourhood ────────────────────────────────────
const NEIGHBOURHOOD_NEWS = {
    'Clementi': [
        { headline: 'Clementi HDB resale prices hit new highs amid strong demand for mature estate flats', source: 'EdgeProp', date: 'Mar 2026', tag: 'Market', color: 'blue', url: 'https://www.edgeprop.sg/property-news/hdb-resale?district=D05' },
        { headline: '4-room flats in Clementi average S$650K as buyers eye proximity to NUS and one-north', source: 'PropertyGuru', date: 'Feb 2026', tag: 'Resale', color: 'emerald', url: 'https://www.propertyguru.com.sg/property-guides/hdb-resale-price-clementi' },
        { headline: 'New BTO launch near Clementi MRT expected mid-2026 — analysts project strong take-up', source: 'Straits Times', date: 'Jan 2026', tag: 'BTO', color: 'purple', url: 'https://www.straitstimes.com/search?keywords=clementi+bto+2026' },
        { headline: 'West Coast corridor sees 12% price jump YoY driven by tech cluster and NUS demand', source: 'Business Times', date: 'Dec 2025', tag: 'Analysis', color: 'amber', url: 'https://www.businesstimes.com.sg/search?q=west+coast+clementi+property' },
    ],
    'Queenstown': [
        { headline: 'Queenstown sees record S$1.4M resale flat as heritage charm drives central-living premium', source: 'EdgeProp', date: 'Mar 2026', tag: 'Record', color: 'rose', url: 'https://www.edgeprop.sg/property-news/hdb-resale?district=D03' },
        { headline: 'Greater Southern Waterfront masterplan to reshape Queenstown and Keppel waterfront', source: 'Straits Times', date: 'Feb 2026', tag: 'Planning', color: 'blue', url: 'https://www.straitstimes.com/search?keywords=greater+southern+waterfront+queenstown' },
        { headline: 'Commonwealth and Queenstown MRT corridors attract young families seeking central access', source: 'PropertyGuru', date: 'Jan 2026', tag: 'Demand', color: 'emerald', url: 'https://www.propertyguru.com.sg/property-for-sale?freetext=queenstown&district_code[]=D03' },
        { headline: 'Analysts: Queenstown en bloc potential remains high despite latest cooling measures', source: 'Business Times', date: 'Dec 2025', tag: 'En Bloc', color: 'purple', url: 'https://www.businesstimes.com.sg/search?q=queenstown+en+bloc+redevelopment' },
    ],
    'Hougang': [
        { headline: 'Hougang resale market heats up with North-East Line ridership growth and new amenities', source: 'PropertyGuru', date: 'Mar 2026', tag: 'Transport', color: 'purple', url: 'https://www.propertyguru.com.sg/property-guides/hougang-hdb-resale' },
        { headline: '3-room HDB flats in Hougang breach S$480K for first time in the estate\'s history', source: 'EdgeProp', date: 'Feb 2026', tag: 'Milestone', color: 'amber', url: 'https://www.edgeprop.sg/property-news/hdb-resale?district=D19' },
        { headline: 'Hougang Town rejuvenation programme to add new community amenities and park connectors by 2027', source: 'Straits Times', date: 'Jan 2026', tag: 'Upgrade', color: 'emerald', url: 'https://www.straitstimes.com/search?keywords=hougang+town+rejuvenation+HDB' },
        { headline: 'Hougang ranked top 5 most searched HDB estates in Q1 2026 on major portals', source: '99.co', date: 'Mar 2026', tag: 'Demand', color: 'blue', url: 'https://www.99.co/singapore/hdb-for-sale?district_code[]=D19' },
    ],
    'Toa Payoh': [
        { headline: 'Toa Payoh Lorong 1 BTO oversubscribed by 8x — demand outpaces available HDB supply', source: 'HDB', date: 'Mar 2026', tag: 'BTO', color: 'rose', url: 'https://homes.hdb.gov.sg/home/finding-a-flat/buying-from-hdb/flat-and-grant-info/types-of-flats/bto' },
        { headline: 'Toa Payoh estate rejuvenation lifts resale appeal with upgraded blocks and facilities', source: 'PropertyGuru', date: 'Feb 2026', tag: 'Upgrade', color: 'emerald', url: 'https://www.propertyguru.com.sg/property-for-sale?freetext=toa+payoh&district_code[]=D12' },
        { headline: 'Central location premium: Toa Payoh flats command 15% above district average in resale market', source: 'EdgeProp', date: 'Jan 2026', tag: 'Analysis', color: 'blue', url: 'https://www.edgeprop.sg/property-news/hdb-resale?district=D12' },
        { headline: 'New polyclinic and community club facilities set to boost Toa Payoh liveability scores', source: 'Straits Times', date: 'Dec 2025', tag: 'Amenity', color: 'purple', url: 'https://www.straitstimes.com/search?keywords=toa+payoh+polyclinic+community' },
    ],
    'Marina Bay': [
        { headline: 'Marina Bay luxury condos see record S$4,200 psf amid limited new supply and global demand', source: 'EdgeProp', date: 'Mar 2026', tag: 'Luxury', color: 'amber', url: 'https://www.edgeprop.sg/property-news/luxury?district=D01' },
        { headline: 'Foreign buyer activity returns to Marina Bay CBD after ABSD rate stabilisation', source: 'Business Times', date: 'Feb 2026', tag: 'Foreign', color: 'rose', url: 'https://www.businesstimes.com.sg/search?q=marina+bay+foreign+buyer+property+2026' },
        { headline: 'Marina Bay Financial Centre expansion drives rental premiums for adjacent condo units', source: 'PropertyGuru', date: 'Jan 2026', tag: 'Rental', color: 'blue', url: 'https://www.propertyguru.com.sg/property-for-rent?freetext=marina+bay&district_code[]=D01' },
        { headline: 'The Sail and Marina One Residences report 20% rental yield increase year-on-year', source: 'Straits Times', date: 'Dec 2025', tag: 'Yield', color: 'emerald', url: 'https://www.straitstimes.com/search?keywords=marina+bay+condo+rental+yield+2025' },
    ],
};

const ABSD_RATES = {
    sc1: 0,    // SC 1st
    sc2: 0.20, // SC 2nd
    sc3: 0.30, // SC 3rd+
    pr1: 0.05, // PR 1st
    pr2: 0.30, // PR 2nd+
    fg:  0.60, // Foreigner
    entity: 0.65,
};

const ABSD_LABELS = {
    sc1: 'Singapore Citizen · 1st Property',
    sc2: 'Singapore Citizen · 2nd Property',
    sc3: 'Singapore Citizen · 3rd+ Property',
    pr1: 'Permanent Resident · 1st Property',
    pr2: 'Permanent Resident · 2nd+ Property',
    fg:  'Foreigner',
    entity: 'Entity / Company',
};

function setNeighbourhood(name, btn) {
    currentNeighbourhood = name;
    document.querySelectorAll('.neighbourhood-btn').forEach(b => {
        b.className = 'neighbourhood-btn px-4 py-1.5 rounded-xl text-xs font-bold bg-slate-100 text-slate-600 hover:bg-slate-200 transition-colors';
    });
    btn.className = 'neighbourhood-btn px-4 py-1.5 rounded-xl text-xs font-bold bg-blue-600 text-white transition-colors';
    const subtitle = document.getElementById('trend-chart-subtitle');
    if (subtitle) subtitle.innerText = `Historical price index — ${name}`;
    renderTrendNews(name);
    setTimeout(() => initHDBTrend(currentRange), 50);
}

function _newsCardHTML(a) {
    return `
        <a href="${a.url}" target="_blank" rel="noopener noreferrer"
           class="flex items-start gap-4 p-4 rounded-2xl border border-slate-100 hover:bg-slate-50 transition-colors cursor-pointer group no-underline block">
            <div class="flex-1 min-w-0">
                <p class="text-sm font-semibold leading-snug group-hover:text-blue-600 transition-colors">${a.title}</p>
                <div class="flex items-center gap-2 mt-1">
                    <p class="text-xs text-slate-400">${a.source} · ${a.date}</p>
                    <i data-lucide="external-link" class="w-3 h-3 text-slate-300 group-hover:text-blue-400 transition-colors"></i>
                </div>
            </div>
        </a>`;
}

function _newsLoadingHTML() {
    return `<div class="animate-pulse space-y-3">
        ${[1,2,3].map(() => `<div class="h-12 bg-slate-100 rounded-xl w-full"></div>`).join('')}
    </div>`;
}

async function renderTrendNews(neighbourhood) {
    const list = document.getElementById('news-list');
    const subtitle = document.getElementById('news-subtitle');
    if (!list) return;
    if (subtitle) subtitle.innerText = `Latest headlines for ${neighbourhood}`;
    list.innerHTML = _newsLoadingHTML();

    try {
        const res = await fetch(`/api/news?neighbourhood=${encodeURIComponent(neighbourhood)}&limit=4`);
        if (!res.ok) throw new Error();
        const data = await res.json();
        const articles = data.articles || [];
        if (!articles.length) throw new Error();
        list.innerHTML = articles.map(_newsCardHTML).join('');
        lucide.createIcons();
    } catch {
        const fallback = NEIGHBOURHOOD_NEWS[neighbourhood] || [];
        if (fallback.length) {
            list.innerHTML = fallback.map(a => _newsCardHTML({
                title: a.headline, url: a.url, source: a.source, date: a.date
            })).join('');
        } else {
            list.innerHTML = '<p class="text-xs text-slate-400 p-4">No recent news found.</p>';
        }
        lucide.createIcons();
    }
}

function runABSDSimulation() {
    const profile = document.getElementById('absd-profile').value;
    const value   = parseFloat(document.getElementById('absd-value').value) || 0;
    const type    = document.getElementById('absd-type').value;

    const rate   = type === 'commercial' ? 0 : (ABSD_RATES[profile] ?? 0);
    const absd   = Math.round(value * rate);
    const bsd    = calcBSD(value);
    const total  = value + absd + bsd;

    const fmt = n => 'S$' + Math.round(n).toLocaleString();
    const pct = (rate * 100).toFixed(0) + '%';

    const resultEl = document.getElementById('absd-result');
    const absdColor = absd === 0 ? 'emerald' : absd > 100000 ? 'rose' : 'amber';
    resultEl.innerHTML = `
        <div class="bg-slate-50 rounded-2xl p-4 border border-slate-100">
            <p class="text-xs text-slate-400 font-medium mb-1">Property Value</p>
            <p class="text-lg font-bold">${fmt(value)}</p>
        </div>
        <div class="bg-slate-50 rounded-2xl p-4 border border-slate-100">
            <p class="text-xs text-slate-400 font-medium mb-1">Buyer Stamp Duty</p>
            <p class="text-lg font-bold text-blue-600">${fmt(bsd)}</p>
        </div>
        <div class="bg-${absdColor}-50 rounded-2xl p-4 border border-${absdColor}-100">
            <p class="text-xs text-${absdColor}-600 font-medium mb-1">ABSD (${pct})</p>
            <p class="text-lg font-bold text-${absdColor}-600">${absd === 0 ? 'Exempt' : fmt(absd)}</p>
        </div>
        <div class="bg-slate-900 rounded-2xl p-4">
            <p class="text-xs text-slate-400 font-medium mb-1">Total Cost</p>
            <p class="text-lg font-bold text-white">${fmt(total)}</p>
        </div>
    `;

    const alertsEl = document.getElementById('policy-alerts');
    const policies = getPolicyAlerts(profile, value, rate);
    alertsEl.innerHTML = policies.map(p => `
        <div class="flex items-start gap-3 p-4 rounded-2xl border ${p.borderClass}">
            <i data-lucide="${p.icon}" class="w-4 h-4 ${p.iconClass} shrink-0 mt-0.5"></i>
            <div>
                <p class="text-sm font-bold ${p.titleClass}">${p.title}</p>
                <p class="text-xs text-slate-500 mt-0.5">${p.body}</p>
            </div>
        </div>
    `).join('');
    lucide.createIcons();
}

function calcBSD(value) {
    let bsd = 0;
    const brackets = [
        [180000, 0.01],
        [180000, 0.02],
        [640000, 0.03],
        [500000, 0.04],
        [1500000, 0.05],
        [Infinity, 0.06],
    ];
    let remaining = value;
    for (const [cap, rate] of brackets) {
        const taxable = Math.min(remaining, cap);
        bsd += taxable * rate;
        remaining -= taxable;
        if (remaining <= 0) break;
    }
    return Math.round(bsd);
}

function getPolicyAlerts(profile, value, rate) {
    const alerts = [];
    if (profile === 'sc1') {
        alerts.push({ icon: 'check-circle', borderClass: 'border-emerald-100 bg-emerald-50', iconClass: 'text-emerald-600', titleClass: 'text-emerald-800', title: 'No ABSD applicable', body: 'Singapore Citizens purchasing their first residential property are exempted from ABSD under current MAS guidelines.' });
    }
    if (profile === 'sc2') {
        alerts.push({ icon: 'alert-triangle', borderClass: 'border-amber-100 bg-amber-50', iconClass: 'text-amber-600', titleClass: 'text-amber-800', title: '20% ABSD on 2nd property', body: 'Consider selling your existing property first to avoid ABSD. A Decoupling strategy may also reduce liability if co-owning.' });
    }
    if (profile === 'fg') {
        alerts.push({ icon: 'alert-circle', borderClass: 'border-rose-100 bg-rose-50', iconClass: 'text-rose-600', titleClass: 'text-rose-800', title: '60% ABSD for foreigners (Apr 2023)', body: 'Singapore doubled ABSD for foreign buyers in April 2023 as part of cooling measures. This significantly impacts investment returns.' });
    }
    if (profile === 'entity') {
        alerts.push({ icon: 'alert-circle', borderClass: 'border-rose-100 bg-rose-50', iconClass: 'text-rose-600', titleClass: 'text-rose-800', title: '65% ABSD for entities', body: 'Entities purchasing residential property face the highest ABSD rate. Developers must complete and sell within 5 years to claim remission.' });
    }
    if (value >= 1500000) {
        alerts.push({ icon: 'info', borderClass: 'border-blue-100 bg-blue-50', iconClass: 'text-blue-600', titleClass: 'text-blue-800', title: 'Total Debt Servicing Ratio (TDSR) applies', body: 'Properties above S$1.5M require financing to pass TDSR of 55% and Mortgage Servicing Ratio (MSR) of 30% for HDB loans.' });
    }
    alerts.push({ icon: 'info', borderClass: 'border-slate-100 bg-slate-50', iconClass: 'text-slate-500', titleClass: 'text-slate-700', title: 'LTV limit: 75% for first property loan', body: 'Max loan-to-value ratio is 75% for a first housing loan from banks. Minimum cash down payment of 5% is required.' });
    return alerts;
}

// ── Trend Chart ───────────────────────────────────────────────
let trendChart;
let currentRange = '6m';

const NEIGHBOURHOOD_BASE = {
    'Clementi':   { base: 430000, growth: 0.018 },
    'Queenstown': { base: 520000, growth: 0.022 },
    'Hougang':    { base: 380000, growth: 0.015 },
    'Toa Payoh':  { base: 460000, growth: 0.020 },
    'Marina Bay': { base: 1800000, growth: 0.031 },
};

function generateNeighbourhoodPrices(neighbourhood, range) {
    const cfg = NEIGHBOURHOOD_BASE[neighbourhood] || NEIGHBOURHOOD_BASE['Clementi'];
    const months = range === '6m' ? 6 : range === '1y' ? 12 : range === '3y' ? 36 : range === '5y' ? 60 : 24;
    const step = months >= 36 ? 3 : 1;
    const labels = [], prices = [];
    const now = new Date();
    let p = cfg.base * Math.pow(1 - cfg.growth / 12, months);
    for (let i = months; i >= 0; i--) {
        if (i % step !== 0) {
            p = p * (1 + cfg.growth / 12 + (Math.sin(i * 0.7) * 0.002));
            continue;
        }
        const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
        labels.push(d.toLocaleString('en-SG', { month: 'short', year: '2-digit' }));
        p = p * (1 + cfg.growth / 12 + (Math.sin(i * 0.7) * 0.002));
        prices.push(Math.round(p));
    }
    return { labels, prices };
}

async function initTrendChart(range = currentRange) {
    const canvas = document.getElementById('trendChart');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    if (trendChart) trendChart.destroy();

    const isDark = document.documentElement.classList.contains('dark');
    const gradient = ctx.createLinearGradient(0, 0, 0, 380);
    gradient.addColorStop(0, 'rgba(99, 179, 237, 0.35)');
    gradient.addColorStop(1, 'rgba(99, 179, 237, 0.0)');

    let labels = [], prices = [];

    try {
        const res = await fetch(`/api/trend?town=${currentNeighbourhood}`);
        if (!res.ok) throw new Error();
        const data = await res.json();
        if (data.trend_data && data.trend_data.length) {
            const cfg = NEIGHBOURHOOD_BASE[currentNeighbourhood] || NEIGHBOURHOOD_BASE['Clementi'];
            const apiAvg = data.trend_data.reduce((s, d) => s + d.price, 0) / data.trend_data.length;
            const scale = cfg.base / apiAvg;
            labels = data.trend_data.map(d => d.month);
            prices = data.trend_data.map(d => Math.round(d.price * scale));
        } else throw new Error();
        loadComparableTable(data);
        loadNearestSale(data);
    } catch {
        const gen = generateNeighbourhoodPrices(currentNeighbourhood, range);
        labels = gen.labels;
        prices = gen.prices;
    }

    const tickColor  = isDark ? '#93C5FD' : '#64748B';
    const gridColor = isDark ? 'rgba(147,197,253,0.08)' : 'rgba(0,0,0,0.04)';

    trendChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels,
            datasets: [{
                label: 'Avg Price (S$)',
                data: prices,
                borderColor: '#3B82F6',
                borderWidth: 3,
                fill: true,
                backgroundColor: gradient,
                tension: 0.4,
                pointRadius: 3,
                pointBackgroundColor: '#3B82F6',
                pointBorderColor: isDark ? '#1E3A5F' : '#fff',
                pointBorderWidth: 2,
                pointHoverRadius: 7,
                pointHoverBackgroundColor: '#60A5FA',
                pointHoverBorderColor: '#fff',
                pointHoverBorderWidth: 3
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { intersect: false, mode: 'index' },
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: isDark ? '#1E3A5F' : '#1E293B',
                    titleColor: '#93C5FD',
                    bodyColor: '#E2E8F0',
                    padding: 14,
                    titleFont: { size: 13, weight: 'bold' },
                    bodyFont: { size: 13 },
                    cornerRadius: 10,
                    displayColors: false,
                    callbacks: {
                        label: ctx => `Avg Price (S$): ${ctx.parsed.y.toLocaleString()}`
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: false,
                    grid: { color: gridColor, drawBorder: false },
                    ticks: { font: { size: 11, weight: '600' }, color: tickColor,
                        callback: v => v >= 1000000 ? `${(v/1000000).toFixed(1)}M` : `${(v/1000).toFixed(0)}K` }
                },
                x: {
                    grid: { display: false },
                    ticks: { font: { size: 11, weight: '600' }, color: tickColor }
                }
            }
        }
    });
}

// ── Home Tab News ────────────────────────────────────────────
async function renderHomeNews() {
    const list = document.getElementById('home-news-list');
    if (!list) return;
    list.innerHTML = _newsLoadingHTML();

    try {
        const res = await fetch('/api/news?limit=6');
        if (!res.ok) throw new Error();
        const data = await res.json();
        const articles = data.articles || [];
        if (!articles.length) throw new Error();
        list.innerHTML = articles.map(_newsCardHTML).join('');
    } catch {
        list.innerHTML = '<p class="text-xs text-slate-400 p-4">Could not load property news. Check back shortly.</p>';
    }
    lucide.createIcons();
}

// ── Predict Tab Neighbourhood News ───────────────────────────
async function renderPredictNews(postal) {
    const section = document.getElementById('predict-news-section');
    const list    = document.getElementById('predict-news-list');
    const label   = document.getElementById('predict-news-area');
    if (!section || !list) return;

    section.classList.remove('hidden');
    list.innerHTML = _newsLoadingHTML();

    try {
        const res = await fetch(`/api/news?postal=${encodeURIComponent(postal)}&limit=4`);
        if (!res.ok) throw new Error();
        const data = await res.json();
        const articles = data.articles || [];
        if (label) label.innerText = `Latest news for ${data.area || 'this area'}`;
        if (!articles.length) throw new Error();
        list.innerHTML = articles.map(_newsCardHTML).join('');
        lucide.createIcons();
    } catch {
        section.classList.add('hidden');
    }
}

document.addEventListener('DOMContentLoaded', () => {
    initHDBTrend();
    renderTrendNews(currentNeighbourhood);
    runABSDSimulation();
    renderHomeNews();
    loadMarketWatch();
});

function changeRange(range, btn) {
    currentRange = range;

    document.querySelectorAll('.range-btn').forEach(b => {
        b.classList.remove('bg-white', 'shadow-sm');
        b.classList.add('text-slate-500');
    });

    btn.classList.add('bg-white', 'shadow-sm');
    btn.classList.remove('text-slate-500');

    setHDBRange(range, btn);
}

function loadNearestSale(data) {
    const list = data.similar_transactions || [];
    if (!list.length) return;

    const first = list[0];

    const priceEl = document.getElementById('nearest-price');
    const addressEl = document.getElementById('nearest-address');

    if (priceEl) priceEl.innerText = `S$${first.price.toLocaleString()}`;
    if (addressEl) addressEl.innerText = first.address;
}


function loadComparableTable(data) {
    const tableBody = document.getElementById('comparable-body');
    if (!tableBody) return;

    tableBody.innerHTML = '';

    const list = data.similar_transactions || [];

    list.forEach(item => {
        const row = document.createElement('tr');
        row.className = "hover:bg-slate-50/50 transition-colors group";

        row.innerHTML = `
            <td class="py-8 pl-4">
                <p class="font-bold text-slate-900">${item.address}</p>
                <p class="text-xs text-slate-400">-</p>
            </td>
            <td class="py-8 text-sm font-medium text-slate-600">${item.type}</td>
            <td class="py-8">
                <span class="font-bold text-slate-900">S$${item.price.toLocaleString()}</span>
            </td>
            <td class="py-8 text-sm text-slate-500">-</td>
            <td class="py-8 pr-4 text-right">
                <span class="px-3 py-1 bg-slate-100 rounded-lg text-[10px] font-bold">${item.date}</span>
            </td>
        `;

        tableBody.appendChild(row);
    });
}

let adminTypeChart;

async function initAdminTypeChart() {
    const canvas = document.getElementById('adminTypeChart');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    if (adminTypeChart) adminTypeChart.destroy();

    let labels = [];
    let dataValues = [];

    try {
        const res = await fetch('/api/stats');
        if (!res.ok) throw new Error('Stats API failed');

        const stats = await res.json();

        labels = ['Total Predictions'];
        dataValues = [stats.total_predictions];

        document.getElementById('admin-users').innerText = stats.total_users || 0;
        document.getElementById('admin-predictions').innerText = stats.total_predictions || 0;
        document.getElementById('admin-db').innerText = stats.db_size || '-';

    } catch (err) {
        console.error('Admin stats failed:', err);
        labels = ['No Data'];
        dataValues = [0];
    }

    adminTypeChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Predictions',
                data: dataValues,
                backgroundColor: '#0F172A',
                borderRadius: 8,
                barThickness: 24
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: '#0F172A',
                    padding: 12,
                    cornerRadius: 8
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: { color: 'rgba(0,0,0,0.03)', drawBorder: false },
                    ticks: { font: { weight: 'bold' }, color: '#94A3B8' }
                },
                x: {
                    grid: { display: false },
                    ticks: { font: { weight: 'bold' }, color: '#94A3B8' }
                }
            }
        }
    });
}

let allUsers = [];

async function loadAdminUsers() {
    try {
        const res = await fetch('/api/users');
        const data = await res.json();
        allUsers = data.users || [];
        renderUsers(allUsers);
    } catch (err) {
        console.error('Failed to load users:', err);
    }
}

function searchUsers(query) {
    const q = query.toLowerCase();
    const filtered = allUsers.filter(user =>
        user.full_name.toLowerCase().includes(q) ||
        user.email.toLowerCase().includes(q)
    );
    renderUsers(filtered);
}

function renderUsers(users) {
    const tbody = document.getElementById('user-table-body');
    if (!tbody) return;

    if (!users.length) {
        tbody.innerHTML = '<tr><td colspan="3" class="py-8 text-center text-slate-400 text-sm">No users found</td></tr>';
        return;
    }

    tbody.innerHTML = users.map(user => {
        const initials = user.full_name.split(' ').map(n => n[0]).join('').toUpperCase().slice(0, 2);
        const isAdmin = user.role === 'admin';
        return `
            <tr class="group">
                <td class="py-5 pl-4">
                    <div class="flex items-center gap-3">
                        <div class="w-8 h-8 rounded-full border-2 border-slate-900 flex items-center justify-center text-xs font-bold">${initials}</div>
                        <div>
                            <p class="text-sm font-bold text-slate-900">${user.full_name}</p>
                            <p class="text-[10px] text-slate-400">${user.email}</p>
                        </div>
                    </div>
                </td>
                <td class="py-5">
                    <span class="px-2 py-1 ${isAdmin ? 'bg-blue-600' : 'bg-emerald-500'} text-white rounded text-[10px] font-bold">${isAdmin ? 'Admin' : 'User'}</span>
                </td>
                <td class="py-5 pr-4 text-right">
                    <div class="flex justify-end gap-3">
                        <button onclick="updateUserRole(${user.id}, '${isAdmin ? 'user' : 'admin'}')" class="p-1.5 hover:bg-slate-100 rounded-lg transition-colors" title="${isAdmin ? 'Demote to User' : 'Promote to Admin'}">
                            <i data-lucide="${isAdmin ? 'shield-off' : 'shield-check'}" class="w-4 h-4 text-slate-400"></i>
                        </button>
                        <button onclick="deleteUser(${user.id})" class="p-1.5 hover:bg-red-50 rounded-lg transition-colors" title="Delete user">
                            <i data-lucide="trash-2" class="w-4 h-4 text-red-400"></i>
                        </button>
                    </div>
                </td>
            </tr>
        `;
    }).join('');

    lucide.createIcons();
}

async function updateUserRole(id, newRole) {
    const label = newRole === 'admin' ? 'promote to Admin' : 'demote to User';
    if (!confirm(`Are you sure you want to ${label}?`)) return;

    const res = await fetch(`/api/users/${id}/role`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ role: newRole })
    });
    const data = await res.json();

    if (data.error) {
        alert(data.error);
        return;
    }

    showToast(`User role updated to ${newRole}`);
    loadAdminUsers();
}

async function deleteUser(id) {
    if (!confirm('Delete this user? This action cannot be undone.')) return;

    const res = await fetch(`/api/users/${id}`, { method: 'DELETE' });
    const data = await res.json();

    if (data.error) {
        alert(data.error);
        return;
    }

    showToast('User deleted');
    loadAdminUsers();
    fetchAdminStats();
}



// ── Admin ────────────────────────────────────────────────────
async function fetchAdminStats() {
    try {
        const response = await fetch('/api/stats');
        const data = await response.json();
        document.getElementById('admin-users').innerText = data.total_users.toLocaleString();
        document.getElementById('admin-predictions').innerText = data.total_predictions.toLocaleString();
        document.getElementById('admin-db').innerText = data.db_size;
        const totalRecordsEl = document.getElementById('admin-total-records');
        if (totalRecordsEl) totalRecordsEl.innerText = (data.total_records || 0).toLocaleString();

        const recentUsersList = document.getElementById('admin-recent-users');
        if (recentUsersList && data.recent_users) {
            recentUsersList.innerHTML = data.recent_users.map(u => `
                <div class="flex items-center gap-3">
                    <div class="w-2 h-2 rounded-full border-2 border-slate-900"></div>
                    <div>
                        <p class="text-sm font-bold text-slate-900">${u.full_name}</p>
                        <p class="text-[10px] text-slate-400">${u.email}</p>
                    </div>
                </div>
            `).join('') || '<p class="text-xs text-slate-400">No users yet</p>';
        }

        const statsList = document.getElementById('system-stats');
        const metrics = [
            { name: 'CPU Utilization', value: 42, color: 'bg-blue-600' },
            { name: 'Memory Usage', value: 68, color: 'bg-purple-600' },
            { name: 'API Latency (avg)', value: 12, unit: 'ms', color: 'bg-emerald-600' }
        ];

        statsList.innerHTML = metrics.map(m => `
            <div class="space-y-4">
                <div class="flex justify-between items-center">
                    <span class="font-bold text-slate-700">${m.name}</span>
                    <span class="font-mono font-bold text-slate-900">${m.value}${m.unit || '%'}</span>
                </div>
                <div class="w-full h-4 bg-slate-50 rounded-full overflow-hidden border border-slate-100">
                    <div class="h-full ${m.color} rounded-full transition-all duration-1000" style="width: ${m.value}%"></div>
                </div>
            </div>
        `).join('');
    } catch (e) { console.error(e); }
}

// ── Auth ─────────────────────────────────────────────────────
let currentUser = null;

function getProfilePhotoKey() {
    if (!currentUser || !currentUser.id) return null;
    return `profilePhoto_${currentUser.id}`;
}

function toggleDarkMode() {
    const html = document.documentElement;
    const toggle = document.getElementById('darkModeToggle');

    if (!toggle) return;

    if (toggle.checked) {
        html.classList.add('dark');
        localStorage.setItem('theme', 'dark');
    } else {
        html.classList.remove('dark');
        localStorage.setItem('theme', 'light');
    }
}

function loadTheme() {
    const savedTheme = localStorage.getItem('theme') ?? 'light';
    const toggle = document.getElementById('darkModeToggle');

    if (savedTheme !== 'light') {
        document.documentElement.classList.add('dark');
        if (toggle) toggle.checked = true;
    } else {
        document.documentElement.classList.remove('dark');
        if (toggle) toggle.checked = false;
    }
    updateDarkModeNavIcon();
}

function saveCurrentUser() {
    localStorage.setItem('currentUser', JSON.stringify(currentUser));
}

function loadCurrentUser() {
    const saved = localStorage.getItem('currentUser');
    if (saved) {
        currentUser = JSON.parse(saved);
    } else {
        currentUser = null;
    }

    updateAuthUI();
    loadProfileForm();
    loadPreferencesForm();
}

async function handleSignIn(e) {
    if (e) e.preventDefault();

    const inputs = document.querySelectorAll('#view-signin input');
    const email = inputs[0].value.trim();
    const password = inputs[1].value.trim();

    const res = await fetch('/api/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, password })
    });

    const data = await res.json();

    if (data.error) {
        const el = document.getElementById('signin-error');
        el.textContent = data.error;
        el.classList.remove('hidden');
        return;
    }

    document.getElementById('signin-error').classList.add('hidden');
    currentUser = data.user;
    saveCurrentUser();
    updateAuthUI();
    loadProfileForm();
    showView('home');
}

async function handleRegister(e) {
    if (e) e.preventDefault();

    const inputs = document.querySelectorAll('#view-register input');
    const full_name = inputs[0].value.trim();
    const email = inputs[1].value.trim();
    const password = inputs[2].value.trim();
    const errEl = document.getElementById('register-error');

    if (!full_name || !email || !password) {
        errEl.textContent = 'All fields are required.';
        errEl.classList.remove('hidden');
        return;
    }
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
        errEl.textContent = 'Please enter a valid email address.';
        errEl.classList.remove('hidden');
        return;
    }

    const res = await fetch('/api/register', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ full_name, email, password })
    });

    const text = await res.text();
    console.log('register response:', text);

    let data;
    try {
        data = JSON.parse(text);
    } catch (err) {
        const el = document.getElementById('register-error');
        el.textContent = 'Unexpected error. Please try again.';
        el.classList.remove('hidden');
        return;
    }

    if (data.error) {
        const el = document.getElementById('register-error');
        el.textContent = data.error;
        el.classList.remove('hidden');
        return;
    }

    document.getElementById('register-error').classList.add('hidden');
    currentUser = data.user;
    saveCurrentUser();
    updateAuthUI();
    loadProfileForm();
    showView('home');
}

async function handleForgotPassword() {
    const email = document.getElementById('forgot-email').value.trim();
    const errEl = document.getElementById('forgot-error');
    const successEl = document.getElementById('forgot-success');

    errEl.classList.add('hidden');
    successEl.classList.add('hidden');

    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
        errEl.textContent = 'Please enter a valid email address.';
        errEl.classList.remove('hidden');
        return;
    }

    try {
        const res = await fetch('/api/forgot-password', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ email })
        });
        const data = await res.json();

        if (data.error) {
            errEl.textContent = data.error;
            errEl.classList.remove('hidden');
        } else {
            successEl.classList.remove('hidden');
        }
    } catch {
        errEl.textContent = 'Something went wrong. Please try again.';
        errEl.classList.remove('hidden');
    }
}

function clearAuthForms() {
    document.querySelectorAll('#view-signin input, #view-register input, #view-forgot input').forEach(input => {
        input.value = '';
    });
    ['signin-error', 'register-error', 'forgot-error', 'forgot-success'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.classList.add('hidden');
    });
}

function handleLogout() {
    currentUser = null;
    localStorage.removeItem('currentUser');

    updateAuthUI();
    clearProfileForm();
    clearAuthForms();

    showView('signin');
}

function updateAuthUI() {
    const authButtons = document.getElementById('nav-auth-buttons');
    const userProfile = document.getElementById('nav-user-profile');
    const userName = document.getElementById('nav-user-name');
    const userAvatar = document.getElementById('nav-user-avatar');
    const adminBtns = [
        document.getElementById('admin-panel-btn'),
        ...document.querySelectorAll('.admin-panel-btn')
    ].filter(Boolean);

    if (currentUser) {
        authButtons.classList.add('hidden');
        userProfile.classList.remove('hidden');
        userProfile.classList.add('flex');
        userName.innerText = currentUser.full_name;

        const photoKey = getProfilePhotoKey();
        const savedPhoto = photoKey ? localStorage.getItem(photoKey) : null;
        userAvatar.src = savedPhoto || "https://picsum.photos/seed/user/100/100";

        adminBtns.forEach(btn => {
            if (currentUser.role === 'admin') {
                btn.classList.remove('hidden');
                btn.classList.add('flex');
            } else {
                btn.classList.add('hidden');
                btn.classList.remove('flex');
            }
        });

    } else {
        authButtons.classList.remove('hidden');
        userProfile.classList.add('hidden');
        userProfile.classList.remove('flex');

        adminBtns.forEach(btn => {
            btn.classList.add('hidden');
            btn.classList.remove('flex');
        });
    }

    lucide.createIcons();
}

function clearProfileForm() {
    const firstNameInput = document.getElementById('profile-first-name');
    const lastNameInput = document.getElementById('profile-last-name');
    const emailInput = document.getElementById('profile-email');
    const phoneInput = document.getElementById('profile-phone');

    if (firstNameInput) firstNameInput.value = '';
    if (lastNameInput) lastNameInput.value = '';
    if (emailInput) emailInput.value = '';
    if (phoneInput) phoneInput.value = '';
}

function loadProfileForm() {
    const firstNameInput = document.getElementById('profile-first-name');
    const lastNameInput = document.getElementById('profile-last-name');
    const emailInput = document.getElementById('profile-email');
    const phoneInput = document.getElementById('profile-phone');

    if (!firstNameInput || !lastNameInput || !emailInput || !phoneInput) return;

    if (!currentUser) {
        clearProfileForm();
        return;
    }

    const fullName = currentUser.full_name || '';
    const parts = fullName.split(' ');
    const firstName = parts[0] || '';
    const lastName = parts.slice(1).join(' ') || '';

    firstNameInput.value = firstName;
    lastNameInput.value = lastName;
    emailInput.value = currentUser.email || '';
    phoneInput.value = currentUser.phone || '';
}

function cancelProfileChanges() {
    loadProfileForm();
    loadProfilePhoto();
}

async function saveProfile() {
    if (!currentUser) {
        alert('Please login first');
        return;
    }

    const firstNameInput = document.getElementById('profile-first-name');
    const lastNameInput = document.getElementById('profile-last-name');
    const emailInput = document.getElementById('profile-email');
    const phoneInput = document.getElementById('profile-phone');

    const firstName = firstNameInput.value.trim();
    const lastName = lastNameInput.value.trim();
    const email = emailInput.value.trim();
    const phone = phoneInput.value.trim();

    const full_name = `${firstName} ${lastName}`.trim();

    const res = await fetch(`/api/profile/${currentUser.id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ full_name, email, phone })
    });

    const data = await res.json();

    if (data.error) {
        alert(data.error);
        return;
    }

    currentUser = data.user;
    saveCurrentUser();
    updateAuthUI();
    loadProfileForm();
    showToast('Profile updated successfully');
}

// ── Profile Photo Upload ──────────────────────────────────────
function handleProfilePhotoUpload(event) {
    if (!currentUser) {
        alert('Please login first');
        return;
    }

    const file = event.target.files[0];
    if (!file) return;

    const reader = new FileReader();

    reader.onload = function(e) {
        const photoData = e.target.result;
        const preview = document.getElementById('profile-preview');
        const icon = document.getElementById('profile-camera-icon');
        const navAvatar = document.getElementById('nav-user-avatar');
        const photoKey = getProfilePhotoKey();

        if (photoKey) {
            localStorage.setItem(photoKey, photoData);
        }

        if (preview) {
            preview.src = photoData;
            preview.classList.remove('hidden');
        }

        if (icon) {
            icon.classList.add('hidden');
        }

        if (navAvatar) {
            navAvatar.src = photoData;
        }
    };

    reader.readAsDataURL(file);
}

function removeProfilePhoto() {
    const preview = document.getElementById('profile-preview');
    const icon = document.getElementById('profile-camera-icon');
    const navAvatar = document.getElementById('nav-user-avatar');
    const photoKey = getProfilePhotoKey();

    if (photoKey) {
        localStorage.removeItem(photoKey);
    }

    if (preview) {
        preview.src = '';
        preview.classList.add('hidden');
    }

    if (icon) {
        icon.classList.remove('hidden');
    }

    if (navAvatar) {
        navAvatar.src = "https://picsum.photos/seed/user/100/100";
    }
}

function loadProfilePhoto() {
    const photoKey = getProfilePhotoKey();
    const savedPhoto = photoKey ? localStorage.getItem(photoKey) : null;

    const preview = document.getElementById('profile-preview');
    const icon = document.getElementById('profile-camera-icon');
    const navAvatar = document.getElementById('nav-user-avatar');

    if (!preview || !icon) return;

    if (savedPhoto) {
        preview.src = savedPhoto;
        preview.classList.remove('hidden');
        icon.classList.add('hidden');

        if (navAvatar) {
            navAvatar.src = savedPhoto;
        }
    } else {
        preview.src = '';
        preview.classList.add('hidden');
        icon.classList.remove('hidden');

        if (navAvatar) {
            navAvatar.src = "https://picsum.photos/seed/user/100/100";
        }
    }
}

let originalPreferences = {
    darkMode: false,
    currency: 'Singapore Dollar ($)',
    dateFormat: 'DD/MM/YYYY',
    shareAnalytics: true
};

let originalNotifications = {
    email: true,
    market: true,
    sms: true
};

function loadPreferencesForm() {
    const darkModeToggle = document.getElementById('darkModeToggle');
    const currencySelect = document.getElementById('preferences-currency');
    const dateFormatSelect = document.getElementById('preferences-date-format');
    const shareAnalyticsToggle = document.getElementById('preferences-share-analytics');

    const savedPreferences = JSON.parse(localStorage.getItem('preferences')) || {
        darkMode: localStorage.getItem('theme') === 'dark',
        currency: 'Singapore Dollar ($)',
        dateFormat: 'DD/MM/YYYY',
        shareAnalytics: true
    };

    originalPreferences = { ...savedPreferences };

    if (darkModeToggle) darkModeToggle.checked = !!savedPreferences.darkMode;
    if (currencySelect) currencySelect.value = savedPreferences.currency;
    if (dateFormatSelect) dateFormatSelect.value = savedPreferences.dateFormat;
    if (shareAnalyticsToggle) shareAnalyticsToggle.checked = !!savedPreferences.shareAnalytics;
}

function savePreferences() {
    const darkModeToggle = document.getElementById('darkModeToggle');
    const currencySelect = document.getElementById('preferences-currency');
    const dateFormatSelect = document.getElementById('preferences-date-format');
    const shareAnalyticsToggle = document.getElementById('preferences-share-analytics');

    const preferences = {
        darkMode: darkModeToggle ? darkModeToggle.checked : false,
        currency: currencySelect ? currencySelect.value : 'Singapore Dollar ($)',
        dateFormat: dateFormatSelect ? dateFormatSelect.value : 'DD/MM/YYYY',
        shareAnalytics: shareAnalyticsToggle ? shareAnalyticsToggle.checked : true
    };

    localStorage.setItem('preferences', JSON.stringify(preferences));
    originalPreferences = { ...preferences };

    if (preferences.darkMode) {
        document.documentElement.classList.add('dark');
        localStorage.setItem('theme', 'dark');
    } else {
        document.documentElement.classList.remove('dark');
        localStorage.setItem('theme', 'light');
    }

    showToast('Preferences updated successfully');
}

function cancelPreferencesChanges() {
    const darkModeToggle = document.getElementById('darkModeToggle');
    const currencySelect = document.getElementById('preferences-currency');
    const dateFormatSelect = document.getElementById('preferences-date-format');
    const shareAnalyticsToggle = document.getElementById('preferences-share-analytics');

    if (darkModeToggle) darkModeToggle.checked = !!originalPreferences.darkMode;
    if (currencySelect) currencySelect.value = originalPreferences.currency;
    if (dateFormatSelect) dateFormatSelect.value = originalPreferences.dateFormat;
    if (shareAnalyticsToggle) shareAnalyticsToggle.checked = !!originalPreferences.shareAnalytics;

    if (originalPreferences.darkMode) {
        document.documentElement.classList.add('dark');
        localStorage.setItem('theme', 'dark');
    } else {
        document.documentElement.classList.remove('dark');
        localStorage.setItem('theme', 'light');
    }
}

function resetPreferencesFormToDefaults() {
    const darkModeToggle = document.getElementById('darkModeToggle');
    const currencySelect = document.getElementById('preferences-currency');
    const dateFormatSelect = document.getElementById('preferences-date-format');
    const shareAnalyticsToggle = document.getElementById('preferences-share-analytics');

    if (darkModeToggle) darkModeToggle.checked = false;
    if (currencySelect) currencySelect.value = 'Singapore Dollar ($)';
    if (dateFormatSelect) dateFormatSelect.value = 'DD/MM/YYYY';
    if (shareAnalyticsToggle) shareAnalyticsToggle.checked = true;

    originalPreferences = {
        darkMode: false,
        currency: 'Singapore Dollar ($)',
        dateFormat: 'DD/MM/YYYY',
        shareAnalytics: true
    };
}

function loadNotificationsForm() {
    const notifEmail = document.getElementById('notif-email');
    const notifMarket = document.getElementById('notif-market');
    const notifSms = document.getElementById('notif-sms');

    const savedNotifications = JSON.parse(localStorage.getItem('notifications')) || {
        email: true,
        market: true,
        sms: true
    };

    originalNotifications = { ...savedNotifications };

    if (notifEmail) notifEmail.checked = !!savedNotifications.email;
    if (notifMarket) notifMarket.checked = !!savedNotifications.market;
    if (notifSms) notifSms.checked = !!savedNotifications.sms;
}

function resetNotifications() {
    const notifEmail = document.getElementById('notif-email');
    const notifMarket = document.getElementById('notif-market');
    const notifSms = document.getElementById('notif-sms');

    if (notifEmail) notifEmail.checked = true;
    if (notifMarket) notifMarket.checked = true;
    if (notifSms) notifSms.checked = true;
}

function saveNotifications() {
    const notifEmail = document.getElementById('notif-email');
    const notifMarket = document.getElementById('notif-market');
    const notifSms = document.getElementById('notif-sms');

    const notifications = {
        email: notifEmail ? notifEmail.checked : true,
        market: notifMarket ? notifMarket.checked : true,
        sms: notifSms ? notifSms.checked : true
    };

    localStorage.setItem('notifications', JSON.stringify(notifications));
    originalNotifications = { ...notifications };
    showToast('Notifications updated successfully');
}

function restoreLastView() {
    const savedView = localStorage.getItem('currentView') || 'home';
    const savedSettingsTab = localStorage.getItem('currentSettingsTab') || 'profile';
    const savedAdminTab = localStorage.getItem('currentAdminTab') || 'overview';

    if (savedView === 'setting' && !currentUser) {
        showView('signin');
        return;
    }

    showView(savedView);

    if (savedView === 'setting') {
        showTab(savedSettingsTab);
    }

    if (savedView === 'admin') {
        showAdminTab(savedAdminTab);
    }
}

function showToast(message) {
    const toast = document.getElementById('toast-message');
    if (!toast) return;

    toast.textContent = message;
    toast.classList.remove('hidden');

    setTimeout(() => {
        toast.classList.add('hidden');
    }, 2500);
}

loadCurrentUser();
loadTheme();
loadProfilePhoto();
loadPreferencesForm();
restoreLastView();
lucide.createIcons();

// ─────────────────────────────────────────────────────────────────────────────
// HDB RESALE TREND  —  paste this entire block at the bottom of frontend/app.js
// ─────────────────────────────────────────────────────────────────────────────

// ── State ────────────────────────────────────────────────────────────────────
const hdb = {
  town:      null,   // e.g. "TAMPINES"
  street:    null,   // e.g. "BEDOK NORTH RD"
  label:     null,   // display label
  flatType:  '',
  months:    60,
  chart:     null,
  debounce:  null,
};

// ── Month window map ─────────────────────────────────────────────────────────
const HDB_RANGES = { '6m': 6, '1y': 12, '2y': 24, '3y': 36, '5y': 60, 'all': 420 };

// ── Called when user opens Trend view ────────────────────────────────────────
function initHDBTrend() {
  // Load default if nothing selected yet
  if (!hdb.town && !hdb.street) {
    hdb.town  = 'TAMPINES';
    hdb.label = 'Tampines';
    const input = document.getElementById('hdb-search-input');
    if (input) input.value = 'Tampines';
  }
  fetchHDBTrend();
}

// ── Search autocomplete ───────────────────────────────────────────────────────
function onHDBSearchInput(val) {
  clearTimeout(hdb.debounce);
  const dropdown = document.getElementById('hdb-autocomplete');
  if (!val || val.length < 2) {
    if (dropdown) dropdown.classList.remove('visible');
    return;
  }
  hdb.debounce = setTimeout(async () => {
    try {
      const res  = await fetch(`/api/hdb/search?q=${encodeURIComponent(val)}`);
      const data = await res.json();
      renderHDBAutocomplete(data);
    } catch { /* ignore */ }
  }, 280);
}

function renderHDBAutocomplete(results) {
  const dropdown = document.getElementById('hdb-autocomplete');
  if (!dropdown) return;
  if (!results.length) { dropdown.classList.remove('visible'); return; }

  dropdown.innerHTML = results.map((r, i) => `
    <div class="autocomplete-item" data-idx="${i}">
      <div class="autocomplete-item-icon">${r.type === 'town' ? '🏘️' : '🏠'}</div>
      <div>
        <p class="autocomplete-item-name">${r.label}</p>
        <p class="autocomplete-item-sub">${r.type === 'town' ? 'Town' : 'Street'}</p>
      </div>
    </div>`).join('');

  dropdown.querySelectorAll('.autocomplete-item').forEach((el, i) => {
    el.addEventListener('mousedown', (e) => {
      e.preventDefault();
      const r = results[i];
      const input = document.getElementById('hdb-search-input');
      if (input) input.value = r.label;
      dropdown.classList.remove('visible');
      hdb.flatType = '';
      if (r.type === 'town') {
        hdb.town   = r.value;
        hdb.street = null;
        hdb.label  = r.label;
      } else {
        hdb.street = r.value;
        hdb.town   = r.town || null;
        hdb.label  = r.label;
      }
      fetchHDBTrend();
    });
  });
  dropdown.classList.add('visible');
}

document.addEventListener('click', (e) => {
  if (!e.target.closest('#hdb-search-wrap')) {
    const d = document.getElementById('hdb-autocomplete');
    if (d) d.classList.remove('visible');
  }
});

// ── Range buttons ─────────────────────────────────────────────────────────────
function setHDBRange(range, btn) {
  hdb.months = HDB_RANGES[range] || 60;
  document.querySelectorAll('.hdb-range-btn').forEach(b => {
    b.classList.remove('bg-white', 'shadow-sm', 'text-slate-900');
    b.classList.add('text-slate-500');
  });
  btn.classList.add('bg-white', 'shadow-sm', 'text-slate-900');
  btn.classList.remove('text-slate-500');
  fetchHDBTrend();
}

// ── Flat type filter ──────────────────────────────────────────────────────────
function setHDBFlatType(ft) {
  hdb.flatType = ft === hdb.flatType ? '' : ft;
  document.querySelectorAll('.hdb-ft-btn').forEach(b => {
    const active = b.dataset.ft === hdb.flatType;
    b.classList.toggle('bg-blue-600',  active);
    b.classList.toggle('text-white',   active);
    b.classList.toggle('bg-slate-100', !active);
    b.classList.toggle('text-slate-600', !active);
  });
  fetchHDBTrend();
}

// ── Main fetch + render ───────────────────────────────────────────────────────
async function fetchHDBTrend() {
  if (!hdb.town && !hdb.street) return;

  // Show loading state
  setHDBLoading(true);

  const params = new URLSearchParams({ months: hdb.months });
  if (hdb.town)     params.set('town',      hdb.town);
  if (hdb.street)   params.set('street',    hdb.street);
  if (hdb.flatType) params.set('flat_type', hdb.flatType);

  try {
    const res  = await fetch(`/api/hdb/trend?${params}`);
    if (!res.ok) throw new Error('API error');
    const data = await res.json();

    renderHDBChart(data);
    renderHDBSummary(data);
    renderHDBFlatTypeButtons(data.flat_types || []);
    renderHDBComparables(data.comparables || []);
    updateHDBSubtitle(data);
  } catch (err) {
    console.error('HDB trend error:', err);
    setHDBError('Could not load HDB data. Make sure hdb.db is in the backend folder.');
  } finally {
    setHDBLoading(false);
  }
}

// ── Chart ─────────────────────────────────────────────────────────────────────
function renderHDBChart(data) {
  const canvas = document.getElementById('hdbTrendChart');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  if (hdb.chart) hdb.chart.destroy();

  const isDark   = document.documentElement.classList.contains('dark');
  const gradient = ctx.createLinearGradient(0, 0, 0, 380);
  gradient.addColorStop(0, 'rgba(99,179,237,0.35)');
  gradient.addColorStop(1, 'rgba(99,179,237,0.0)');

  const labels  = data.trend.map(d => d.month.slice(0, 7));
  const avgs    = data.trend.map(d => d.avg_price);
  const medians = data.trend.map(d => d.median_price);
  const vols    = data.trend.map(d => d.transactions);

  const tickColor = isDark ? '#93C5FD' : '#64748B';
  const gridColor = isDark ? 'rgba(147,197,253,0.08)' : 'rgba(0,0,0,0.04)';

  hdb.chart = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [
        {
          label: 'Avg Price (S$)',
          data: avgs,
          borderColor: '#3B82F6',
          borderWidth: 3,
          fill: true,
          backgroundColor: gradient,
          tension: 0.4,
          pointRadius: labels.length > 60 ? 0 : 3,
          pointBackgroundColor: '#3B82F6',
          pointBorderColor: isDark ? '#1E3A5F' : '#fff',
          pointBorderWidth: 2,
          pointHoverRadius: 7,
          yAxisID: 'y',
        },
        {
          label: 'Median Price (S$)',
          data: medians,
          borderColor: '#8B5CF6',
          borderWidth: 2,
          borderDash: [5, 4],
          fill: false,
          tension: 0.4,
          pointRadius: 0,
          pointHoverRadius: 5,
          yAxisID: 'y',
        },
        {
          label: 'Transactions',
          data: vols,
          type: 'bar',
          backgroundColor: 'rgba(148,163,184,0.18)',
          yAxisID: 'y2',
          order: 10,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { intersect: false, mode: 'index' },
      plugins: {
        legend: {
          display: true,
          position: 'top',
          labels: { font: { size: 11, weight: '600' }, color: tickColor, boxWidth: 14, padding: 16 },
        },
        tooltip: {
          backgroundColor: isDark ? '#1E3A5F' : '#1E293B',
          titleColor: '#93C5FD',
          bodyColor: '#E2E8F0',
          padding: 14,
          cornerRadius: 10,
          callbacks: {
            label: ctx => {
              if (ctx.dataset.label === 'Transactions')
                return `  Transactions: ${ctx.parsed.y.toLocaleString()}`;
              return `  ${ctx.dataset.label}: S$${ctx.parsed.y.toLocaleString()}`;
            },
          },
        },
      },
      scales: {
        y: {
          position: 'left',
          beginAtZero: false,
          grid: { color: gridColor, drawBorder: false },
          ticks: {
            font: { size: 11, weight: '600' }, color: tickColor,
            callback: v => v >= 1e6 ? `${(v/1e6).toFixed(1)}M` : `${(v/1e3).toFixed(0)}K`,
          },
        },
        y2: {
          position: 'right',
          grid: { display: false },
          ticks: { font: { size: 10 }, color: tickColor,
                   callback: v => v >= 1000 ? `${(v/1000).toFixed(1)}k` : v },
        },
        x: {
          grid: { display: false },
          ticks: {
            font: { size: 11, weight: '600' }, color: tickColor,
            maxTicksLimit: 14,
          },
        },
      },
    },
  });
}

// ── Summary badges ────────────────────────────────────────────────────────────
function renderHDBSummary(data) {
  const s = data.summary;
  const trend = data.trend;
  const el = document.getElementById('hdb-summary-row');
  if (!el) return;

  // Price change over period
  let changePct = null, changeHtml = '';
  if (trend.length >= 2) {
    const first = trend[0].avg_price, last = trend[trend.length-1].avg_price;
    changePct = ((last - first) / first * 100).toFixed(1);
    const up = changePct >= 0;
    changeHtml = `<span class="font-bold ${up ? 'text-emerald-600' : 'text-rose-500'}">${up ? '↑' : '↓'} ${Math.abs(changePct)}%</span>`;
  }

  el.innerHTML = `
    <div class="bg-white dark:bg-slate-800/80 rounded-2xl border border-slate-200 dark:border-slate-700 p-5 text-center shadow-sm">
      <p class="text-xs font-bold text-slate-400 uppercase mb-1">Avg Price</p>
      <p class="text-xl font-bold">S$${(s.avg_price||0).toLocaleString()}</p>
      ${changePct !== null ? `<p class="text-xs mt-1">${changeHtml} over period</p>` : ''}
    </div>
    <div class="bg-white dark:bg-slate-800/80 rounded-2xl border border-slate-200 dark:border-slate-700 p-5 text-center shadow-sm">
      <p class="text-xs font-bold text-slate-400 uppercase mb-1">Lowest</p>
      <p class="text-xl font-bold">S$${(s.min_price||0).toLocaleString()}</p>
    </div>
    <div class="bg-white dark:bg-slate-800/80 rounded-2xl border border-slate-200 dark:border-slate-700 p-5 text-center shadow-sm">
      <p class="text-xs font-bold text-slate-400 uppercase mb-1">Highest</p>
      <p class="text-xl font-bold">S$${(s.max_price||0).toLocaleString()}</p>
    </div>
    <div class="bg-white dark:bg-slate-800/80 rounded-2xl border border-slate-200 dark:border-slate-700 p-5 text-center shadow-sm">
      <p class="text-xs font-bold text-slate-400 uppercase mb-1">Transactions</p>
      <p class="text-xl font-bold">${(s.total||0).toLocaleString()}</p>
    </div>`;
}

// ── Flat type filter buttons ───────────────────────────────────────────────────
function renderHDBFlatTypeButtons(flatTypes) {
  const wrap = document.getElementById('hdb-flat-type-wrap');
  if (!wrap) return;
  if (!flatTypes.length) { wrap.innerHTML = ''; return; }

  wrap.innerHTML = ['', ...flatTypes].map(ft => {
    const label   = ft || 'All Types';
    const active  = hdb.flatType === ft;
    const cls     = active
      ? 'bg-blue-600 text-white'
      : 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300 hover:bg-slate-200 dark:hover:bg-slate-600';
    return `<button class="hdb-ft-btn px-3 py-1.5 rounded-xl text-xs font-bold transition-colors ${cls}"
              data-ft="${ft}" onclick="setHDBFlatType('${ft}')">${label}</button>`;
  }).join('');
}

// ── Comparable sales table ────────────────────────────────────────────────────
function renderHDBComparables(rows) {
  const tbody = document.getElementById('hdb-comparable-body');
  if (!tbody) return;
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="5" class="py-6 text-center text-sm text-slate-400">No recent transactions found</td></tr>';
    return;
  }
  tbody.innerHTML = rows.map(r => `
    <tr class="hover:bg-slate-50/50 dark:hover:bg-slate-800/50 transition-colors">
      <td class="py-4 pl-2">
        <p class="font-bold text-sm text-slate-900 dark:text-white">${r.address}</p>
        <p class="text-xs text-slate-400">${r.storey_range} · ${r.floor_area} sqm</p>
      </td>
      <td class="py-4 text-sm text-slate-600 dark:text-slate-300">${r.flat_type}</td>
      <td class="py-4 font-bold text-slate-900 dark:text-white">S$${r.price.toLocaleString()}</td>
      <td class="py-4 text-sm text-slate-500">${(r.price / r.floor_area / 10.764).toFixed(0)} psf</td>
      <td class="py-4 pr-2 text-right">
        <span class="px-2 py-1 bg-slate-100 dark:bg-slate-700 rounded-lg text-[10px] font-bold">${r.month}</span>
      </td>
    </tr>`).join('');
}

function updateHDBSubtitle(data) {
  const el = document.getElementById('hdb-chart-subtitle');
  if (!el) return;
  const scope = data.meta.street
    ? data.meta.street.toLowerCase().replace(/\b\w/g, c => c.toUpperCase())
    : (data.meta.town||'').toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
  const ft = data.meta.flat_type ? ` · ${data.meta.flat_type}` : '';
  el.innerText = `Resale prices — ${scope}${ft}`;
}

function setHDBLoading(on) {
  const spinner = document.getElementById('hdb-chart-spinner');
  const chart   = document.getElementById('hdb-chart-wrap');
  if (spinner) spinner.classList.toggle('hidden', !on);
  if (chart)   chart.classList.toggle('opacity-30', on);
}

function setHDBError(msg) {
  const el = document.getElementById('hdb-chart-wrap');
  if (el) el.innerHTML = `<p class="text-center text-sm text-slate-400 py-20">${msg}</p>`;
}

// ─────────────────────────────────────────────────────────────────────────────
// END HDB TREND BLOCK
// ─────────────────────────────────────────────────────────────────────────────


