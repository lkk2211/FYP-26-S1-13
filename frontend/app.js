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
        setTimeout(() => { loadMarketWatch(); initTrendChart(); renderTrendNews(currentNeighbourhood); runABSDSimulation(); }, 100);
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

// Available floor areas for the snapping slider (populated per postal + bedrooms)
let _availableAreas = [];

function updateSlider(id) {
    const range   = document.getElementById('range-' + id);
    const display = document.getElementById('val-' + id);
    if (!range) return;
    if (id === 'area' && _availableAreas.length > 0) {
        // Index-based: slider value is index into _availableAreas
        const idx    = Math.min(parseInt(range.value), _availableAreas.length - 1);
        const actual = _availableAreas[idx];
        range.dataset.actualValue = actual;
        if (display) display.innerText = actual.toLocaleString();
    } else {
        if (display) display.innerText = id === 'area' ? parseInt(range.value).toLocaleString() : range.value;
    }
    if (id === 'bedrooms') _loadFlatSpecs();
}

function getAreaValue() {
    const range = document.getElementById('range-area');
    if (!range) return 1000;
    if (_availableAreas.length > 0) {
        const idx = Math.min(parseInt(range.value), _availableAreas.length - 1);
        return _availableAreas[idx];
    }
    return parseInt(range.value);
}

function _onPropertyTypeChange() {
    _loadFlatSpecs();
}

// Flat type mapping (bedrooms → HDB flat type)
const _BEDS_TO_FLAT_TYPE = {
    1: '1 ROOM', 2: '2 ROOM', 3: '3 ROOM', 4: '4 ROOM', 5: '5 ROOM', 6: 'EXECUTIVE',
};

async function _loadFlatSpecs() {
    const bedsEl = document.getElementById('range-bedrooms');
    const propEl = document.getElementById('input-property-type');
    if (!bedsEl) return;

    const beds     = parseInt(bedsEl.value);
    const propType = propEl ? propEl.value : 'HDB';
    const postal   = document.getElementById('input-postal')?.value?.trim() || '';

    try {
        const url = `/api/property-areas?bedrooms=${beds}&property_type=${encodeURIComponent(propType)}` +
                    (postal ? `&postal=${encodeURIComponent(postal)}` : '');
        const res  = await fetch(url);
        const data = await res.json();

        const areas      = data.floor_areas || [];
        const maxFloor   = data.max_floor   || 50;

        // ── Area slider (index-based snap) ────────────────────────
        const areaRange = document.getElementById('range-area');
        const areaHint  = document.getElementById('area-range-hint');
        if (areaRange && areas.length > 0) {
            _availableAreas = areas;

            // Find index of current value's nearest match
            const curActual = parseInt(areaRange.dataset.actualValue || areas[Math.floor(areas.length / 2)]);
            const nearestIdx = areas.reduce((best, v, i) =>
                Math.abs(v - curActual) < Math.abs(areas[best] - curActual) ? i : best, 0);

            areaRange.min   = 0;
            areaRange.max   = areas.length - 1;
            areaRange.step  = 1;
            areaRange.value = nearestIdx;
            updateSlider('area');   // sets dataset.actualValue + display

            if (areaHint) {
                const min = areas[0], max = areas[areas.length - 1];
                areaHint.textContent = `${areas.length} size option${areas.length > 1 ? 's' : ''}: `
                    + `${min.toLocaleString()}–${max.toLocaleString()} sq ft`;
            }
        }

        // ── Floor slider ──────────────────────────────────────────
        const floorRange   = document.getElementById('range-floor');
        const floorHint    = document.getElementById('floor-max-hint');
        const floorDisplay = document.getElementById('floor-max-display');
        if (floorRange && maxFloor) {
            floorRange.max = maxFloor;
            if (parseInt(floorRange.value) > maxFloor) {
                floorRange.value = Math.max(1, Math.floor(maxFloor / 2));
                updateSlider('floor');
            }
            if (floorDisplay) floorDisplay.textContent = maxFloor;
            if (floorHint) floorHint.textContent =
                `Max: ${maxFloor} floors` +
                (_predictTown ? ` in ${_predictTown.charAt(0) + _predictTown.slice(1).toLowerCase()}` : '') +
                ' (from transaction data)';
        }
    } catch { /* silent */ }
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

        // Reject non-residential properties
        const NON_RESIDENTIAL_KEYWORDS = [
            'MRT STATION', ' MRT', 'LRT STATION', ' LRT', 'SHOPPING MALL', 'SHOPPING CENTRE',
            'SHOPPING CENTER', 'RETAIL MALL', 'OFFICE', 'INDUSTRIAL', 'COMMERCIAL',
            'HOSPITAL', 'POLYCLINIC', 'CLINIC', 'HOTEL', 'SERVICED APARTMENT',
            'AIRPORT', 'TERMINAL', 'BUS INTERCHANGE', 'INTERCHANGE', 'COMMUNITY CLUB',
            'COMMUNITY CENTRE', 'COMMUNITY CENTER', 'SCHOOL', 'UNIVERSITY', 'POLYTECHNIC',
            'INSTITUTE', 'LIBRARY', 'MOSQUE', 'TEMPLE', 'CHURCH', 'SYNAGOGUE',
            'MILITARY', 'CAMP', 'BARRACKS', 'PRISON', 'REMAND', 'STADIUM',
        ];
        const buildingUpper = building.toUpperCase();
        const isNonResidential = NON_RESIDENTIAL_KEYWORDS.some(kw => buildingUpper.includes(kw));
        if (isNonResidential) {
            errorEl.textContent = 'Please enter a valid residential property address. Shopping malls, MRT stations, offices, and institutions are not supported.';
            errorEl.classList.remove('hidden');
            return;
        }

        document.getElementById('display-address').innerText = address;
        document.getElementById('display-building').innerText = building;
        document.getElementById('input-postal').value = postal;

        const placeholder = document.getElementById('postal-placeholder');
        const details     = document.getElementById('postal-details');
        const landedBanner = document.getElementById('landed-rejection');
        placeholder.classList.add('hidden');
        details.classList.add('hidden');          // will be revealed by property-lookup
        if (landedBanner) landedBanner.classList.add('hidden');

        // Auto-fill property type + lease type from backend; detect landed
        fetch(`/api/property-lookup?postal=${encodeURIComponent(postal)}`)
            .then(r => r.json())
            .then(info => {
                const landedEl  = document.getElementById('landed-rejection');
                const detailsEl = document.getElementById('postal-details');

                if (info.is_landed) {
                    // Show landed rejection, hide prediction form
                    if (landedEl)  landedEl.classList.remove('hidden');
                    if (detailsEl) detailsEl.classList.add('hidden');
                    lucide.createIcons();
                    return;
                }

                // Valid HDB or Condo — show details
                if (landedEl)  landedEl.classList.add('hidden');
                if (detailsEl) detailsEl.classList.remove('hidden');

                if (info.property_type) {
                    const ptEl = document.getElementById('input-property-type');
                    if (ptEl) ptEl.value = info.property_type;
                }
                if (info.lease_type) {
                    const ltEl = document.getElementById('input-lease-type');
                    if (ltEl) ltEl.value = info.lease_type;
                }
                _predictTown = info.town || '';
                _loadFlatSpecs();
            })
            .catch(() => {});
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
    
    const postal   = document.getElementById('input-postal').value;
    lastMapPostal  = postal;
    const area     = getAreaValue();
    const bedrooms = document.getElementById('range-bedrooms').value;
    const floor    = document.getElementById('range-floor').value;
    const propType = document.getElementById('input-property-type')?.value || 'HDB';

    try {
        const response = await fetch('/api/predict', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                postal, area, bedrooms, floor,
                property_type: propType,
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
        const recEl = document.getElementById('output-recommendation');
        if (insightEl || recEl) {
            const rawInsight = data.insight || '';
            const rawRec    = data.recommendation || '';
            const isSell = _userIntent === 'sell';
            const intentInsight = isSell
                ? `As a seller: ${rawInsight}`
                : `As a buyer: ${rawInsight}`;
            const intentRec = isSell
                ? rawRec.replace(/\bOffer\b/gi, 'List at').replace(/\bbudget\b/gi, 'asking price').replace(/\b(consider|look for)\b/gi, 'highlight')
                    .replace(/^/, 'Seller tip: ')
                : `Buyer tip: ${rawRec}`;
            if (insightEl) insightEl.innerText = intentInsight;
            if (recEl)     recEl.innerText     = intentRec;
        }

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
    } else if (tabId === 'data') {
        loadDataTabStats();
    }

    lucide.createIcons();
}

// ── Global state ────────────────────────────────────────────
let lastMapPostal = '';
let currentNeighbourhood = 'Clementi';
let _predictTown = '';

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

        // Fetch property type to know if prediction is available
        let propInfo = null;
        if (validPostal) {
            try {
                const pr = await fetch(`/api/property-lookup?postal=${validPostal}`);
                propInfo = await pr.json();
            } catch { /* silent */ }
        }
        showPinResultBar(validPostal, shortAddr, lat, lng, propInfo);
    } catch {
        addDraggablePin(lat, lng);
        showPinResultBar(null, `${lat.toFixed(5)}, ${lng.toFixed(5)}`, lat, lng, null);
    }
}

function showPinResultBar(postal, address, lat, lng, propInfo) {
    if (!_draggablePin) return;

    const isLanded    = propInfo?.is_landed === true;
    const canPredict  = postal && !isLanded && (propInfo?.property_type === 'HDB' || propInfo?.property_type === 'Condominium');
    const propType    = propInfo?.property_type || '';

    // Prediction availability badge
    const predBadge = canPredict
        ? `<div style="background:linear-gradient(135deg,#dcfce7,#bbf7d0);border:1px solid #86efac;border-radius:10px;padding:5px 10px;margin-bottom:8px;display:flex;align-items:center;gap:5px">
            <span style="width:7px;height:7px;border-radius:50%;background:#16a34a;flex-shrink:0"></span>
            <span style="font-size:10px;font-weight:700;color:#15803d">Price prediction available for this ${propType}!</span>
           </div>`
        : (isLanded
            ? `<div style="background:#fefce8;border:1px solid #fde047;border-radius:10px;padding:5px 10px;margin-bottom:8px">
                <span style="font-size:10px;font-weight:700;color:#a16207">Landed property — prediction unavailable</span>
               </div>`
            : '');

    const predictBtn = canPredict
        ? `<button onclick="usePostalFromMap('${postal}')" style="background:linear-gradient(135deg,#2563eb,#7c3aed);color:white;border:none;padding:6px 14px;border-radius:10px;font-size:12px;font-weight:700;cursor:pointer;margin-right:6px">Predict</button>`
        : '';

    const popupHtml = `
        <div style="min-width:210px;font-family:inherit">
            ${predBadge}
            <p style="font-weight:700;font-size:13px;color:#0f172a;margin:0 0 3px">${postal ? '📮 ' + postal : '📍 Location'}</p>
            <p style="font-size:11px;color:#64748b;margin:0 0 10px;max-width:210px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${address}">${address}</p>
            <div style="display:flex;gap:6px">
                ${predictBtn}
                <button onclick="(function(){document.querySelector('.leaflet-popup-close-button')&&document.querySelector('.leaflet-popup-close-button').click();loadAmenities(${lat},${lng},'${postal||''}');})()" style="background:none;border:1.5px solid #f97316;color:#f97316;padding:6px 14px;border-radius:10px;font-size:12px;font-weight:700;cursor:pointer">Explore</button>
            </div>
        </div>`;
    _draggablePin.bindPopup(popupHtml, { offset: [0, -30], closeButton: true, maxWidth: 270 }).openPopup();
    const bar = document.getElementById('pin-result-bar');
    if (bar) bar.classList.add('hidden');
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
    setTimeout(() => initTrendChart(currentRange), 50);
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
    const months = range === '6m' ? 6 : range === '1y' ? 12 : range === '3y' ? 36 : 60;
    // step controls how many months between each data point / label
    const step = range === '5y' ? 6 : range === '3y' ? 3 : 1;
    const labels = [], prices = [];
    const now = new Date();
    let p = cfg.base * Math.pow(1 - cfg.growth / 12, months);
    for (let i = months; i >= 0; i--) {
        p = p * (1 + cfg.growth / 12 + (Math.sin(i * 0.7) * 0.002));
        if (i % step !== 0) continue;
        const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
        // Use "Q1 '23" style for 3Y/5Y, "Jan '24" style for 6M/1Y
        let label;
        if (step >= 3) {
            const q = Math.floor(d.getMonth() / 3) + 1;
            label = `Q${q} '${String(d.getFullYear()).slice(2)}`;
        } else {
            label = d.toLocaleString('en-SG', { month: 'short', year: '2-digit' });
        }
        labels.push(label);
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
        if (range === '6m') {
            const res = await fetch(`/api/trend`);
            if (!res.ok) throw new Error();
            const data = await res.json();
            if (data.trend_data && data.trend_data.length) {
                const cfg = NEIGHBOURHOOD_BASE[currentNeighbourhood] || NEIGHBOURHOOD_BASE['Clementi'];
                const apiAvg = data.trend_data.reduce((s, d) => s + d.price, 0) / data.trend_data.length;
                const scale = cfg.base / apiAvg;
                labels = data.trend_data.map(d => d.month);
                prices = data.trend_data.map(d => Math.round(d.price * scale));
                loadComparableTable(data);
                loadNearestSale(data);
            } else throw new Error();
        } else {
            throw new Error('use generated');
        }
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
                    ticks: {
                        font: { size: 11, weight: '600' },
                        color: tickColor,
                        maxRotation: 45,
                        minRotation: 0,
                        autoSkip: true,
                        maxTicksLimit: range === '5y' ? 10 : range === '3y' ? 12 : range === '1y' ? 12 : 6
                    }
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
    initTrendChart();
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

    initTrendChart(range);
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

// ── Admin: Export Report ──────────────────────────────────────
async function exportAdminReport(btn) {
    const orig = btn.innerHTML;
    btn.disabled = true;
    btn.innerHTML = '<i data-lucide="loader-2" class="w-4 h-4 animate-spin"></i> Generating...';
    lucide.createIcons();
    try {
        const res = await fetch('/api/admin/export-report');
        if (!res.ok) throw new Error(`Server error ${res.status}`);
        const blob = await res.blob();
        const url  = URL.createObjectURL(blob);
        const a    = document.createElement('a');
        a.href     = url;
        a.download = `propai_report_${new Date().toISOString().slice(0,10)}.pdf`;
        a.click();
        URL.revokeObjectURL(url);
        showToast('Report downloaded');
    } catch (e) {
        showToast('Failed to generate report: ' + e.message);
    } finally {
        btn.disabled = false;
        btn.innerHTML = orig;
        lucide.createIcons();
    }
}

// ── Buying / Selling Intent ───────────────────────────────────
let _userIntent = 'buy';

function setIntent(intent) {
    _userIntent = intent;
    const buyBtn  = document.getElementById('intent-buy');
    const sellBtn = document.getElementById('intent-sell');
    const active  = 'flex-1 py-2.5 rounded-xl text-sm font-bold border-2 border-blue-600 bg-blue-600 text-white transition-all';
    const inactive = 'flex-1 py-2.5 rounded-xl text-sm font-bold border-2 border-slate-200 text-slate-500 hover:border-slate-300 transition-all';
    if (buyBtn)  buyBtn.className  = intent === 'buy'  ? active : inactive;
    if (sellBtn) sellBtn.className = intent === 'sell' ? active : inactive;
}


// ── Footer Modal ──────────────────────────────────────────────
const _FOOTER_CONTENT = {
    about: {
        title: 'About PropAI.sg',
        body: `<p>PropAI.sg is an AI-powered property valuation platform built for Singapore. We combine real HDB and private property transaction data with government policy signals and interest rate trends to deliver data-driven price estimates.</p><p>Our ensemble machine learning model (XGBoost + LightGBM + CatBoost) is trained on historical Singapore resale transactions from 2017 onwards.</p>`,
    },
    team: {
        title: 'Who We Are',
        body: `<p>PropAI.sg was developed as a Final Year Project by a team of Singapore Polytechnic students passionate about making property market intelligence accessible to everyone.</p><p>We believe that transparent, data-driven valuations help buyers and sellers make more informed decisions.</p>`,
    },
    feedback: {
        title: 'Feedback',
        body: `<p>We value your feedback! If you've found an issue, have a feature suggestion, or simply want to share your experience, please reach out.</p><p class="font-medium text-slate-900">📧 Contact your administrator or leave a note in the platform's admin panel.</p>`,
    },
    terms: {
        title: 'Terms & Conditions',
        body: `<p><strong>Use of Service:</strong> PropAI.sg provides property valuation estimates for informational purposes only. By using this platform you agree not to rely solely on these estimates for financial or legal decisions.</p><p><strong>Accuracy:</strong> Estimates are generated by machine learning models trained on historical data. Actual market prices may vary significantly.</p><p><strong>No Warranty:</strong> This service is provided "as is" without warranty of any kind.</p>`,
    },
    privacy: {
        title: 'Privacy Policy',
        body: `<p><strong>Data Collection:</strong> We collect your name, email, and search activity to personalise your experience and improve our models.</p><p><strong>Data Use:</strong> Your data is never sold to third parties. Prediction history is stored to enable your valuation history and aggregate analytics.</p><p><strong>Cookies:</strong> We use local storage to maintain your session and theme preference.</p>`,
    },
    disclaimer: {
        title: 'Disclaimer',
        body: `<p>Property valuations shown on PropAI.sg are AI-generated estimates and do not constitute financial, legal, or real estate advice.</p><p>Always consult a licensed property agent or financial advisor before making any property investment decisions. Past transaction prices are not a guarantee of future values.</p>`,
    },
};

function showFooterModal(key) {
    const modal = document.getElementById('footer-modal');
    const title = document.getElementById('footer-modal-title');
    const body  = document.getElementById('footer-modal-body');
    const c     = _FOOTER_CONTENT[key];
    if (!modal || !c) return;
    title.textContent = c.title;
    body.innerHTML    = c.body;
    modal.classList.remove('hidden');
    modal.classList.add('flex');
    lucide.createIcons();
}

function closeFooterModal() {
    const modal = document.getElementById('footer-modal');
    if (modal) { modal.classList.add('hidden'); modal.classList.remove('flex'); }
}


// ── Admin: CSV Upload ─────────────────────────────────────────
let _uploadType = 'hdb';

const _UPLOAD_HINTS = {
    hdb:      { label: 'Expected HDB Resale CSV columns:', cols: 'month, town, flat_type, block, street_name, storey_range, floor_area_sqm, flat_model, lease_commence_date, remaining_lease, resale_price' },
    ura:      { label: 'Expected URA Transactions CSV columns (download from URA website):', cols: 'Project Name, Street Name, Property Type, Market Segment, Postal District, Floor Level, Area (SQFT), Area (SQM), Type of Sale, Transacted Price ($), Unit Price ($ PSF), Unit Price ($ PSM), Tenure, Number of Units, Sale Date' },
    geocoded: { label: 'Expected Geocoded Addresses CSV columns:', cols: 'search_text, lat, lon' },
    policy:   { label: 'Expected Policy Changes XLSX columns:', cols: 'effective_month, effective_date, policy_name, category, direction, severity, source' },
    sora:     { label: 'Expected SORA Rates XLSX columns:', cols: 'SORA Publication Date, Compound SORA - 3 month' },
};

function setUploadType(type) {
    _uploadType = type;
    const activeClass   = 'px-5 py-2.5 bg-slate-900 text-white rounded-xl text-sm font-bold transition-all';
    const inactiveClass = 'px-5 py-2.5 bg-slate-100 text-slate-600 rounded-xl text-sm font-bold transition-all hover:bg-slate-200';
    ['hdb','ura','geocoded','policy','sora'].forEach(t => {
        const btn = document.getElementById(`upload-type-${t}`);
        if (btn) btn.className = t === type ? activeClass : inactiveClass;
    });
    const hint = document.getElementById('upload-format-hint');
    if (hint && _UPLOAD_HINTS[type]) {
        const h = _UPLOAD_HINTS[type];
        hint.innerHTML = `<p class="font-bold text-slate-700 mb-1 font-sans">${h.label}</p>${h.cols}`;
    }
}

function onCsvFileSelected(event) {
    const file  = event.target.files[0];
    const label = document.getElementById('upload-file-label');
    if (file && label) label.textContent = file.name;
}

async function handleCsvUpload() {
    const input  = document.getElementById('tx-csv-input');
    const status = document.getElementById('upload-status');
    const btn    = document.getElementById('upload-submit-btn');
    if (!input || !input.files.length) { showToast('Please select a CSV file first'); return; }

    const formData = new FormData();
    formData.append('file', input.files[0]);
    formData.append('type', _uploadType);

    btn.disabled = true;
    btn.innerHTML = '<i data-lucide="loader-2" class="w-4 h-4 animate-spin"></i> Uploading...';
    lucide.createIcons();
    if (status) { status.className = 'text-sm text-slate-500'; status.textContent = 'Uploading file…'; status.classList.remove('hidden'); }

    try {
        const res  = await fetch('/api/admin/upload-transactions', { method: 'POST', body: formData });
        const data = await res.json();
        if (data.error) throw new Error(data.error);

        // Async job — poll for completion
        if (data.job_id) {
            input.value = '';
            const label = document.getElementById('upload-file-label');
            if (label) label.textContent = 'Click to select a CSV file';
            btn.disabled = false;
            btn.innerHTML = '<i data-lucide="upload" class="w-4 h-4"></i> Upload to Database';
            lucide.createIcons();
            _pollUploadStatus(data.job_id, status);
            return;
        }

        // Sync response (URA / SQLite)
        const msg = `Uploaded ${(data.inserted||0).toLocaleString()} of ${(data.total_rows||0).toLocaleString()} rows successfully.`;
        if (status) { status.className = 'text-sm text-emerald-600 font-medium'; status.textContent = msg; }
        showToast(msg);
        input.value = '';
        const label2 = document.getElementById('upload-file-label');
        if (label2) label2.textContent = 'Click to select a CSV file';
        loadDataTabStats();
    } catch (e) {
        if (status) { status.className = 'text-sm text-red-500 font-medium'; status.textContent = `Error: ${e.message}`; }
        showToast('Upload failed: ' + e.message);
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i data-lucide="upload" class="w-4 h-4"></i> Upload to Database';
        lucide.createIcons();
    }
}

async function _pollUploadStatus(jobId, statusEl) {
    const start = Date.now();
    while (true) {
        await new Promise(r => setTimeout(r, 2500));
        try {
            const res  = await fetch(`/api/admin/upload-status?job_id=${jobId}`);
            const data = await res.json();
            const elapsed = Math.round((Date.now() - start) / 1000);

            if (data.state === 'processing') {
                if (statusEl) statusEl.textContent = `Processing… ${(data.inserted||0).toLocaleString()} rows staged (${elapsed}s)`;
            } else if (data.state === 'done') {
                const msg = `Upload complete — ${(data.inserted||0).toLocaleString()} rows processed.`;
                if (statusEl) { statusEl.className = 'text-sm text-emerald-600 font-medium'; statusEl.textContent = msg; }
                showToast(msg);
                loadDataTabStats();
                return;
            } else if (data.state === 'error') {
                if (statusEl) { statusEl.className = 'text-sm text-red-500 font-medium'; statusEl.textContent = `Error: ${data.message}`; }
                showToast('Upload failed: ' + data.message);
                return;
            }
        } catch (_) { /* keep polling on transient network errors */ }
    }
}

async function loadDataTabStats() {
    try {
        const res  = await fetch('/api/stats');
        const data = await res.json();
        const ids = {
            'data-hdb-count':      data.hdb_tx_count,
            'data-ura-count':      data.priv_tx_count,
            'data-geocoded-count': data.geocoded_count,
            'data-policy-count':   data.policy_count,
            'data-sora-count':     data.sora_count,
        };
        for (const [id, val] of Object.entries(ids)) {
            const el = document.getElementById(id);
            if (el) el.textContent = (val || 0).toLocaleString();
        }
    } catch (e) { /* silent */ }

    // Refresh retrain status dots
    try {
        const res  = await fetch('/api/admin/retrain-status');
        const data = await res.json();
        const anyRunning = _updateRetrainStatusUI(data);
        if (anyRunning && !_retrainPollInterval) {
            _retrainPollInterval = setInterval(_pollRetrainStatus, 3000);
        }
    } catch (e) { /* silent */ }
}

// ── Admin: URA Sync ───────────────────────────────────────────
async function handleUraSync() {
    const btn    = document.getElementById('ura-sync-btn');
    const status = document.getElementById('ura-sync-status');
    if (!btn || !status) return;
    btn.disabled = true;
    btn.innerHTML = '<i data-lucide="loader-2" class="w-4 h-4 animate-spin"></i> Syncing…';
    lucide.createIcons();
    status.className = 'mt-4 text-sm rounded-xl p-4 bg-slate-50 text-slate-600';
    status.textContent = 'Contacting URA API — this may take up to a minute…';
    status.classList.remove('hidden');
    try {
        const res  = await fetch('/api/admin/sync-ura', { method: 'POST' });
        const data = await res.json();
        if (res.ok) {
            status.className = 'mt-4 text-sm rounded-xl p-4 bg-emerald-50 text-emerald-700 font-medium';
            status.textContent = `Sync complete. ${data.inserted ?? 0} new records inserted, ${data.deleted ?? 0} old records removed.`;
            loadDataTabStats();
        } else {
            status.className = 'mt-4 text-sm rounded-xl p-4 bg-rose-50 text-rose-700 font-medium';
            status.textContent = `Sync failed: ${data.error || 'Unknown error.'}`;
        }
    } catch (e) {
        status.className = 'mt-4 text-sm rounded-xl p-4 bg-rose-50 text-rose-700 font-medium';
        status.textContent = 'Network error — could not reach the server.';
    }
    btn.disabled = false;
    btn.innerHTML = '<i data-lucide="refresh-cw" class="w-4 h-4"></i> Sync URA Data Now';
    lucide.createIcons();
}

// ── Upload Colab-trained model files ──────────────────────────
async function handleModelUpload() {
    const input  = document.getElementById('model-file-input');
    const btn    = document.getElementById('model-upload-btn');
    const status = document.getElementById('model-upload-status');
    const label  = document.getElementById('model-file-label');
    if (!input.files.length) { alert('Please select a .joblib file first.'); return; }
    const file = input.files[0];
    btn.disabled = true;
    status.className = 'mt-4 text-sm rounded-xl p-4 bg-slate-50 text-slate-600';
    status.textContent = `Uploading ${file.name}…`;
    status.classList.remove('hidden');
    const form = new FormData();
    form.append('file', file);
    try {
        const res  = await _fetchWithRetry('/api/admin/upload-model', { method: 'POST', body: form }, { retries: 3, delayMs: 6000 });
        const data = await res.json();
        if (res.ok) {
            status.className = 'mt-4 text-sm rounded-xl p-4 bg-emerald-50 text-emerald-700 font-medium';
            status.textContent = data.message || 'Uploaded successfully.';
            input.value = '';
            label.textContent = 'Choose .joblib file';
        } else {
            status.className = 'mt-4 text-sm rounded-xl p-4 bg-rose-50 text-rose-700 font-medium';
            status.textContent = `Upload failed: ${data.error || 'Unknown error'}`;
        }
    } catch (e) {
        status.className = 'mt-4 text-sm rounded-xl p-4 bg-rose-50 text-rose-700 font-medium';
        status.textContent = 'Network error — could not reach the server.';
    }
    btn.disabled = false;
}

// ── Fetch with retry (handles Render free-tier cold-start) ────
async function _fetchWithRetry(url, opts = {}, { retries = 4, delayMs = 7000 } = {}) {
    for (let attempt = 0; attempt <= retries; attempt++) {
        try {
            return await fetch(url, opts);
        } catch (e) {
            if (attempt === retries) throw e;
            await new Promise(r => setTimeout(r, delayMs));
        }
    }
}

// ── Retrain Models ────────────────────────────────────────────

let _retrainPollInterval = null;

async function handleRetrain(type) {
    ['hdb', 'both', 'private'].forEach(t => {
        const b = document.getElementById(`retrain-${t}-btn`);
        if (b) { b.disabled = true; b.classList.add('opacity-50'); }
    });
    // Show a status banner so the user knows we're waiting on a cold start
    const statusEl = document.getElementById('retrain-status') || null;
    const _setStatus = msg => { if (statusEl) { statusEl.textContent = msg; statusEl.classList.remove('hidden'); } };
    _setStatus('Contacting server…');
    try {
        const res  = await _fetchWithRetry('/api/admin/retrain', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ type }),
        }, { retries: 5, delayMs: 8000 });
        const data = await res.json();
        if (!res.ok) {
            alert(`Could not start training: ${data.error || data.message || 'Unknown error'}`);
            _retrainEnableButtons();
            return;
        }
    } catch (e) {
        alert('Network error — could not reach the server. Please wait a moment and try again (the server may be waking up).');
        _retrainEnableButtons();
        return;
    }
    // Start polling
    if (_retrainPollInterval) clearInterval(_retrainPollInterval);
    _retrainPollInterval = setInterval(_pollRetrainStatus, 3000);
    _pollRetrainStatus();
}

async function _pollRetrainStatus() {
    try {
        const res  = await fetch('/api/admin/retrain-status');
        const data = await res.json();
        const anyRunning = _updateRetrainStatusUI(data);
        if (!anyRunning) {
            clearInterval(_retrainPollInterval);
            _retrainPollInterval = null;
            _retrainEnableButtons();
        }
    } catch (_) {}
}

function _updateRetrainStatusUI(data) {
    let anyRunning = false;
    for (const [key, info] of Object.entries(data)) {
        const dot   = document.getElementById(`retrain-${key}-dot`);
        const state = document.getElementById(`retrain-${key}-state`);
        const msg   = document.getElementById(`retrain-${key}-msg`);
        if (!dot) continue;
        const s = info.state;
        if (s === 'running') {
            dot.className   = 'w-2.5 h-2.5 rounded-full bg-amber-400 animate-pulse';
            state.textContent = 'Training…';
            state.className   = 'text-xs font-bold text-amber-600';
            anyRunning = true;
        } else if (s === 'success') {
            dot.className   = 'w-2.5 h-2.5 rounded-full bg-emerald-500';
            state.textContent = `Done · ${info.finished_at || ''}`;
            state.className   = 'text-xs font-bold text-emerald-600';
        } else if (s === 'error') {
            dot.className   = 'w-2.5 h-2.5 rounded-full bg-rose-500';
            state.textContent = `Error · ${info.finished_at || ''}`;
            state.className   = 'text-xs font-bold text-rose-600';
        } else {
            dot.className   = 'w-2.5 h-2.5 rounded-full bg-slate-300';
            state.textContent = 'Idle';
            state.className   = 'text-xs font-bold text-slate-500';
        }
        if (msg) msg.textContent = info.message || '';
    }
    return anyRunning;
}

function _retrainEnableButtons() {
    ['hdb', 'both', 'private'].forEach(t => {
        const b = document.getElementById(`retrain-${t}-btn`);
        if (b) { b.disabled = false; b.classList.remove('opacity-50'); }
    });
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
startMarketTicker();

// ── Market ticker (floating price badge) ─────────────────────
// Ticker data based on Singapore HDB/Condo market Q1 2026 estimates
// CCR/RCR/OCR condo PSF and HDB resale medians from transaction data trends
const _TICKER_ITEMS = [
    { label: 'CCR Condo Avg PSF',       value: 'S$2,286', change: '↑ 2.8%', up: true,  sub: 'Q1 2026 · Marina Bay / Orchard' },
    { label: 'OCR HDB 4-Room Median',   value: 'S$556k',  change: '↑ 1.9%', up: true,  sub: 'Q1 2026 · Jurong West / Woodlands' },
    { label: 'RCR Condo Avg PSF',       value: 'S$1,904', change: '↑ 2.1%', up: true,  sub: 'Q1 2026 · Queenstown / Toa Payoh' },
    { label: 'Bishan HDB 5-Room Median',value: 'S$796k',  change: '↑ 3.2%', up: true,  sub: 'Q1 2026 · Bishan' },
    { label: 'Tampines HDB 4-Room Avg', value: 'S$562k',  change: '↑ 1.7%', up: true,  sub: 'Q1 2026 · Tampines' },
    { label: 'Sentosa Cove Condo PSF',  value: 'S$2,148', change: '↓ 0.4%', up: false, sub: 'Q1 2026 · Sentosa Cove' },
];

let _tickerIdx  = 0;
let _tickerBusy = false;

function startMarketTicker() {
    setInterval(() => {
        const viewport = document.getElementById('market-ticker-viewport');
        if (!viewport || _tickerBusy) return;
        _tickerBusy = true;

        const next = (_tickerIdx + 1) % _TICKER_ITEMS.length;
        const item = _TICKER_ITEMS[next];

        // Build incoming slide
        const incoming = document.createElement('div');
        incoming.className = 'market-ticker-slide slide-enter';
        incoming.innerHTML = `
            <p class="text-[10px] font-bold text-slate-400 uppercase tracking-wider mb-0.5">${item.label}</p>
            <p class="text-lg font-bold text-slate-900 dark:text-white leading-tight">
                ${item.value} <span class="text-sm font-normal ${item.up ? 'text-emerald-500' : 'text-rose-500'}">${item.change}</span>
            </p>
            <p class="text-[9px] text-slate-400 mt-0.5">${item.sub}</p>
        `;
        viewport.appendChild(incoming);

        // Trigger transition on next paint
        requestAnimationFrame(() => {
            requestAnimationFrame(() => {
                const current = viewport.querySelector('.slide-active');
                if (current) current.classList.add('slide-exit');
                incoming.classList.remove('slide-enter');
                incoming.classList.add('slide-active');

                setTimeout(() => {
                    if (current) current.remove();
                    _tickerIdx  = next;
                    _tickerBusy = false;
                }, 500);
            });
        });
    }, 4000);
}

// ── Feedback form ─────────────────────────────────────────────
async function submitFeedback() {
    const name    = document.getElementById('feedback-name')?.value.trim();
    const email   = document.getElementById('feedback-email')?.value.trim();
    const message = document.getElementById('feedback-message')?.value.trim();
    const status  = document.getElementById('feedback-status');
    const btn     = document.getElementById('feedback-submit-btn');

    if (!name || !email || !message) {
        status.className = 'rounded-2xl px-4 py-3 text-sm font-medium bg-rose-50 text-rose-600';
        status.textContent = 'Please fill in all fields before submitting.';
        status.classList.remove('hidden');
        return;
    }
    const emailRe = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRe.test(email)) {
        status.className = 'rounded-2xl px-4 py-3 text-sm font-medium bg-rose-50 text-rose-600';
        status.textContent = 'Please enter a valid email address.';
        status.classList.remove('hidden');
        return;
    }

    btn.disabled = true;
    btn.innerHTML = '<i data-lucide="loader-2" class="w-4 h-4 animate-spin"></i> Sending…';
    lucide.createIcons();
    status.classList.add('hidden');

    try {
        const res  = await fetch('/api/feedback', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name, email, message }),
        });
        const data = await res.json();
        if (res.ok) {
            status.className = 'rounded-2xl px-4 py-3 text-sm font-medium bg-emerald-50 text-emerald-700';
            status.textContent = 'Thank you! Your feedback has been sent to our team.';
            document.getElementById('feedback-name').value    = '';
            document.getElementById('feedback-email').value   = '';
            document.getElementById('feedback-message').value = '';
        } else {
            throw new Error(data.error || 'Failed to send feedback.');
        }
    } catch (e) {
        status.className = 'rounded-2xl px-4 py-3 text-sm font-medium bg-rose-50 text-rose-600';
        status.textContent = `Error: ${e.message}`;
    } finally {
        status.classList.remove('hidden');
        btn.disabled = false;
        btn.innerHTML = '<i data-lucide="send" class="w-4 h-4"></i> Send Feedback';
        lucide.createIcons();
    }
}



