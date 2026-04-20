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

    if (viewId === 'guides') {
        loadAgents();
        switchGuideTab('hdb');
    }

    if (viewId === 'predict') {
        renderRecentSearches();
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

function _isHdbMode() {
    const el = document.getElementById('input-property-type');
    return !el || el.value === 'HDB';
}

function updateSlider(id) {
    const range   = document.getElementById('range-' + id);
    const display = document.getElementById('val-' + id);
    if (!range) return;
    if (id === 'area' && _availableAreas.length > 0) {
        // Index-based: slider value is index into _availableAreas
        const idx    = Math.min(parseInt(range.value), _availableAreas.length - 1);
        const actual = _availableAreas[idx];
        range.dataset.actualValue = actual;
        if (display) {
            if (_isHdbMode()) {
                const sqft = Math.round(actual * 10.764);
                display.innerText = `${actual} sqm / ${sqft.toLocaleString()} sq ft`;
            } else {
                display.innerText = `${actual.toLocaleString()} sq ft`;
            }
        }
    } else {
        if (display) display.innerText = id === 'area' ? parseInt(range.value).toLocaleString() : range.value;
    }
    if (id === 'bedrooms') _loadFlatSpecs();
}

function getAreaValue() {
    const range = document.getElementById('range-area');
    if (!range) return 90;
    if (_availableAreas.length > 0) {
        const idx = Math.min(parseInt(range.value), _availableAreas.length - 1);
        return _availableAreas[idx];
    }
    return parseInt(range.value);
}

function _onPropertyTypeChange() {
    const propEl    = document.getElementById('input-property-type');
    const isHdb     = propEl && propEl.value === 'HDB';
    const hdbSpecs  = document.getElementById('hdb-specs');
    const condoSpecs= document.getElementById('condo-specs');
    if (hdbSpecs)   hdbSpecs.classList.toggle('hidden', !isHdb);
    if (condoSpecs) condoSpecs.classList.toggle('hidden', isHdb);
    _availableAreas = [];   // reset so slider re-snaps
    _loadFlatSpecs();
}

function _onFlatTypeChange() {
    _availableAreas = [];
    _loadFlatSpecs();
}

// Flat type mapping (bedrooms → HDB flat type)
const _BEDS_TO_FLAT_TYPE = {
    1: '1 ROOM', 2: '2 ROOM', 3: '3 ROOM', 4: '4 ROOM', 5: '5 ROOM', 6: 'EXECUTIVE',
};

function _storeyRangeMidpoint(range) {
    const parts = range.split(' TO ');
    if (parts.length === 2) return Math.round((parseInt(parts[0]) + parseInt(parts[1])) / 2);
    return parseInt(range) || 10;
}

function _populateFloorRanges(ranges, maxFloor, defaultRange) {
    const sel = document.getElementById('input-floor-range');
    if (!sel) return;
    if (!ranges || ranges.length === 0) {
        // Generate synthetic ranges up to maxFloor in steps of 3
        ranges = [];
        for (let lo = 1; lo <= maxFloor; lo += 3) {
            const hi = Math.min(lo + 2, maxFloor);
            ranges.push(`${String(lo).padStart(2,'0')} TO ${String(hi).padStart(2,'0')}`);
        }
    }
    const prevVal = sel.value;
    sel.innerHTML = ranges.map(r => {
        const label = r.replace(' TO ', ' to ');
        return `<option value="${r}">${label}</option>`;
    }).join('');

    // Priority: 1) most-frequent range for this block, 2) previous selection, 3) first option
    if (defaultRange && [...sel.options].some(o => o.value === defaultRange)) {
        sel.value = defaultRange;
    } else if ([...sel.options].some(o => o.value === prevVal)) {
        sel.value = prevVal;
    }
}

async function _loadFlatSpecs() {
    const propEl = document.getElementById('input-property-type');
    const propType = propEl ? propEl.value : 'HDB';
    const isHdb  = propType === 'HDB';
    const postal = document.getElementById('input-postal')?.value?.trim() || '';

    // Determine flat_type or bedrooms to send
    let urlExtra = '';
    if (isHdb) {
        const ftEl = document.getElementById('input-flat-type');
        const ft   = ftEl ? ftEl.value : '4 ROOM';
        urlExtra = `&flat_type=${encodeURIComponent(ft)}`;
    } else {
        const bedsEl = document.getElementById('range-bedrooms');
        const beds   = bedsEl ? parseInt(bedsEl.value) : 3;
        urlExtra = `&bedrooms=${beds}`;
    }

    try {
        const url = `/api/property-areas?property_type=${encodeURIComponent(propType)}` +
                    urlExtra +
                    (postal ? `&postal=${encodeURIComponent(postal)}` : '') +
                    (_predictBlock   ? `&block=${encodeURIComponent(_predictBlock)}`     : '') +
                    (_predictRoad    ? `&road=${encodeURIComponent(_predictRoad)}`       : '') +
                    (_predictTown    ? `&town=${encodeURIComponent(_predictTown)}`       : '') +
                    (_predictProject ? `&project=${encodeURIComponent(_predictProject)}` : '');
        const res  = await fetch(url);
        const data = await res.json();

        const areas = data.floor_areas || [];
        // Use cached floor data from property_lookup if backend couldn't narrow it down
        // Default 20 (not 50) — typical HDB mid-range, avoids inflating predictions
        const maxFloor     = data.max_floor     || _cachedMaxFloor     || 20;
        const storeyRanges = (data.storey_ranges && data.storey_ranges.length)
                             ? data.storey_ranges
                             : (_cachedStoreyRanges.length ? _cachedStoreyRanges : []);
        const defaultRange = data.default_storey_range || null;

        // ── Floor range dropdown (HDB only) ──────────────────────
        if (isHdb) _populateFloorRanges(storeyRanges, maxFloor, defaultRange);

        // ── Floor slider (condo only) ─────────────────────────────
        if (!isHdb) {
            const floorRange   = document.getElementById('range-floor');
            const floorHint    = document.getElementById('floor-max-hint');
            const floorDisplay = document.getElementById('floor-max-display');
            if (floorRange) {
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
        }

        // ── Area slider (index-based snap, shared) ────────────────
        const areaRange = document.getElementById('range-area');
        const areaHint  = document.getElementById('area-range-hint');
        if (areaRange && areas.length > 0) {
            _availableAreas = areas;

            const curActual = parseInt(areaRange.dataset.actualValue || areas[Math.floor(areas.length / 2)]);
            const nearestIdx = areas.reduce((best, v, i) =>
                Math.abs(v - curActual) < Math.abs(areas[best] - curActual) ? i : best, 0);

            areaRange.min   = 0;
            areaRange.max   = areas.length - 1;
            areaRange.step  = 1;
            areaRange.value = nearestIdx;
            updateSlider('area');

            if (areaHint) {
                const min = areas[0], max = areas[areas.length - 1];
                if (isHdb) {
                    areaHint.textContent = `${areas.length} size option${areas.length > 1 ? 's' : ''}: `
                        + `${min}–${max} sqm (${Math.round(min*10.764).toLocaleString()}–${Math.round(max*10.764).toLocaleString()} sq ft)`;
                } else {
                    areaHint.textContent = `${areas.length} size option${areas.length > 1 ? 's' : ''}: `
                        + `${min.toLocaleString()}–${max.toLocaleString()} sq ft`;
                }
            }
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

                // Property not found — show modal
                if (info.error || (!info.property_type && !info.town)) {
                    openPropertyNotFoundModal(postal);
                    return;
                }

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
                _predictTown    = info.town         || '';
                _predictBlock   = info.block        || '';
                _predictRoad    = info.road_name    || '';
                _predictProject = info.project_name || '';
                // Cache remaining lease years from DB for use in the chart
                if (info.remaining_lease_years != null) {
                    window._cachedRemainingLease = info.remaining_lease_years;
                } else {
                    window._cachedRemainingLease = null;
                }
                // Apply floor data from DB lookup if available
                if (info.storey_ranges && info.storey_ranges.length) {
                    _cachedStoreyRanges = info.storey_ranges;
                    _cachedMaxFloor     = info.max_floor || 50;
                } else if (info.max_floor) {
                    _cachedMaxFloor     = info.max_floor;
                    _cachedStoreyRanges = [];
                } else {
                    _cachedStoreyRanges = [];
                    _cachedMaxFloor     = null;
                }
                // Show/hide correct spec section, then apply cached floor data
                _onPropertyTypeChange();
            })
            .catch(() => { openPropertyNotFoundModal(postal); });
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
    const propType = document.getElementById('input-property-type')?.value || 'HDB';
    const isHdb    = propType === 'HDB';

    let floor, flatType, bedrooms;
    if (isHdb) {
        const floorRangeSel = document.getElementById('input-floor-range');
        const rangeVal = floorRangeSel ? floorRangeSel.value : '10';
        floor    = _storeyRangeMidpoint(rangeVal);
        flatType = document.getElementById('input-flat-type')?.value || '4 ROOM';
        bedrooms = {'1 ROOM':1,'2 ROOM':2,'3 ROOM':3,'4 ROOM':4,'5 ROOM':5,'EXECUTIVE':6}[flatType] || 4;
    } else {
        floor    = parseInt(document.getElementById('range-floor')?.value || 10);
        bedrooms = parseInt(document.getElementById('range-bedrooms')?.value || 3);
        flatType = null;
    }

    try {
        const body = { postal, area, bedrooms, floor, property_type: propType, town: _predictTown };
        if (flatType) body.flat_type = flatType;
        const response = await fetch('/api/predict', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });
        const data = await response.json();

        document.getElementById('output-price').innerText = `S$${data.estimated_value.toLocaleString()}`;
        document.getElementById('output-confidence').innerText = `${data.confidence}%`;
        const ppsfEl = document.getElementById('output-ppsf');
        if (ppsfEl && data.ppsf) ppsfEl.innerText = `S$${data.ppsf.toLocaleString()}`;

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

        // Lease decay chart — prefer property-lookup remaining lease, fall back to model median
        const leaseType  = document.getElementById('input-lease-type')?.value || '';
        const isFreehold = leaseType.toLowerCase().includes('freehold');
        const remLease   = isFreehold ? 0
            : (window._cachedRemainingLease != null ? window._cachedRemainingLease
                : (data.remaining_lease_years || 70));
        renderLeaseDecayChart(data.estimated_value, remLease, propType);

        // 12-month price forecast chart
        if (data.price_forecast && data.price_forecast.length) {
            renderForecastChart(data.estimated_value, data.price_forecast);
        }

        // Save to recent searches
        const addrEl = document.getElementById('display-address');
        saveRecentSearch({
            postal,
            address: addrEl ? addrEl.innerText : postal,
            property_type: propType,
            estimate: data.estimated_value,
            date: new Date().toLocaleDateString('en-SG', { day: 'numeric', month: 'short', year: 'numeric' })
        });
        renderRecentSearches();

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

    if (tabId === 'security') load2FAStatus();
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
let _predictTown    = '';
let _predictBlock   = '';
let _predictRoad    = '';
let _predictProject = '';
let _cachedStoreyRanges = [];
let _cachedMaxFloor     = null;

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
    // Only offer prediction when confirmed in our DB and not landed (URA also stores landed sales)
    const dbConfirmed = propInfo?.db_is_hdb === true || propInfo?.db_is_condo === true;
    const canPredict  = postal && dbConfirmed && !isLanded;
    const propType    = propInfo?.property_type || '';

    // Prediction availability badge
    const predBadge = canPredict
        ? `<div style="background:linear-gradient(135deg,#dcfce7,#bbf7d0);border:1px solid #86efac;border-radius:10px;padding:5px 10px;margin-bottom:8px;display:flex;align-items:center;gap:5px">
            <span style="width:7px;height:7px;border-radius:50%;background:#16a34a;flex-shrink:0"></span>
            <span style="font-size:10px;font-weight:700;color:#15803d">Price prediction available for this ${propType}!</span>
           </div>`
        : `<div style="background:#fefce8;border:1px solid #fde047;border-radius:10px;padding:5px 10px;margin-bottom:8px">
            <span style="font-size:10px;font-weight:700;color:#a16207">${isLanded ? 'Landed property' : 'Property not in our database'} — explore only</span>
           </div>`;

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

function setNeighbourhood(name, _btn) {
    // Legacy shim — delegates to new handler
    setNeighbourhoodName(name);
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

function getPolicyAlerts(profile, value, _rate) {
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
    'Ang Mo Kio':       { base: 480000, growth: 0.019 },
    'Bedok':            { base: 420000, growth: 0.016 },
    'Bishan':           { base: 720000, growth: 0.022 },
    'Bukit Batok':      { base: 390000, growth: 0.015 },
    'Bukit Merah':      { base: 580000, growth: 0.021 },
    'Bukit Panjang':    { base: 370000, growth: 0.014 },
    'Bukit Timah':      { base: 1200000, growth: 0.025 },
    'Central Area':     { base: 1400000, growth: 0.028 },
    'Choa Chu Kang':    { base: 360000, growth: 0.013 },
    'Clementi':         { base: 430000, growth: 0.018 },
    'Geylang':          { base: 420000, growth: 0.016 },
    'Hougang':          { base: 380000, growth: 0.015 },
    'Jurong East':      { base: 410000, growth: 0.016 },
    'Jurong West':      { base: 350000, growth: 0.013 },
    'Kallang/Whampoa':  { base: 550000, growth: 0.021 },
    'Marine Parade':    { base: 630000, growth: 0.022 },
    'Marina Bay':       { base: 1800000, growth: 0.031 },
    'Novena':           { base: 850000, growth: 0.023 },
    'Orchard':          { base: 2100000, growth: 0.028 },
    'Pasir Ris':        { base: 390000, growth: 0.015 },
    'Potong Pasir':     { base: 510000, growth: 0.020 },
    'Punggol':          { base: 400000, growth: 0.017 },
    'Queenstown':       { base: 520000, growth: 0.022 },
    'River Valley':     { base: 1600000, growth: 0.027 },
    'Sembawang':        { base: 350000, growth: 0.013 },
    'Sengkang':         { base: 400000, growth: 0.017 },
    'Sentosa':          { base: 2500000, growth: 0.025 },
    'Serangoon':        { base: 510000, growth: 0.019 },
    'Tampines':         { base: 420000, growth: 0.016 },
    'Toa Payoh':        { base: 460000, growth: 0.020 },
    'Woodlands':        { base: 340000, growth: 0.013 },
    'Yishun':           { base: 360000, growth: 0.014 },
    'Paya Lebar':       { base: 490000, growth: 0.019 },
    'Buona Vista':      { base: 800000, growth: 0.022 },
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

    // Update subtitle to reflect current PSF type + neighbourhood
    const psfLabel = PSF_TYPE_CONFIG[_currentPsfType]?.label || 'Historical price index';
    const subtitle = document.getElementById('trend-chart-subtitle');
    if (subtitle) subtitle.innerText = `${psfLabel} — ${currentNeighbourhood}`;

    const isDark = document.documentElement.classList.contains('dark');
    const gradient = ctx.createLinearGradient(0, 0, 0, 380);
    gradient.addColorStop(0, 'rgba(99, 179, 237, 0.35)');
    gradient.addColorStop(1, 'rgba(99, 179, 237, 0.0)');

    let labels = [], prices = [];

    try {
        if (range === '6m') {
            // Map neighbourhood display name → HDB town name for API
            const townMap = {
                'Ang Mo Kio':'ANG MO KIO','Bedok':'BEDOK','Bishan':'BISHAN','Bukit Batok':'BUKIT BATOK',
                'Bukit Merah':'BUKIT MERAH','Bukit Panjang':'BUKIT PANJANG','Bukit Timah':'BUKIT TIMAH',
                'Central Area':'CENTRAL AREA','Choa Chu Kang':'CHOA CHU KANG','Clementi':'CLEMENTI',
                'Geylang':'GEYLANG','Hougang':'HOUGANG','Jurong East':'JURONG EAST','Jurong West':'JURONG WEST',
                'Kallang/Whampoa':'KALLANG/WHAMPOA','Marine Parade':'MARINE PARADE','Pasir Ris':'PASIR RIS',
                'Punggol':'PUNGGOL','Queenstown':'QUEENSTOWN','Sembawang':'SEMBAWANG','Sengkang':'SENGKANG',
                'Serangoon':'SERANGOON','Tampines':'TAMPINES','Toa Payoh':'TOA PAYOH',
                'Woodlands':'WOODLANDS','Yishun':'YISHUN'
            };
            const apiTown = townMap[currentNeighbourhood] || '';
            const trendUrl = apiTown ? `/api/trend?town=${encodeURIComponent(apiTown)}` : '/api/trend';
            const res = await fetch(trendUrl);
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

    // PSF type override
    const psfCfg = PSF_TYPE_CONFIG[_currentPsfType] || PSF_TYPE_CONFIG['avg'];
    if (_currentPsfType !== 'avg') {
        const cfg = NEIGHBOURHOOD_BASE[currentNeighbourhood] || NEIGHBOURHOOD_BASE['Clementi'];
        // Scale PSF base by neighbourhood price factor
        const neighbourhoodFactor = cfg.base / 560000; // 4-room baseline
        const psfBase = psfCfg.psfBase * Math.sqrt(neighbourhoodFactor) * psfCfg.growth;
        const months = range === '6m' ? 6 : range === '1y' ? 12 : range === '3y' ? 36 : 60;
        const step = range === '5y' ? 6 : range === '3y' ? 3 : 1;
        prices = []; labels = [];
        const now = new Date();
        let p = psfBase * Math.pow(1 - cfg.growth / 12, months);
        for (let i = months; i >= 0; i--) {
            p = p * (1 + cfg.growth / 12 + (Math.sin(i * 0.7) * 0.001));
            if (i % step !== 0) continue;
            const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
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
    }

    const tickColor  = isDark ? '#93C5FD' : '#64748B';
    const gridColor = isDark ? 'rgba(147,197,253,0.08)' : 'rgba(0,0,0,0.04)';
    const isPsf      = _currentPsfType !== 'avg';
    const chartColor = isPsf ? '#7C3AED' : '#3B82F6';
    const chartGrad  = ctx.createLinearGradient(0, 0, 0, 380);
    chartGrad.addColorStop(0, isPsf ? 'rgba(124,58,237,0.3)' : 'rgba(99, 179, 237, 0.35)');
    chartGrad.addColorStop(1, 'rgba(0,0,0,0)');

    trendChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels,
            datasets: [{
                label: psfCfg.label,
                data: prices,
                borderColor: chartColor,
                borderWidth: 3,
                fill: true,
                backgroundColor: chartGrad,
                tension: 0.4,
                pointRadius: 3,
                pointBackgroundColor: chartColor,
                pointBorderColor: isDark ? '#1E3A5F' : '#fff',
                pointBorderWidth: 2,
                pointHoverRadius: 7,
                pointHoverBackgroundColor: isPsf ? '#A78BFA' : '#60A5FA',
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
                        label: ctx => isPsf
                            ? `${psfCfg.label}: S$${ctx.parsed.y.toLocaleString()}/sqft`
                            : `Avg Price: S$${ctx.parsed.y.toLocaleString()}`
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: false,
                    grid: { color: gridColor, drawBorder: false },
                    ticks: { font: { size: 11, weight: '600' }, color: tickColor,
                        callback: v => isPsf ? `S$${v}` : (v >= 1000000 ? `${(v/1000000).toFixed(1)}M` : `${(v/1000).toFixed(0)}K`) }
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

        const ppsf = item.floor_area > 0 ? Math.round(item.price / item.floor_area) : null;
        row.innerHTML = `
            <td class="py-8 pl-4">
                <p class="font-bold text-slate-900">${item.address}</p>
                <p class="text-xs text-slate-400">${item.storey || '–'}</p>
            </td>
            <td class="py-8 text-sm font-medium text-slate-600">${item.type}</td>
            <td class="py-8">
                <span class="font-bold text-slate-900">S$${item.price.toLocaleString()}</span>
                ${ppsf ? `<p class="text-[10px] text-slate-400">S$${ppsf.toLocaleString()} psf</p>` : ''}
            </td>
            <td class="py-8 text-sm text-slate-500">${item.floor_area ? item.floor_area.toLocaleString() + ' sqft' : '–'}</td>
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
    const files = event.target.files;
    const label = document.getElementById('upload-file-label');
    if (!files.length) return;
    if (label) label.textContent = files.length === 1 ? files[0].name : `${files.length} files selected`;
}

async function handleCsvUpload() {
    const input  = document.getElementById('tx-csv-input');
    const status = document.getElementById('upload-status');
    const btn    = document.getElementById('upload-submit-btn');
    if (!input || !input.files.length) { showToast('Please select a CSV file first'); return; }

    const files = Array.from(input.files);
    if (files.length === 1) {
        const fd = new FormData();
        fd.append('file', files[0]);
        fd.append('type', _uploadType);
        await _doUploadFile(fd, status, btn, input);
        return;
    }

    // Multiple files — sequential uploads
    btn.disabled = true;
    btn.innerHTML = '<i data-lucide="loader-2" class="w-4 h-4 animate-spin"></i> Uploading…';
    lucide.createIcons();
    if (status) { status.className = 'text-sm text-slate-500'; status.textContent = `Uploading ${files.length} files…`; status.classList.remove('hidden'); }

    let totalInserted = 0, totalRows = 0, errors = [];
    for (let i = 0; i < files.length; i++) {
        const fname = files[i].name;
        if (status) status.textContent = `Uploading file ${i + 1} of ${files.length}: ${fname}…`;
        const formData = new FormData();
        formData.append('file', files[i]);
        formData.append('type', _uploadType);
        try {
            const res  = await fetch('/api/admin/upload-transactions', { method: 'POST', body: formData });
            const data = await res.json();
            if (data.error) { errors.push(`${fname}: ${data.error}`); continue; }

            // Postgres returns a job_id — poll until done
            if (data.job_id) {
                if (status) status.textContent = `Processing ${fname}… (this may take a minute)`;
                const result = await _awaitUploadJob(data.job_id);
                if (result.error) { errors.push(`${fname}: ${result.error}`); continue; }
                totalInserted += result.inserted || 0;
                totalRows     += result.total_rows || result.inserted || 0;
            } else {
                totalInserted += data.inserted || 0;
                totalRows     += data.total_rows || 0;
            }
        } catch (e) { errors.push(`${fname}: ${e.message}`); }
    }

    btn.disabled = false;
    btn.innerHTML = '<i data-lucide="upload" class="w-4 h-4"></i> Upload to Database';
    lucide.createIcons();
    input.value = '';
    const label2 = document.getElementById('upload-file-label');
    if (label2) label2.textContent = 'Click to select a file';

    if (errors.length) {
        if (status) { status.className = 'text-sm text-red-500 font-medium'; status.textContent = `Errors: ${errors.join('; ')}`; }
    } else {
        const msg = `Uploaded ${totalInserted.toLocaleString()} of ${totalRows.toLocaleString()} rows across ${files.length} files.`;
        if (status) { status.className = 'text-sm text-emerald-600 font-medium'; status.textContent = msg; }
        showToast(msg);
    }
    loadDataTabStats();
}

async function _doUploadFile(formData, status, btn, input) {
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
            if (label) label.textContent = 'Click to select a file';
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
        if (label2) label2.textContent = 'Click to select a file';
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

// ── URA API Sync ─────────────────────────────────────────────
async function handleSyncUra() {
    const btn    = document.getElementById('ura-sync-btn');
    const status = document.getElementById('ura-sync-status');
    if (!btn) return;
    btn.disabled = true;
    btn.innerHTML = '<i data-lucide="loader-2" class="w-4 h-4 animate-spin"></i> Syncing…';
    lucide.createIcons();
    if (status) { status.classList.remove('hidden'); status.textContent = 'Connecting to URA DataService…'; }
    try {
        const res  = await fetch('/api/admin/sync-ura', { method: 'POST' });
        const data = await res.json();
        if (data.error) throw new Error(data.error);
        const msg = data.message || `Synced ${(data.inserted||0).toLocaleString()} new records.`;
        if (status) status.textContent = msg;
        showToast(msg);
        loadDataTabStats();
    } catch (e) {
        if (status) status.textContent = `Error: ${e.message}`;
        showToast('URA sync failed: ' + e.message);
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i data-lucide="refresh-cw" class="w-4 h-4"></i> Sync URA Transactions';
        lucide.createIcons();
    }
}

// ── Property Not Found Modal ──────────────────────────────────
function openPropertyNotFoundModal(postal) {
    const modal   = document.getElementById('property-not-found-modal');
    const postalEl = document.getElementById('pnf-postal');
    const statusEl = document.getElementById('pnf-status');
    const contactEl = document.getElementById('pnf-contact');
    const descEl   = document.getElementById('pnf-desc');
    if (!modal) return;
    if (postalEl) postalEl.value = postal || '';
    if (statusEl) { statusEl.classList.add('hidden'); statusEl.textContent = ''; }
    if (contactEl) contactEl.value = '';
    if (descEl) descEl.value = '';
    modal.classList.remove('hidden');
    modal.classList.add('flex');
}

function closePropertyNotFoundModal() {
    const modal = document.getElementById('property-not-found-modal');
    if (modal) { modal.classList.add('hidden'); modal.classList.remove('flex'); }
}

async function submitPropertyRequest() {
    const postal   = document.getElementById('pnf-postal')?.value || '';
    const contact  = document.getElementById('pnf-contact')?.value || '';
    const desc     = document.getElementById('pnf-desc')?.value || '';
    const statusEl = document.getElementById('pnf-status');
    try {
        const res  = await fetch('/api/property-request', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ postal, contact, description: desc })
        });
        const data = await res.json();
        if (data.error) throw new Error(data.error);
        if (statusEl) { statusEl.textContent = 'Request submitted! We\'ll review and add it soon.'; statusEl.classList.remove('hidden'); }
        setTimeout(closePropertyNotFoundModal, 2200);
    } catch (e) {
        if (statusEl) { statusEl.textContent = `Error: ${e.message}`; statusEl.classList.remove('hidden'); statusEl.className = 'text-xs text-center mt-3 text-red-500 font-medium'; }
    }
}

// ── Neighbourhood PSF / Search ────────────────────────────────
const ALL_NEIGHBOURHOODS = [
    'Ang Mo Kio','Bedok','Bishan','Bukit Batok','Bukit Merah','Bukit Panjang',
    'Bukit Timah','Central Area','Choa Chu Kang','Clementi','Geylang',
    'Hougang','Jurong East','Jurong West','Kallang/Whampoa','Marine Parade',
    'Marina Bay','Orchard','Pasir Ris','Punggol','Queenstown','Sembawang',
    'Sengkang','Serangoon','Tampines','Toa Payoh','Woodlands','Yishun',
    'Novena','River Valley','Sentosa','Potong Pasir','Buona Vista','Paya Lebar'
];

let _currentPsfType = 'avg';

const PSF_TYPE_CONFIG = {
    avg:    { label: 'Overall Avg Price (S$)', psfFactor: null,  growth: 1.0,  yTick: v => 'S$' + (v/1000).toFixed(0) + 'k' },
    hdb2:   { label: 'HDB 2-Room PSF (S$/sqft)', psfBase: 710,  growth: 0.90, yTick: v => 'S$' + v },
    hdb3:   { label: 'HDB 3-Room PSF (S$/sqft)', psfBase: 680,  growth: 0.95, yTick: v => 'S$' + v },
    hdb4:   { label: 'HDB 4-Room PSF (S$/sqft)', psfBase: 655,  growth: 1.00, yTick: v => 'S$' + v },
    hdb5:   { label: 'HDB 5-Room PSF (S$/sqft)', psfBase: 620,  growth: 1.00, yTick: v => 'S$' + v },
    hdb3gen:{ label: '3-Gen HDB PSF (S$/sqft)',  psfBase: 555,  growth: 0.85, yTick: v => 'S$' + v },
    condo:  { label: 'Private Condo PSF (S$/sqft)', psfBase: 1640, growth: 1.20, yTick: v => 'S$' + v },
};

function setPsfType(type) {
    _currentPsfType = type;
    document.querySelectorAll('.psf-type-btn').forEach(b => {
        b.className = 'psf-type-btn px-3 py-1.5 rounded-xl text-xs font-bold bg-slate-100 text-slate-600 hover:bg-slate-200 transition-colors';
    });
    const activeBtn = document.querySelector(`[onclick="setPsfType('${type}')"]`);
    if (activeBtn) activeBtn.className = 'psf-type-btn px-3 py-1.5 rounded-xl text-xs font-bold bg-violet-600 text-white transition-colors';
    setTimeout(() => initTrendChart(currentRange), 50);
}

function setNeighbourhoodName(name) {
    // Close dropdown, update quick buttons, then set neighbourhood
    const dd = document.getElementById('neighbourhood-dropdown');
    const inp = document.getElementById('neighbourhood-search');
    if (dd) { dd.classList.add('hidden'); dd.innerHTML = ''; }
    if (inp) inp.value = '';

    document.querySelectorAll('.neighbourhood-btn').forEach(b => {
        b.className = 'neighbourhood-btn px-3 py-1.5 rounded-xl text-xs font-bold bg-slate-100 text-slate-600 hover:bg-slate-200 transition-colors';
    });
    const matching = document.querySelector(`[onclick="setNeighbourhoodName('${name}')"]`);
    if (matching) matching.className = 'neighbourhood-btn px-3 py-1.5 rounded-xl text-xs font-bold bg-blue-600 text-white transition-colors';

    currentNeighbourhood = name;
    const subtitle = document.getElementById('trend-chart-subtitle');
    if (subtitle) subtitle.innerText = `${PSF_TYPE_CONFIG[_currentPsfType]?.label || 'Historical price index'} — ${name}`;
    renderTrendNews(name);
    setTimeout(() => initTrendChart(currentRange), 50);
}

function filterNeighbourhoods(query) {
    const dd = document.getElementById('neighbourhood-dropdown');
    if (!dd) return;
    const q = query.trim().toLowerCase();
    if (!q) { dd.classList.add('hidden'); dd.innerHTML = ''; return; }
    const matches = ALL_NEIGHBOURHOODS.filter(n => n.toLowerCase().includes(q)).slice(0, 8);
    if (!matches.length) { dd.classList.add('hidden'); return; }
    dd.innerHTML = matches.map(n => `
        <button onclick="setNeighbourhoodName('${n}');document.getElementById('neighbourhood-search').value=''"
            class="w-full text-left px-4 py-2.5 text-sm hover:bg-slate-50 transition-colors font-medium text-slate-700 first:rounded-t-xl last:rounded-b-xl">
            ${n}
        </button>`).join('');
    dd.classList.remove('hidden');
}

// Close neighbourhood dropdown on outside click
document.addEventListener('click', e => {
    const dd  = document.getElementById('neighbourhood-dropdown');
    const inp = document.getElementById('neighbourhood-search');
    if (dd && inp && !dd.contains(e.target) && e.target !== inp) {
        dd.classList.add('hidden');
    }
});

// Awaitable version — resolves with {inserted, total_rows} or {error}
async function _awaitUploadJob(jobId) {
    const start = Date.now();
    while (true) {
        await new Promise(r => setTimeout(r, 2500));
        try {
            const res  = await fetch(`/api/admin/upload-status?job_id=${jobId}`);
            const data = await res.json();
            if (data.state === 'done')   return { inserted: data.inserted, total_rows: data.inserted };
            if (data.state === 'error')  return { error: data.message };
            if (Date.now() - start > 300000) return { error: 'Timed out after 5 min' };
        } catch (_) { /* keep polling */ }
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

    loadModelStatus();
}

// ── Admin: URA Sync ───────────────────────────────────────────
// ── Model management ──────────────────────────────────────────

let _trainingPollInterval = null;
let _trainingPollStart    = null;

async function handleTriggerTraining(type) {
    ['hdb', 'private', 'both'].forEach(t => {
        const b = document.getElementById(`trigger-${t}-btn`);
        if (b) { b.disabled = true; b.classList.add('opacity-50'); }
    });
    const status = document.getElementById('trigger-training-status');
    status.className = 'mt-3 text-xs rounded-xl p-3 bg-slate-100 text-slate-600';
    status.textContent = 'Contacting GitHub API…';
    status.classList.remove('hidden');
    try {
        const res  = await _fetchWithRetry('/api/admin/trigger-training', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ type }),
        }, { retries: 3, delayMs: 6000 });
        const data = await res.json();
        if (res.ok) {
            status.className = 'mt-3 text-xs rounded-xl p-3 bg-emerald-50 text-emerald-700';
            status.textContent = `Queued on GitHub Actions. Training takes ~10–15 min — the status panel will update automatically when done.`;
            _startTrainingPoll();
        } else {
            status.className = 'mt-3 text-xs rounded-xl p-3 bg-rose-50 text-rose-700';
            status.textContent = `Error: ${data.error || 'Unknown error'}`;
            _triggerEnableButtons();
        }
    } catch (e) {
        status.className = 'mt-3 text-xs rounded-xl p-3 bg-rose-50 text-rose-700';
        status.textContent = 'Network error — could not reach the server.';
        _triggerEnableButtons();
    }
}

function _triggerEnableButtons() {
    ['hdb', 'private', 'both'].forEach(t => {
        const b = document.getElementById(`trigger-${t}-btn`);
        if (b) { b.disabled = false; b.classList.remove('opacity-50'); }
    });
}

function _startTrainingPoll() {
    if (_trainingPollInterval) clearInterval(_trainingPollInterval);
    _trainingPollStart = Date.now();
    // snapshot trained_at before we started so we can detect a new model
    let _prevTrainedAt = null;
    fetch('/api/admin/model-status').then(r => r.json()).then(d => {
        _prevTrainedAt = d?.hdb?.trained_at || d?.private?.trained_at || null;
    }).catch(() => {});

    _trainingPollInterval = setInterval(async () => {
        const elapsed = Math.round((Date.now() - _trainingPollStart) / 60000);
        const status  = document.getElementById('trigger-training-status');

        if (elapsed >= 25) {
            clearInterval(_trainingPollInterval); _trainingPollInterval = null;
            _triggerEnableButtons();
            if (status) {
                status.className  = 'mt-3 text-xs rounded-xl p-3 bg-amber-50 text-amber-700';
                status.textContent = 'Timed out waiting for GitHub Actions (25 min). Check the Actions tab in your repo.';
            }
            return;
        }

        try {
            const res  = await fetch('/api/admin/model-status');
            const data = await res.json();
            loadModelStatus();  // refresh dots
            // detect a newer trained_at vs what we had before triggering
            const newAt = data?.hdb?.trained_at || data?.private?.trained_at || null;
            if (newAt && newAt !== _prevTrainedAt) {
                clearInterval(_trainingPollInterval); _trainingPollInterval = null;
                _triggerEnableButtons();
                if (status) {
                    status.className  = 'mt-3 text-xs rounded-xl p-3 bg-emerald-50 text-emerald-700 font-medium';
                    status.textContent = `Training complete! Models updated — check the status panel.`;
                }
                return;
            }
        } catch (_) {}

        if (status) status.textContent = `Queued on GitHub Actions — ${elapsed} min elapsed, waiting for models…`;
    }, 30000);
}

async function loadModelStatus() {
    try {
        const res  = await fetch('/api/admin/model-status');
        const data = await res.json();
        for (const [key, info] of Object.entries(data)) {
            const dot   = document.getElementById(`model-${key}-dot`);
            const state = document.getElementById(`model-${key}-state`);
            const date  = document.getElementById(`model-${key}-date`);
            if (!dot) continue;
            if (info.live) {
                dot.className     = 'w-2.5 h-2.5 rounded-full bg-emerald-500';
                state.textContent = 'Live';
                state.className   = 'text-xs font-bold text-emerald-600';
                if (info.trained_at) {
                    const d = new Date(info.trained_at);
                    date.textContent = 'Last updated: ' + d.toLocaleDateString('en-SG', { day: 'numeric', month: 'short', year: 'numeric' });
                } else {
                    date.textContent = 'Last updated: unknown';
                }
            } else {
                dot.className     = 'w-2.5 h-2.5 rounded-full bg-rose-400';
                state.textContent = 'Not loaded';
                state.className   = 'text-xs font-bold text-rose-500';
                date.textContent  = 'No model file found';
            }
        }
    } catch (e) { /* silent */ }
}

async function handleModelUpload() {
    const input  = document.getElementById('model-file-input');
    const btn    = document.getElementById('model-upload-btn');
    const status = document.getElementById('model-upload-status');
    const label  = document.getElementById('model-file-label');
    if (!input.files.length) { alert('Please select a .joblib file first.'); return; }
    const file = input.files[0];
    btn.disabled = true;
    status.className = 'mt-3 text-sm rounded-xl p-3 bg-slate-100 text-slate-600';
    status.textContent = `Uploading ${file.name}…`;
    status.classList.remove('hidden');
    const form = new FormData();
    form.append('file', file);
    try {
        const res  = await _fetchWithRetry('/api/admin/upload-model', { method: 'POST', body: form }, { retries: 3, delayMs: 6000 });
        const data = await res.json();
        if (res.ok) {
            status.className = 'mt-3 text-sm rounded-xl p-3 bg-emerald-50 text-emerald-700 font-medium';
            status.textContent = data.message || 'Uploaded successfully.';
            input.value = '';
            label.textContent = 'Choose .joblib file';
            loadModelStatus();
        } else {
            status.className = 'mt-3 text-sm rounded-xl p-3 bg-rose-50 text-rose-700 font-medium';
            status.textContent = `Upload failed: ${data.error || 'Unknown error'}`;
        }
    } catch (e) {
        status.className = 'mt-3 text-sm rounded-xl p-3 bg-rose-50 text-rose-700 font-medium';
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

    // 2FA required — show verification modal
    if (data.requires_2fa) {
        window._2faTempToken = data.temp_token;
        const modal = document.getElementById('twofa-verify-modal');
        document.getElementById('twofa-verify-code').value = '';
        document.getElementById('twofa-verify-error').classList.add('hidden');
        document.getElementById('twofa-remember-device').checked = false;
        modal.classList.remove('hidden');
        modal.classList.add('flex');
        setTimeout(() => document.getElementById('twofa-verify-code').focus(), 100);
        return;
    }

    currentUser = data.user;
    saveCurrentUser();
    updateAuthUI();
    loadProfileForm();
    showView('home');
}

function selectAccountType(type) {
    document.getElementById('reg-account-type').value = type;
    const homeBtn  = document.getElementById('reg-type-homeowner');
    const agentBtn = document.getElementById('reg-type-agent');
    if (type === 'homeowner') {
        homeBtn.className  = 'reg-type-btn py-4 rounded-2xl border-2 border-blue-500 bg-blue-50 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 font-bold text-sm transition-all';
        agentBtn.className = 'reg-type-btn py-4 rounded-2xl border-2 border-slate-200 dark:border-slate-600 text-slate-500 dark:text-slate-400 font-bold text-sm transition-all hover:border-blue-400';
    } else {
        agentBtn.className = 'reg-type-btn py-4 rounded-2xl border-2 border-blue-500 bg-blue-50 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 font-bold text-sm transition-all';
        homeBtn.className  = 'reg-type-btn py-4 rounded-2xl border-2 border-slate-200 dark:border-slate-600 text-slate-500 dark:text-slate-400 font-bold text-sm transition-all hover:border-blue-400';
    }
}

async function handleRegister(e) {
    if (e) e.preventDefault();

    const full_name    = (document.getElementById('reg-name')?.value     || '').trim();
    const email        = (document.getElementById('reg-email')?.value    || '').trim();
    const password     = (document.getElementById('reg-password')?.value || '').trim();
    const account_type = (document.getElementById('reg-account-type')?.value || 'homeowner').trim();
    const errEl = document.getElementById('register-error');

    if (!full_name || !email || !password) {
        errEl.textContent = 'All fields are required.';
        errEl.classList.remove('hidden');
        return;
    }
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(email)) {
        errEl.textContent = 'Please enter a valid email address.';
        errEl.classList.remove('hidden');
        return;
    }
    const pwErr = _validatePassword(password);
    if (pwErr) {
        errEl.textContent = pwErr;
        errEl.classList.remove('hidden');
        return;
    }

    const res = await fetch('/api/register', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ full_name, email, password, account_type })
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

    // Prompt new users to set up 2FA
    const prompt = document.getElementById('register-2fa-prompt');
    if (prompt) {
        prompt.classList.remove('hidden');
        prompt.classList.add('flex');
    }
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

    // Agent fields
    const agentFields = document.getElementById('agent-fields');
    if (agentFields) {
        const isAgent = (currentUser.account_type === 'agent');
        agentFields.classList.toggle('hidden', !isAgent);
        if (isAgent) {
            const ceaEl = document.getElementById('profile-cea');
            const waEl  = document.getElementById('profile-whatsapp');
            const bioEl = document.getElementById('profile-bio');
            if (ceaEl) ceaEl.value = currentUser.cea_number || '';
            if (waEl)  waEl.value  = currentUser.whatsapp   || '';
            if (bioEl) bioEl.value = currentUser.bio        || '';
        }
    }
    // Clear password fields on reload
    const curPwEl = document.getElementById('profile-cur-password');
    const newPwEl = document.getElementById('profile-new-password');
    if (curPwEl) curPwEl.value = '';
    if (newPwEl) newPwEl.value = '';
}

function cancelProfileChanges() {
    loadProfileForm();
    loadProfilePhoto();
}

async function saveProfile() {
    if (!currentUser) { alert('Please login first'); return; }

    const firstName    = (document.getElementById('profile-first-name')?.value  || '').trim();
    const lastName     = (document.getElementById('profile-last-name')?.value   || '').trim();
    const email        = (document.getElementById('profile-email')?.value       || '').trim();
    const phone        = (document.getElementById('profile-phone')?.value       || '').trim();
    const cur_password = (document.getElementById('profile-cur-password')?.value || '').trim();
    const new_password = (document.getElementById('profile-new-password')?.value || '').trim();
    const cea_number   = (document.getElementById('profile-cea')?.value         || '').trim();
    const whatsapp     = (document.getElementById('profile-whatsapp')?.value    || '').trim();
    const bio          = (document.getElementById('profile-bio')?.value         || '').trim();

    if (new_password && !cur_password) {
        showToast('Enter your current password to set a new one.', true);
        return;
    }
    if (new_password) {
        const pwErr = _validatePassword(new_password);
        if (pwErr) { showToast(pwErr, true); return; }
    }

    const full_name = `${firstName} ${lastName}`.trim();
    const payload   = { full_name, email, phone, cea_number, whatsapp, bio };
    if (new_password) {
        payload.current_password = cur_password;
        payload.new_password = new_password;
        // If user has 2FA enabled, collect TOTP code
        if (currentUser?.totp_enabled) {
            const code = prompt('Enter your 6-digit authenticator code to confirm the password change:');
            if (!code || code.trim().length !== 6) {
                showToast('Password change cancelled — 2FA code required.', true);
                return;
            }
            payload.totp_code = code.trim();
        }
    }

    const res  = await fetch(`/api/profile/${currentUser.id}`, {
        method: 'PUT', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    });
    const data = await res.json();
    if (data.error) { showToast(data.error, true); return; }

    currentUser = { ...currentUser, ...data.user };
    saveCurrentUser();
    updateAuthUI();
    loadProfileForm();
    showToast('Profile updated successfully');
}

function confirmDeleteAccount() {
    const modal = document.getElementById('delete-account-modal');
    if (modal) { modal.classList.remove('hidden'); modal.classList.add('flex'); }
    const errEl = document.getElementById('delete-account-error');
    if (errEl) errEl.classList.add('hidden');
    const pwEl = document.getElementById('delete-confirm-password');
    if (pwEl) pwEl.value = '';
    lucide.createIcons();
}

async function executeDeleteAccount() {
    if (!currentUser) return;
    const password = (document.getElementById('delete-confirm-password')?.value || '').trim();
    const errEl    = document.getElementById('delete-account-error');
    if (!password) {
        if (errEl) { errEl.textContent = 'Password is required.'; errEl.classList.remove('hidden'); }
        return;
    }
    const res  = await fetch(`/api/profile/${currentUser.id}`, {
        method: 'DELETE', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password })
    });
    const data = await res.json();
    if (data.error) {
        if (errEl) { errEl.textContent = data.error; errEl.classList.remove('hidden'); }
        return;
    }
    // Logged out
    const modal = document.getElementById('delete-account-modal');
    if (modal) { modal.classList.add('hidden'); modal.classList.remove('flex'); }
    currentUser = null;
    localStorage.removeItem('currentUser');
    updateAuthUI();
    showView('home');
    showToast('Account deleted.');
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

function showToast(message, isError = false) {
    const toast = document.getElementById('toast-message');
    if (!toast) return;
    toast.textContent = message;
    toast.classList.remove('hidden', 'bg-emerald-600', 'bg-rose-600');
    toast.classList.add(isError ? 'bg-rose-600' : 'bg-emerald-600');
    setTimeout(() => { toast.classList.add('hidden'); }, 2500);
}

// ── Feedback Modal ────────────────────────────────────────────
function openFeedbackModal() {
    const m = document.getElementById('feedback-modal');
    if (m) { m.classList.remove('hidden'); m.classList.add('flex'); }
    lucide.createIcons();
}
function closeFeedbackModal() {
    const m = document.getElementById('feedback-modal');
    if (m) { m.classList.add('hidden'); m.classList.remove('flex'); }
}

// ── Chatbot ───────────────────────────────────────────────────
let _chatOpen    = false;
let _chatHistory = []; // [{role, content}]

function toggleChat() {
    _chatOpen = !_chatOpen;
    const panel = document.getElementById('chat-panel');
    const emoji = document.getElementById('chat-toggle-emoji');
    if (panel) panel.style.display = _chatOpen ? 'flex' : 'none';
    if (emoji) emoji.textContent   = _chatOpen ? '✕' : '🏡';
    if (_chatOpen) {
        const input = document.getElementById('chat-input');
        if (input) setTimeout(() => input.focus(), 150);
    }
}

async function sendChatMessage() {
    const input = document.getElementById('chat-input');
    const text  = (input?.value || '').trim();
    if (!text) return;
    input.value = '';

    appendChatMessage('user', text);
    _chatHistory.push({ role: 'user', content: text });

    // Typing indicator
    const typingId = 'chat-typing-' + Date.now();
    appendChatMessage('assistant', '…', typingId);

    try {
        const res  = await fetch('/api/chat', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ messages: _chatHistory })
        });
        const data = await res.json();
        const reply = data.reply || 'Sorry, I could not respond.';

        // Replace typing indicator
        const typingEl = document.getElementById(typingId);
        if (typingEl) typingEl.textContent = reply;
        else appendChatMessage('assistant', reply);

        _chatHistory.push({ role: 'assistant', content: reply });
        if (_chatHistory.length > 20) _chatHistory = _chatHistory.slice(-20);
    } catch {
        const typingEl = document.getElementById(typingId);
        if (typingEl) typingEl.textContent = 'Connection error. Please try again.';
    }
}

function appendChatMessage(role, text, id) {
    const container = document.getElementById('chat-messages');
    if (!container) return;
    const isUser = role === 'user';
    const wrapper = document.createElement('div');
    wrapper.className = `flex gap-2 ${isUser ? 'flex-row-reverse' : ''}`;
    const bubble = document.createElement('div');
    if (id) bubble.id = id;
    bubble.className = isUser
        ? 'bg-blue-600 text-white rounded-2xl rounded-tr-sm px-4 py-2.5 text-sm max-w-[80%] ml-auto'
        : 'bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-200 rounded-2xl rounded-tl-sm px-4 py-2.5 text-sm max-w-[80%]';
    bubble.textContent = text;
    if (!isUser) {
        const icon = document.createElement('div');
        icon.className = 'w-7 h-7 bg-blue-100 rounded-full flex items-center justify-center shrink-0 self-end text-sm';
        icon.textContent = '🏡';
        wrapper.appendChild(icon);
    }
    wrapper.appendChild(bubble);
    container.appendChild(wrapper);
    container.scrollTop = container.scrollHeight;
}

// ── Guides — Tab Switching & News ────────────────────────────
function switchGuideTab(tab) {
    ['hdb','condo','financing','policy'].forEach(t => {
        const content = document.getElementById(`guide-content-${t}`);
        const btn     = document.getElementById(`guide-tab-${t}`);
        if (content) content.classList.toggle('hidden', t !== tab);
        if (btn) {
            btn.className = t === tab
                ? 'guide-tab-btn px-5 py-2.5 rounded-xl text-sm font-bold bg-blue-600 text-white transition-all'
                : 'guide-tab-btn px-5 py-2.5 rounded-xl text-sm font-bold bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300 hover:bg-slate-200 transition-all';
        }
    });
    if (tab === 'policy') loadGuidesNews('policy');
}

async function loadGuidesNews(topic = 'policy') {
    // Update filter button states
    document.querySelectorAll('.news-filter-btn').forEach(b => {
        b.className = 'news-filter-btn px-4 py-1.5 rounded-full text-xs font-bold bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300 hover:bg-slate-200';
    });
    const activeBtn = document.querySelector(`.news-filter-btn[onclick="loadGuidesNews('${topic}')"]`);
    if (activeBtn) activeBtn.className = 'news-filter-btn px-4 py-1.5 rounded-full text-xs font-bold bg-blue-600 text-white';

    const loading = document.getElementById('guides-news-loading');
    const list    = document.getElementById('guides-news-list');
    if (loading) { loading.classList.remove('hidden'); loading.textContent = 'Loading news…'; }
    if (list)    list.classList.add('hidden');

    try {
        const res  = await fetch(`/api/guides-news?topic=${topic}`);
        const data = await res.json();
        const articles = data.articles || [];
        if (loading) loading.classList.add('hidden');
        if (!list) return;
        if (!articles.length) {
            list.innerHTML = '<p class="text-sm text-slate-400 text-center py-4">No recent articles found.</p>';
            list.classList.remove('hidden');
            return;
        }
        list.innerHTML = articles.map(a => `
            <a href="${a.url || '#'}" target="_blank" rel="noopener"
               class="flex items-start gap-4 p-4 bg-white dark:bg-slate-800 rounded-2xl border border-slate-100 dark:border-slate-700 hover:border-blue-300 transition-all group no-underline block">
                <div class="w-10 h-10 bg-blue-50 dark:bg-blue-900/30 rounded-xl flex items-center justify-center shrink-0 mt-0.5">
                    <svg class="w-5 h-5 text-blue-600" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M19 20H5a2 2 0 01-2-2V6a2 2 0 012-2h10l6 6v8a2 2 0 01-2 2z"/><path d="M15 2v6h6"/></svg>
                </div>
                <div class="flex-1 min-w-0">
                    <p class="font-bold text-slate-900 dark:text-white text-sm leading-snug group-hover:text-blue-600 transition-colors line-clamp-2">${a.title || ''}</p>
                    <p class="text-xs text-slate-400 mt-1">${a.source || ''} · ${a.date || 'Recent'}</p>
                </div>
            </a>
        `).join('');
        list.classList.remove('hidden');
    } catch {
        if (loading) { loading.classList.remove('hidden'); loading.textContent = 'Unable to load news. Try again later.'; }
    }
}

// ── Guides — Calculators ──────────────────────────────────────
function calcStampDuty() {
    const price   = parseFloat(document.getElementById('calc-bsd-price')?.value) || 0;
    const profile = document.getElementById('calc-buyer-profile')?.value || 'sc_first';
    if (!price) return;

    // BSD: progressive rates
    let bsd = 0;
    const bsdBands = [[180000,0.01],[180000,0.02],[640000,0.03],[500000,0.04],[1500000,0.05],[Infinity,0.06]];
    let rem = price;
    for (const [band, rate] of bsdBands) {
        const chunk = Math.min(rem, band);
        bsd += chunk * rate;
        rem -= chunk;
        if (rem <= 0) break;
    }

    // ABSD rates (2023 revised)
    const absdRates = {
        sc_first:0, sc_second:0.20, sc_third:0.30,
        pr_first:0.05, pr_second:0.30,
        foreigner:0.60, entity:0.65
    };
    const absd = price * (absdRates[profile] || 0);

    const fmt = v => 'S$' + Math.round(v).toLocaleString();
    document.getElementById('bsd-amount').textContent   = fmt(bsd);
    document.getElementById('absd-amount').textContent  = fmt(absd);
    document.getElementById('total-stamp').textContent  = fmt(bsd + absd);
    document.getElementById('stamp-duty-result').classList.remove('hidden');
}

function calcLoan() {
    const P = parseFloat(document.getElementById('calc-loan-amt')?.value)    || 0;
    const r = parseFloat(document.getElementById('calc-loan-rate')?.value)   / 100 / 12 || 0;
    const n = parseFloat(document.getElementById('calc-loan-tenure')?.value) * 12 || 0;
    if (!P || !r || !n) return;
    const monthly  = P * r * Math.pow(1+r,n) / (Math.pow(1+r,n)-1);
    const totalPay = monthly * n;
    const totalInt = totalPay - P;
    const fmt = v => 'S$' + Math.round(v).toLocaleString();
    document.getElementById('monthly-repayment').textContent = fmt(monthly);
    document.getElementById('total-interest').textContent    = fmt(totalInt);
    document.getElementById('total-payment').textContent     = fmt(totalPay);
    document.getElementById('loan-result').classList.remove('hidden');
}

function calcAffordability() {
    const income = parseFloat(document.getElementById('calc-income')?.value)  || 0;
    const debts  = parseFloat(document.getElementById('calc-debts')?.value)   || 0;
    const cpf    = parseFloat(document.getElementById('calc-cpf')?.value)     || 0;
    const cash   = parseFloat(document.getElementById('calc-cash')?.value)    || 0;
    if (!income) return;
    const maxMonthly = income * 0.30 - debts;
    if (maxMonthly <= 0) {
        document.getElementById('max-monthly').textContent = 'Exceeds MSR';
        document.getElementById('max-loan').textContent    = 'S$0';
        document.getElementById('max-price').textContent   = 'S$0';
        document.getElementById('affordability-result').classList.remove('hidden');
        return;
    }
    // Approx max loan at 2.6% over 25 years
    const r = 0.026/12, n = 25*12;
    const maxLoan  = maxMonthly * (Math.pow(1+r,n)-1) / (r * Math.pow(1+r,n));
    const maxPrice = maxLoan + cpf + cash;
    const fmt = v => 'S$' + Math.round(v).toLocaleString();
    document.getElementById('max-monthly').textContent  = fmt(maxMonthly);
    document.getElementById('max-loan').textContent     = fmt(maxLoan);
    document.getElementById('max-price').textContent    = fmt(maxPrice);
    document.getElementById('affordability-result').classList.remove('hidden');
}

// ── Guides — Agent Listings ───────────────────────────────────
async function loadAgents() {
    try {
        const res    = await fetch('/api/agents');
        const data   = await res.json();
        const agents = data.agents || [];
        const list   = document.getElementById('agents-list');
        const empty  = document.getElementById('agents-empty');
        const loading = document.getElementById('agents-loading');
        if (loading) loading.classList.add('hidden');
        if (!agents.length) { if (empty) empty.classList.remove('hidden'); return; }
        if (list) {
            list.classList.remove('hidden');
            list.innerHTML = agents.map(a => `
                <div class="snap-start shrink-0 w-56 bg-white dark:bg-slate-800 p-6 rounded-3xl border border-slate-100 dark:border-slate-700 card-shadow flex flex-col items-center text-center gap-3">
                    <div class="w-16 h-16 bg-gradient-to-br from-blue-400 to-violet-500 rounded-full flex items-center justify-center text-2xl font-black text-white">
                        ${(a.full_name || 'A')[0].toUpperCase()}
                    </div>
                    <div>
                        <p class="font-bold text-slate-900 dark:text-white text-sm">${a.full_name || ''}</p>
                        <p class="text-xs text-slate-400 mt-0.5">${a.cea_number ? 'CEA: ' + a.cea_number : 'Property Agent'}</p>
                        ${a.bio ? `<p class="text-xs text-slate-500 dark:text-slate-400 mt-1 line-clamp-2">${a.bio}</p>` : ''}
                    </div>
                    ${a.whatsapp ? `<a href="https://wa.me/${a.whatsapp.replace(/[^0-9]/g,'')}" target="_blank" class="w-full py-2.5 bg-emerald-500 hover:bg-emerald-600 text-white rounded-2xl text-xs font-bold transition-colors flex items-center justify-center gap-1.5">
                        <svg xmlns="http://www.w3.org/2000/svg" class="w-3.5 h-3.5" viewBox="0 0 24 24" fill="currentColor"><path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.67.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.57-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347z"/><path d="M11.5 0C5.149 0 0 5.149 0 11.5c0 2.107.573 4.077 1.567 5.765L0 23l5.898-1.543A11.454 11.454 0 0011.5 23C17.851 23 23 17.851 23 11.5S17.851 0 11.5 0zm0 21.1a9.593 9.593 0 01-4.9-1.345l-.351-.208-3.624.949.967-3.533-.228-.363A9.564 9.564 0 011.9 11.5C1.9 6.198 6.198 1.9 11.5 1.9S21.1 6.198 21.1 11.5 16.802 21.1 11.5 21.1z"/></svg>
                        WhatsApp
                    </a>` : `<p class="text-xs text-slate-400">No contact listed</p>`}
                </div>
            `).join('');
        }
    } catch { /* silent */ }
}

// ── Recently Searched ─────────────────────────────────────────
const _RECENT_KEY = 'propai_recent_searches';
const _RECENT_MAX = 5;

function saveRecentSearch(entry) {
    let items = getRecentSearches();
    // Remove duplicate same postal
    items = items.filter(i => i.postal !== entry.postal);
    items.unshift(entry);
    if (items.length > _RECENT_MAX) items = items.slice(0, _RECENT_MAX);
    localStorage.setItem(_RECENT_KEY, JSON.stringify(items));
    renderRecentSearches();
}

function getRecentSearches() {
    try { return JSON.parse(localStorage.getItem(_RECENT_KEY)) || []; }
    catch { return []; }
}

function clearRecentSearches() {
    localStorage.removeItem(_RECENT_KEY);
    renderRecentSearches();
}

function renderRecentSearches() {
    const section = document.getElementById('recent-searches-section');
    const list    = document.getElementById('recent-searches-list');
    const items   = getRecentSearches();
    if (!section || !list) return;
    if (!items.length) { section.classList.add('hidden'); return; }
    section.classList.remove('hidden');
    list.innerHTML = items.map(i => `
        <button onclick="restoreRecentSearch('${i.postal}')"
            class="w-full flex items-center justify-between px-5 py-4 bg-slate-50 dark:bg-slate-700/50 rounded-2xl hover:bg-blue-50 dark:hover:bg-blue-900/20 transition-colors text-left group">
            <div class="flex items-center gap-4">
                <div class="w-10 h-10 bg-white dark:bg-slate-600 rounded-xl flex items-center justify-center shrink-0 shadow-sm">
                    <span class="text-lg">${i.property_type === 'HDB' ? '🏢' : '🏙️'}</span>
                </div>
                <div>
                    <p class="font-bold text-slate-900 dark:text-white text-sm">${i.address || i.postal}</p>
                    <p class="text-xs text-slate-400">${i.property_type} · ${i.estimate ? 'S$' + parseInt(i.estimate).toLocaleString() : 'searched'} · ${i.date || ''}</p>
                </div>
            </div>
            <svg class="w-4 h-4 text-slate-300 group-hover:text-blue-500 transition-colors shrink-0" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="m9 18 6-6-6-6"/></svg>
        </button>
    `).join('');
}

function restoreRecentSearch(postal) {
    const postalInput = document.getElementById('input-postal');
    if (postalInput) {
        postalInput.value = postal;
        handlePostalSearch();
        showView('predict');
    }
}

// ── Lease Decay Chart ─────────────────────────────────────────
let _leaseDecayChart = null;

function renderLeaseDecayChart(estimatedValue, remainingLease, propertyType) {
    const section = document.getElementById('lease-decay-section');
    const canvas  = document.getElementById('lease-decay-chart');
    if (!section || !canvas) return;

    // Only show for leasehold (HDB or 99-yr condo)
    if (!remainingLease || remainingLease <= 0 || propertyType === 'Freehold') {
        section.classList.add('hidden');
        return;
    }
    section.classList.remove('hidden');

    // Build projection: every 5 years, estimate value decay
    const years   = [];
    const values  = [];
    const maxYrs  = Math.min(remainingLease, 60);

    for (let t = 0; t <= maxYrs; t += 5) {
        years.push(`${new Date().getFullYear() + t}`);
        const lr = remainingLease - t;
        // Lease decay model: minimal decay above 60yr, accelerating below
        let factor;
        if (lr >= 60)      factor = 1 - (remainingLease - lr) * 0.002;
        else if (lr >= 30) factor = (0.88 - (60 - lr) * 0.008);
        else               factor = Math.max(0.1, 0.64 - (30 - lr) * 0.018);
        values.push(Math.round(estimatedValue * factor));
    }

    if (_leaseDecayChart) _leaseDecayChart.destroy();
    const ctx = canvas.getContext('2d');
    const isDark = document.documentElement.classList.contains('dark');
    _leaseDecayChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: years,
            datasets: [{
                label: 'Estimated Value (S$)',
                data: values,
                borderColor: '#f59e0b',
                backgroundColor: 'rgba(245,158,11,0.1)',
                borderWidth: 2.5,
                pointBackgroundColor: '#f59e0b',
                pointRadius: 4,
                fill: true,
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        label: ctx => ' S$' + ctx.parsed.y.toLocaleString()
                    }
                }
            },
            scales: {
                x: { grid: { color: isDark ? 'rgba(255,255,255,0.05)' : 'rgba(0,0,0,0.05)' },
                     ticks: { color: isDark ? '#94a3b8' : '#64748b', font: { size: 11 } } },
                y: { grid: { color: isDark ? 'rgba(255,255,255,0.05)' : 'rgba(0,0,0,0.05)' },
                     ticks: { color: isDark ? '#94a3b8' : '#64748b', font: { size: 11 },
                              callback: v => 'S$' + (v/1000).toFixed(0) + 'k' } }
            }
        }
    });
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
            setTimeout(() => closeFeedbackModal(), 2000);
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


// ── 12-Month Price Forecast Chart ────────────────────────────────────────────
let _forecastChart = null;
function renderForecastChart(currentPrice, forecast) {
    const section = document.getElementById('forecast-section');
    const canvas  = document.getElementById('forecast-chart');
    if (!section || !canvas) return;

    section.classList.remove('hidden');
    const ctx = canvas.getContext('2d');
    if (_forecastChart) { _forecastChart.destroy(); _forecastChart = null; }

    const labels = [new Date().toLocaleDateString('en-SG', { month: 'short', year: '2-digit' }),
                    ...forecast.map(f => f.month)];
    const prices = [currentPrice, ...forecast.map(f => f.price)];
    const isDark = document.documentElement.classList.contains('dark');

    const grad = ctx.createLinearGradient(0, 0, 0, 220);
    grad.addColorStop(0, 'rgba(16,185,129,0.25)');
    grad.addColorStop(1, 'rgba(16,185,129,0.00)');

    _forecastChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels,
            datasets: [{
                label: 'Projected Price',
                data: prices,
                borderColor: '#10b981',
                backgroundColor: grad,
                borderWidth: 2.5,
                pointBackgroundColor: '#10b981',
                pointRadius: (ctx) => ctx.dataIndex === 0 ? 6 : 3,
                pointHoverRadius: 6,
                fill: true,
                tension: 0.35,
            }]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        label: (ctx) => `  S$${ctx.parsed.y.toLocaleString()}`,
                        title: (items) => items[0].dataIndex === 0 ? 'Today (Estimate)' : items[0].label,
                    },
                    backgroundColor: '#0f172a', padding: 12, cornerRadius: 10,
                    titleFont: { size: 11 }, bodyFont: { size: 13, weight: 'bold' },
                }
            },
            scales: {
                x: { grid: { display: false }, ticks: { font: { size: 10 }, color: isDark ? '#94a3b8' : '#64748b' } },
                y: {
                    grid: { color: isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.05)', drawBorder: false },
                    ticks: { callback: v => `S$${(v/1000).toFixed(0)}k`, font: { size: 10 }, color: isDark ? '#94a3b8' : '#64748b' },
                }
            }
        }
    });
}

// ── Password strength validator ───────────────────────────────────────────────
function _validatePassword(pw) {
    if (!pw || pw.length < 8)          return 'Password must be at least 8 characters.';
    if (!/[A-Z]/.test(pw))             return 'Password must contain at least one uppercase letter.';
    if (!/[a-z]/.test(pw))             return 'Password must contain at least one lowercase letter.';
    if (!/\d/.test(pw))                return 'Password must contain at least one number.';
    if (!/[!@#$%^&*()_+\-=\[\]{};':"\\|,.<>/?]/.test(pw))
        return 'Password must include a special character (!@#$%^&* etc.).';
    return null;
}

// ── 2FA: post-login verification modal ───────────────────────────────────────

async function submitVerify2FA() {
    const code   = document.getElementById('twofa-verify-code').value.trim();
    const remember = document.getElementById('twofa-remember-device').checked;
    const errEl  = document.getElementById('twofa-verify-error');

    if (code.length !== 6) {
        errEl.textContent = 'Please enter a 6-digit code.';
        errEl.classList.remove('hidden');
        return;
    }
    errEl.classList.add('hidden');

    try {
        const res  = await fetch('/api/2fa/verify', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ temp_token: window._2faTempToken, code, remember_device: remember }),
            credentials: 'include',
        });
        const data = await res.json();

        if (data.error) {
            errEl.textContent = data.error;
            errEl.classList.remove('hidden');
            return;
        }

        // Success — close modal, complete login
        const modal = document.getElementById('twofa-verify-modal');
        modal.classList.add('hidden');
        modal.classList.remove('flex');
        window._2faTempToken = null;

        currentUser = data.user;
        saveCurrentUser();
        updateAuthUI();
        loadProfileForm();
        showView('home');
    } catch (err) {
        errEl.textContent = 'Network error — please try again.';
        errEl.classList.remove('hidden');
    }
}

function cancelVerify2FA() {
    const modal = document.getElementById('twofa-verify-modal');
    modal.classList.add('hidden');
    modal.classList.remove('flex');
    window._2faTempToken = null;
}

// ── 2FA: Settings > Security tab ─────────────────────────────────────────────

function load2FAStatus() {
    if (!currentUser) return;
    const enabled = currentUser.totp_enabled;
    const icon   = document.getElementById('twofa-status-icon');
    const title  = document.getElementById('twofa-status-title');
    const desc   = document.getElementById('twofa-status-desc');
    const btnEn  = document.getElementById('twofa-enable-btn');
    const btnDis = document.getElementById('twofa-disable-btn');

    if (enabled) {
        icon.className  = 'w-12 h-12 rounded-full bg-emerald-100 dark:bg-emerald-900/40 flex items-center justify-center';
        icon.innerHTML  = '<i data-lucide="shield-check" class="w-6 h-6 text-emerald-600"></i>';
        title.textContent = '2FA Enabled';
        title.className   = 'font-bold text-emerald-700 dark:text-emerald-400';
        desc.textContent  = 'Your account is secured with an authenticator app.';
        btnEn.classList.add('hidden');
        btnDis.classList.remove('hidden');
    } else {
        icon.className  = 'w-12 h-12 rounded-full bg-slate-200 dark:bg-slate-700 flex items-center justify-center';
        icon.innerHTML  = '<i data-lucide="shield" class="w-6 h-6 text-slate-400"></i>';
        title.textContent = '2FA Not Enabled';
        title.className   = 'font-bold text-slate-900 dark:text-white';
        desc.textContent  = 'Your account is protected by password only.';
        btnEn.classList.remove('hidden');
        btnDis.classList.add('hidden');
    }
    lucide.createIcons();
}

async function initSetup2FA() {
    if (!currentUser) return;
    const panel = document.getElementById('twofa-setup-panel');
    panel.classList.remove('hidden');
    document.getElementById('twofa-setup-code').value = '';
    document.getElementById('twofa-setup-error').classList.add('hidden');
    document.getElementById('twofa-qr-canvas').innerHTML = '';
    document.getElementById('twofa-manual-secret').textContent = 'Loading…';
    document.getElementById('twofa-enable-btn').classList.add('hidden');

    try {
        const res  = await fetch('/api/2fa/setup', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ user_id: currentUser.id }),
        });
        const data = await res.json();
        if (data.error) { alert(data.error); return; }

        document.getElementById('twofa-manual-secret').textContent = data.secret;

        // Render QR code
        const canvas = document.getElementById('twofa-qr-canvas');
        canvas.innerHTML = '';
        if (typeof QRCode !== 'undefined') {
            new QRCode(canvas, { text: data.uri, width: 180, height: 180, correctLevel: QRCode.CorrectLevel.M });
        } else {
            canvas.textContent = 'QR library not loaded — use manual code above.';
        }
        setTimeout(() => document.getElementById('twofa-setup-code').focus(), 300);
    } catch (err) {
        alert('Failed to start 2FA setup. Please try again.');
        cancelSetup2FA();
    }
}

function cancelSetup2FA() {
    document.getElementById('twofa-setup-panel').classList.add('hidden');
    document.getElementById('twofa-enable-btn').classList.remove('hidden');
}

async function enable2FA() {
    const code  = document.getElementById('twofa-setup-code').value.trim();
    const errEl = document.getElementById('twofa-setup-error');
    if (code.length !== 6) {
        errEl.textContent = 'Please enter a 6-digit code.';
        errEl.classList.remove('hidden');
        return;
    }
    errEl.classList.add('hidden');

    const res  = await fetch('/api/2fa/enable', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ user_id: currentUser.id, code }),
    });
    const data = await res.json();

    if (data.error) {
        errEl.textContent = data.error;
        errEl.classList.remove('hidden');
        return;
    }

    // Update local user state
    currentUser.totp_enabled = true;
    saveCurrentUser();
    cancelSetup2FA();
    load2FAStatus();
    showToast('2FA enabled successfully!');
}

function openDisable2FAModal() {
    document.getElementById('twofa-disable-code').value = '';
    document.getElementById('twofa-disable-error').classList.add('hidden');
    const modal = document.getElementById('twofa-disable-modal');
    modal.classList.remove('hidden');
    modal.classList.add('flex');
    setTimeout(() => document.getElementById('twofa-disable-code').focus(), 100);
}

function closeDisable2FAModal() {
    const modal = document.getElementById('twofa-disable-modal');
    modal.classList.add('hidden');
    modal.classList.remove('flex');
}

async function confirmDisable2FA() {
    const code  = document.getElementById('twofa-disable-code').value.trim();
    const errEl = document.getElementById('twofa-disable-error');
    if (code.length !== 6) {
        errEl.textContent = 'Please enter a 6-digit code.';
        errEl.classList.remove('hidden');
        return;
    }
    errEl.classList.add('hidden');

    const res  = await fetch('/api/2fa/disable', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ user_id: currentUser.id, code }),
        credentials: 'include',
    });
    const data = await res.json();

    if (data.error) {
        errEl.textContent = data.error;
        errEl.classList.remove('hidden');
        return;
    }

    currentUser.totp_enabled = false;
    saveCurrentUser();
    closeDisable2FAModal();
    load2FAStatus();
    showToast('2FA has been disabled.');
}

// Helper toast (reuse if already defined, else simple alert fallback)
function showToast(msg) {
    const existing = document.getElementById('_toast_notify');
    if (existing) existing.remove();
    const t = document.createElement('div');
    t.id = '_toast_notify';
    t.className = 'fixed bottom-8 left-1/2 -translate-x-1/2 z-[99999] px-6 py-3 bg-slate-900 text-white text-sm font-bold rounded-2xl shadow-xl transition-all';
    t.textContent = msg;
    document.body.appendChild(t);
    setTimeout(() => t.remove(), 3000);
}

