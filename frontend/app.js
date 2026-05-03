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
        showAuthWall('setting');
        return;
    }

    if ((viewId === 'predict' || viewId === 'trend') && !currentUser) {
        showAuthWall(viewId);
        return;
    }

    document.querySelectorAll('.view-section').forEach(s => s.classList.remove('active'));
    target.classList.add('active');

    document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
    const activeLink = document.getElementById('nav-' + viewId);
    if (activeLink) activeLink.classList.add('active');

    if (viewId === 'trend') {
        setTimeout(() => { loadMarketWatch(); initTrendChart(); renderTrendNews(currentNeighbourhood); loadMopLeads(); loadGapAnalysis(); }, 100);
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

// ── Auth wall — shown when a guest tries to access a gated view ───────────────
function showAuthWall(intendedView) {
    const modal = document.getElementById('auth-wall-modal');
    if (!modal) { showView('signin'); return; }
    const labels = {
        predict: { icon: 'brain-circuit', title: 'AI Valuation is members-only', desc: 'Get instant property valuations, XAI explanations, and Price Intelligence scenarios.' },
        trend:   { icon: 'trending-up',   title: 'Market Insights is members-only', desc: 'Access live PSF trends, neighbourhood analytics, and MOP opportunity leads.' },
        setting: { icon: 'settings',      title: 'Sign in to manage your account', desc: 'Update your profile, notifications, and security preferences.' },
    };
    const cfg = labels[intendedView] || { icon: 'lock', title: 'Sign in to continue', desc: '' };
    const iconEl  = modal.querySelector('#aw-icon');
    const titleEl = modal.querySelector('#aw-title');
    const descEl  = modal.querySelector('#aw-desc');
    if (iconEl)  iconEl.setAttribute('data-lucide', cfg.icon);
    if (titleEl) titleEl.textContent = cfg.title;
    if (descEl)  descEl.textContent  = cfg.desc;
    modal.dataset.intendedView = intendedView;
    modal.classList.remove('hidden');
    lucide.createIcons();
}
function closeAuthWall() {
    document.getElementById('auth-wall-modal')?.classList.add('hidden');
}
function authWallSignIn() {
    closeAuthWall();
    showView('signin');
}
function authWallRegister() {
    closeAuthWall();
    showView('register');
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
    _availableAreas     = [];
    _cachedStoreyRanges = [];   // clear stale ranges so new flat_type doesn't inherit old floors
    _cachedMaxFloor     = null;
    _loadFlatSpecs();
}

// Flat type mapping (bedrooms → HDB flat type)
const _BEDS_TO_FLAT_TYPE = {
    1: '1 ROOM', 2: '2 ROOM', 3: '3 ROOM', 4: '4 ROOM', 5: '5 ROOM', 6: 'EXECUTIVE',
};

function setManualFloor(val, btn) {
    const el = document.getElementById('input-floor-manual');
    if (el) el.value = val;
    document.querySelectorAll('.floor-quick-btn').forEach(b => {
        b.classList.remove('border-blue-500','bg-blue-50','text-blue-700');
        b.classList.add('border-slate-200','bg-slate-50','text-slate-600');
    });
    if (btn) {
        btn.classList.add('border-blue-500','bg-blue-50','text-blue-700');
        btn.classList.remove('border-slate-200','bg-slate-50','text-slate-600');
    }
}

function adjustManualFloor(delta) {
    const el = document.getElementById('input-floor-manual');
    if (!el) return;
    el.value = Math.max(1, Math.min(99, (parseInt(el.value) || 10) + delta));
}

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
        urlExtra = '';   // condo: no bedroom filter — floor area comes from project/district URA data
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
        // Always prefer server response; only fall back to cache when server returned nothing
        // AND the cache was seeded from the same context (same flat_type search)
        const storeyRanges = (data.storey_ranges && data.storey_ranges.length)
                             ? data.storey_ranges
                             : _cachedStoreyRanges;   // cache was cleared on flat_type change so this is safe
        const defaultRange = data.default_storey_range || null;

        // Derive max floor: prefer explicit value, fall back to parsing "XX TO XX" strings
        let maxFloor = data.max_floor || _cachedMaxFloor || 0;
        if (!maxFloor && storeyRanges.length) {
            maxFloor = Math.max(...storeyRanges.map(s => {
                const top = s.split(/\s+TO\s+/i).pop();
                return parseInt(top) || 0;
            }));
        }
        if (!maxFloor) maxFloor = 20;

        // Keep _cachedMaxFloor in sync — property-areas may have a better value
        if (maxFloor > (_cachedMaxFloor || 0)) _cachedMaxFloor = maxFloor;
        // Track highest transacted floor for confidence penalty
        window._maxTransactedFloor = data.max_transacted_floor || null;

        // ── Floor range dropdown (HDB only) ──────────────────────
        if (isHdb) {
            const floorDataSource = data.floor_data_source || 'block';
            const hasBlockData = floorDataSource === 'block';
            const sel  = document.getElementById('input-floor-range');
            const wrap = document.getElementById('floor-manual-wrap');
            window._floorDataSource = floorDataSource;
            if (!hasBlockData) {
                if (sel)  sel.classList.add('hidden');
                if (wrap) wrap.classList.remove('hidden');
                // Show town-wide max as a reference hint
                const townMax = data.town_max_floor;
                const hintEl  = wrap ? wrap.querySelector('p.text-xs') : null;
                if (hintEl && townMax) {
                    hintEl.textContent = `No floor data for this block. Highest floor seen in this area: ${townMax}. Which level is the unit on?`;
                }
                // Update the Top quick-select button label with town max
                const topBtn = wrap ? wrap.querySelector('.floor-quick-btn:last-of-type') : null;
                if (topBtn && townMax) {
                    topBtn.innerHTML = `Top<br><span class="font-normal text-slate-400">31–${townMax}</span>`;
                    topBtn.onclick = () => setManualFloor(Math.round(townMax * 0.85), topBtn);
                }
            } else {
                if (sel)  sel.classList.remove('hidden');
                if (wrap) wrap.classList.add('hidden');
                _populateFloorRanges(storeyRanges, maxFloor, defaultRange);
            }
        }

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
        const postalLoadingEl = document.getElementById('postal-loading');
        placeholder.classList.add('hidden');
        details.classList.add('hidden');
        if (landedBanner) landedBanner.classList.add('hidden');
        if (postalLoadingEl) postalLoadingEl.classList.remove('hidden');

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

                // Valid HDB or Condo — hide skeleton, show details
                if (postalLoadingEl) postalLoadingEl.classList.add('hidden');
                if (landedEl)  landedEl.classList.add('hidden');
                if (detailsEl) detailsEl.classList.remove('hidden');

                // Save to recent searches on property lookup (no estimate yet)
                saveRecentSearch({
                    postal,
                    address: document.getElementById('display-address')?.innerText || postal,
                    property_type: info.property_type || 'HDB',
                    estimate: null,
                    date: new Date().toLocaleDateString('en-SG', { day: 'numeric', month: 'short', year: 'numeric' })
                });

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
                // Cache lat/lon for amenity future-proofing
                window._predictLat = info.lat || null;
                window._predictLon = info.lon || null;
                // Cache remaining lease years from DB for use in the chart
                if (info.remaining_lease_years != null) {
                    window._cachedRemainingLease = info.remaining_lease_years;
                } else {
                    window._cachedRemainingLease = null;
                }
                // Apply floor data from DB lookup — derive max from storey_range strings
                // if the server didn't send an explicit max_floor value
                const _parseMaxFromRanges = (ranges) => {
                    const tops = ranges.map(s => {
                        const top = s.split(/\s+TO\s+/i).pop();
                        return parseInt(top) || 0;
                    }).filter(n => n > 0);
                    return tops.length ? Math.max(...tops) : 0;
                };
                if (info.storey_ranges && info.storey_ranges.length) {
                    _cachedStoreyRanges = info.storey_ranges;
                    _cachedMaxFloor     = (info.max_floor && info.max_floor > 0)
                        ? info.max_floor
                        : (_parseMaxFromRanges(info.storey_ranges) || 20);
                } else if (info.max_floor && info.max_floor > 0) {
                    _cachedMaxFloor     = info.max_floor;
                    _cachedStoreyRanges = [];
                } else {
                    _cachedStoreyRanges = [];
                    _cachedMaxFloor     = null;
                }
                // Show/hide correct spec section, then apply cached floor data
                _onPropertyTypeChange();
            })
            .catch(() => {
                if (postalLoadingEl) postalLoadingEl.classList.add('hidden');
                if (placeholder) placeholder.classList.remove('hidden');
                openPropertyNotFoundModal(postal);
            });
    } catch {
        if (postalLoadingEl) postalLoadingEl.classList.add('hidden');
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

    // Show skeleton immediately
    togglePredictView('output');
    const loadingEl  = document.getElementById('predict-loading');
    const resultsEl  = document.getElementById('predict-results');
    const loadingMsg = document.getElementById('predict-loading-msg');
    if (loadingEl)  loadingEl.classList.remove('hidden');
    if (resultsEl)  resultsEl.classList.add('hidden');

    const steps = ['Analysing property…', 'Running AI model…', 'Generating insights…'];
    let stepIdx = 0;
    const stepTimer = setInterval(() => {
        stepIdx = Math.min(stepIdx + 1, steps.length - 1);
        if (loadingMsg) loadingMsg.textContent = steps[stepIdx];
    }, 1800);

    // Reset dynamic sections so stale charts from prior search never linger
    ['forecast-section','shap-section','whatif-section','safe-buy-section','amenity-future-section'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.classList.add('hidden');
    });
    if (_forecastChart) { _forecastChart.destroy(); _forecastChart = null; }
    if (_shapChart)     { _shapChart.destroy();     _shapChart     = null; }
    if (_leaseDecayChart) { _leaseDecayChart.destroy(); _leaseDecayChart = null; }
    
    const postal   = document.getElementById('input-postal').value;
    lastMapPostal  = postal;
    const area     = getAreaValue();
    const propType = document.getElementById('input-property-type')?.value || 'HDB';
    const isHdb    = propType === 'HDB';

    let floor, flatType, floorIsManual = false;
    if (isHdb) {
        const manualEl = document.getElementById('input-floor-manual');
        const wrapEl   = document.getElementById('floor-manual-wrap');
        const useManual = wrapEl && !wrapEl.classList.contains('hidden');
        if (useManual) {
            floor = parseInt(manualEl.value || '10');
            floorIsManual = true;
        } else {
            const floorRangeSel = document.getElementById('input-floor-range');
            const rangeVal = floorRangeSel ? floorRangeSel.value : '10';
            floor = _storeyRangeMidpoint(rangeVal);
        }
        flatType = document.getElementById('input-flat-type')?.value || '4 ROOM';
    } else {
        floor    = parseInt(document.getElementById('range-floor')?.value || 10);
        flatType = null;
    }

    try {
        const body = { postal, area, floor, property_type: propType, town: _predictTown };
        if (flatType) body.flat_type = flatType;
        if (window._cachedRemainingLease != null) body.remaining_lease_years = window._cachedRemainingLease;
        if (_cachedMaxFloor)              body.max_floor            = _cachedMaxFloor;
        if (_predictBlock)                body.block                = _predictBlock;
        if (_predictRoad)                 body.street_name          = _predictRoad;
        if (_predictProject)              body.project              = _predictProject;
        if (window._maxTransactedFloor)   body.max_transacted_floor = window._maxTransactedFloor;
        const response = await fetch('/api/predict', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });
        const data = await response.json();

        document.getElementById('output-price').innerText = `S$${data.estimated_value.toLocaleString()}`;
        const disclaimerEl = document.getElementById('output-floor-disclaimer');
        if (disclaimerEl) {
            const showDisclaimer = floorIsManual || data.floor_extrapolated;
            disclaimerEl.classList.toggle('hidden', !showDisclaimer);
            if (data.floor_extrapolated && !floorIsManual) {
                const maxTf = window._maxTransactedFloor;
                disclaimerEl.querySelector('span:last-child').textContent =
                    `No resale transactions found at this floor level in this estate (highest on record: floor ${maxTf}). ` +
                    `Price is estimated using floor premium modelling — treat as a directional guide.`;
            }
        }
        document.getElementById('output-confidence').innerText = `${data.confidence}%`;

        const mape = data.mape || (propType === 'HDB' ? 7.0 : 10.0);
        const priceLo = data.min_value || Math.round(data.estimated_value * (1 - mape / 100) / 1000) * 1000;
        const priceHi = data.max_value || Math.round(data.estimated_value * (1 + mape / 100) / 1000) * 1000;
        const rangeEl = document.getElementById('output-price-range');
        if (rangeEl) rangeEl.innerText = `Estimated range: S$${priceLo.toLocaleString()} – S$${priceHi.toLocaleString()} (±${mape.toFixed(1)}% model MAPE)`;

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
            const isSell    = _userIntent === 'sell';
            const intentInsight = isSell
                ? rawInsight.replace('values this', 'values your')
                : rawInsight;
            const intentRec = isSell
                ? rawRec
                    .replace("We'd expect this unit to transact", "We'd price this unit")
                    .replace('It\'s always worth checking', 'Before listing, check')
                    .replace('to fine-tune your expectations', 'to make sure your asking price is competitive')
                : rawRec;
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

        // Hide skeleton, show results
        clearInterval(stepTimer);
        if (loadingEl) loadingEl.classList.add('hidden');
        if (resultsEl) resultsEl.classList.remove('hidden');

        // Fire non-blocking secondary calls in parallel
        renderPredictNews(postal, _predictTown);
        if (isHdb && _predictBlock && _predictTown) {
            fetchFloorComps(_predictBlock, _predictTown, flatType || '4 ROOM', _predictRoad);
        }

        // Lease decay chart — prefer property-lookup remaining lease, fall back to model median
        const leaseType  = document.getElementById('input-lease-type')?.value || '';
        const isFreehold = leaseType.toLowerCase().includes('freehold');
        const remLease   = isFreehold ? 0
            : (window._cachedRemainingLease != null ? window._cachedRemainingLease
                : (data.remaining_lease_years || 70));
        renderLeaseDecayChart(data.estimated_value, remLease, propType);

        // Safe-Buy Buffer Predictor
        const segment = data.location
            ? (data.location.includes('Core Central') ? 'CCR'
               : data.location.includes('Rest of Central') ? 'RCR' : 'OCR')
            : 'OCR';
        renderSafeBuyPanel(data.estimated_value, area, propType, segment, isFreehold);

        // Amenity Future-Proofing
        renderAmenityFuture(window._predictLat, window._predictLon, data.estimated_value);

        // 12-month price forecast chart
        if (data.price_forecast && data.price_forecast.length) {
            renderForecastChart(data.estimated_value, data.price_forecast);
        }

        // SHAP feature contributions chart
        if (data.shap_contributions && data.shap_contributions.length) {
            renderShapChart(data.shap_contributions);
        } else {
            document.getElementById('shap-section').classList.add('hidden');
        }

        // What-if sliders — initialise with current prediction values
        // Use _cachedMaxFloor (kept in sync by _loadFlatSpecs); fall back to
        // manual floor value so the slider at least starts at the right level
        const wiMaxFloor = _cachedMaxFloor
            || (floorIsManual ? Math.max((body.floor || 10) + 10, 30) : null)
            || (isHdb ? 50 : 50);
        initWhatIfSliders({
            floor:      body.floor || 10,
            lease:      data.remaining_lease_years || window._cachedRemainingLease || 65,
            basePrice:  data.estimated_value,
            isCondo:    propType.toLowerCase().includes('condo') || propType.toLowerCase().includes('private'),
            isFreehold: isFreehold,
            maxFloor:   wiMaxFloor,
        });

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
        clearInterval(stepTimer);
        if (loadingEl) loadingEl.classList.add('hidden');
        if (resultsEl) resultsEl.classList.remove('hidden');
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
    document.getElementById('output-confidence').innerText = "—";
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
    } else if (tabId === 'auditlog') {
        loadAuditLog();
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

    // Map tab is explore-only — prediction is done from the Predict tab
    const predictBtn = document.getElementById('map-predict-btn');
    if (predictBtn) predictBtn.classList.add('hidden');

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

    // Building name for condos/named developments
    const buildingName = (propInfo?.building_name && propInfo.building_name !== 'NIL')
        ? propInfo.building_name : '';

    const nameHtml = buildingName
        ? `<p style="font-weight:700;font-size:13px;color:#0f172a;margin:0 0 2px;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${buildingName}</p>`
        : '';

    const exploreBtn = `<button onclick="(function(){document.querySelector('.leaflet-popup-close-button')&&document.querySelector('.leaflet-popup-close-button').click();loadAmenities(${lat},${lng},'${postal||''}');})()" style="background:linear-gradient(135deg,#f97316,#ea580c);color:white;border:none;padding:7px 18px;border-radius:10px;font-size:12px;font-weight:700;cursor:pointer;width:100%">🔍 Explore Amenities</button>`;

    const popupHtml = `
        <div style="min-width:200px;font-family:inherit">
            ${nameHtml}
            <p style="font-weight:${buildingName ? '400' : '700'};font-size:${buildingName ? '11' : '13'}px;color:#0f172a;margin:0 0 1px">${postal ? postal : '—'}</p>
            <p style="font-size:11px;color:#64748b;margin:0 0 10px;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${address}">${address}</p>
            ${exploreBtn}
        </div>`;
    _draggablePin.bindPopup(popupHtml, { offset: [0, -30], closeButton: true, maxWidth: 260 }).openPopup();
    const bar = document.getElementById('pin-result-bar');
    if (bar) bar.classList.add('hidden');
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
            // Map PSF type button to HDB flat_type filter
            const _PSF_TO_FLAT = {
                hdb2: '2 ROOM', hdb3: '3 ROOM', hdb4: '4 ROOM',
                hdb5: '5 ROOM', hdb3gen: 'MULTI-GENERATION',
            };
            const apiFlatType = _PSF_TO_FLAT[_currentPsfType] || '';
            let trendUrl = apiTown ? `/api/trend?town=${encodeURIComponent(apiTown)}` : '/api/trend';
            if (apiFlatType) trendUrl += `&flat_type=${encodeURIComponent(apiFlatType)}`;
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
async function renderPredictNews(postal, town) {
    const section = document.getElementById('predict-news-section');
    const list    = document.getElementById('predict-news-list');
    const label   = document.getElementById('predict-news-area');
    if (!section || !list) return;

    section.classList.remove('hidden');
    list.innerHTML = _newsLoadingHTML();

    try {
        // Prefer town name (from geocode) over postal sector — more accurate area mapping
        const params = town
            ? `neighbourhood=${encodeURIComponent(town.charAt(0) + town.slice(1).toLowerCase())}&limit=4`
            : `postal=${encodeURIComponent(postal)}&limit=4`;
        const res = await fetch(`/api/news?${params}`);
        if (!res.ok) throw new Error();
        const data = await res.json();
        const articles = data.articles || [];
        if (label) label.innerText = `Latest news for ${data.area || town || 'this area'}`;
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

    const list   = data.similar_transactions || [];
    const filter = data.summary?.flat_type_filter;
    const latest = data.summary?.latest_month || '';

    // Update table caption / subtitle if one exists
    const caption = document.getElementById('comparable-caption');
    if (caption) {
        caption.textContent = filter
            ? `${filter.replace(/\b\w/g, c => c.toUpperCase())} transactions · last 3 months from ${latest}`
            : `All flat types · last 3 months from ${latest}`;
    }

    if (!list.length) {
        tableBody.innerHTML = `<tr><td colspan="5" class="py-8 text-center text-slate-400 text-sm">No comparable transactions found${filter ? ` for ${filter}` : ''}.</td></tr>`;
        return;
    }

    list.forEach(item => {
        const row = document.createElement('tr');
        row.className = "hover:bg-slate-50/50 dark:hover:bg-slate-700/30 transition-colors";

        const ppsf = item.floor_area > 0 ? Math.round(item.price / item.floor_area) : null;
        row.innerHTML = `
            <td class="py-4 pl-4">
                <p class="font-semibold text-slate-900 dark:text-slate-100">${item.address}</p>
                <p class="text-xs text-slate-400">${item.storey || '–'}</p>
            </td>
            <td class="py-4 text-sm font-medium text-slate-600 dark:text-slate-300">${item.type}</td>
            <td class="py-4">
                <span class="font-bold text-slate-900 dark:text-slate-100">S$${item.price.toLocaleString()}</span>
                ${ppsf ? `<p class="text-[10px] text-slate-400">S$${ppsf.toLocaleString()} psf</p>` : ''}
            </td>
            <td class="py-4 text-sm text-slate-500 dark:text-slate-400">${item.floor_area ? item.floor_area.toLocaleString() + ' sqft' : '–'}</td>
            <td class="py-4 pr-4 text-right">
                <span class="px-3 py-1 bg-slate-100 dark:bg-slate-700 rounded-lg text-[10px] font-bold text-slate-600 dark:text-slate-300">${item.date}</span>
            </td>
        `;

        tableBody.appendChild(row);
    });
}

let adminTypeChart;

function initAdminTypeChart(stats) {
    const canvas = document.getElementById('adminTypeChart');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    if (adminTypeChart) adminTypeChart.destroy();

    const setEl = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };

    // HDB vs Private breakdown from recent_predictions
    const rp = stats.recent_predictions || [];
    const hdbTypes = new Set(['1 ROOM','2 ROOM','3 ROOM','4 ROOM','5 ROOM','EXECUTIVE','MULTI-GENERATION']);
    const hdbPreds  = rp.filter(r => hdbTypes.has((r.flat_type||'').toUpperCase()) || (r.flat_type||'').toUpperCase().includes('ROOM'));
    const privPreds = rp.filter(r => !hdbTypes.has((r.flat_type||'').toUpperCase()) && !(r.flat_type||'').toUpperCase().includes('ROOM'));
    const avg     = arr => arr.length ? Math.round(arr.reduce((s,r) => s + (r.estimated_value||0), 0) / arr.length) : 0;
    const avgConf = arr => arr.length ? (arr.reduce((s,r) => s + (r.confidence||0), 0) / arr.length).toFixed(1) : '—';
    const hdbAvg = avg(hdbPreds), privAvg = avg(privPreds);
    const fmt = v => v ? `S$${v.toLocaleString()}` : '—';

    // Use server-side counts (more accurate than filtering 50 rows)
    const hdbCount  = stats.predictions_by_type?.hdb     || 0;
    const privCount = stats.predictions_by_type?.private  || 0;

    setEl('admin-hdb-avg-val', fmt(hdbAvg));
    setEl('admin-hdb-count',   hdbCount.toLocaleString());
    setEl('admin-hdb-conf',    hdbPreds.length  ? avgConf(hdbPreds)  + '%' : '—');
    setEl('admin-priv-avg-val', fmt(privAvg));
    setEl('admin-priv-count',   privCount.toLocaleString());
    setEl('admin-priv-conf',    privPreds.length ? avgConf(privPreds) + '%' : '—');

    // Top towns
    const townsEl = document.getElementById('admin-top-towns');
    if (townsEl) {
        const towns = stats.predictions_by_town || [];
        townsEl.innerHTML = towns.length
            ? towns.slice(0,8).map((t,i) => `
                <div class="flex items-center justify-between">
                    <div class="flex items-center gap-3">
                        <span class="w-5 h-5 rounded-full bg-slate-100 dark:bg-slate-700 text-slate-500 dark:text-slate-400 text-xs font-black flex items-center justify-center">${i+1}</span>
                        <span class="text-sm font-medium text-slate-700 dark:text-slate-300">${t.town||'Unknown'}</span>
                    </div>
                    <span class="text-sm font-bold text-slate-900 dark:text-white">${(t.count||0).toLocaleString()}</span>
                </div>`).join('')
            : '<p class="text-xs text-slate-400">No prediction data yet.</p>';
    }

    // Avg Estimated Value compare chart
    const compareCanvas = document.getElementById('adminCompareChart');
    if (compareCanvas) {
        if (window._adminCompareChart) window._adminCompareChart.destroy();
        if (hdbAvg || privAvg) {
            window._adminCompareChart = new Chart(compareCanvas.getContext('2d'), {
                type: 'bar',
                data: {
                    labels: ['HDB Resale', 'Private / Condo'],
                    datasets: [{
                        label: 'Avg Estimated Value (S$)',
                        data: [hdbAvg, privAvg],
                        backgroundColor: ['rgba(59,130,246,0.85)', 'rgba(139,92,246,0.85)'],
                        borderRadius: 8, barThickness: 40,
                    }]
                },
                options: {
                    responsive: true, maintainAspectRatio: false,
                    plugins: { legend: { display: false }, tooltip: { callbacks: { label: c => `  S$${c.parsed.y.toLocaleString()}` } } },
                    scales: {
                        y: { beginAtZero: true, grid: { color: 'rgba(0,0,0,0.03)' }, ticks: { callback: v => `S$${(v/1000).toFixed(0)}k`, font: { size: 10 }, color: '#94a3b8' } },
                        x: { grid: { display: false }, ticks: { font: { weight: 'bold', size: 11 }, color: '#334155' } }
                    }
                }
            });
        }
    }

    // Predictions by Type bar chart — HDB vs Private counts
    const labels     = ['HDB Resale', 'Private / Condo'];
    const dataValues = [hdbCount, privCount];

    adminTypeChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                label: 'Predictions',
                data: dataValues,
                backgroundColor: ['rgba(59,130,246,0.85)', 'rgba(139,92,246,0.85)'],
                borderRadius: 8,
                barThickness: 40,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: { callbacks: { label: c => `  ${c.parsed.y} predictions` } }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: { color: 'rgba(0,0,0,0.03)', drawBorder: false },
                    ticks: { font: { weight: 'bold' }, color: '#94A3B8', precision: 0 }
                },
                x: {
                    grid: { display: false },
                    ticks: { font: { weight: 'bold' }, color: '#94A3B8' }
                }
            }
        }
    });
}

async function loadAuditLog() {
    const tbody = document.getElementById('audit-log-body');
    const monthSel = document.getElementById('audit-month-filter');
    if (!tbody) return;

    const month = monthSel ? monthSel.value : '';

    tbody.innerHTML = '<tr><td colspan="4" class="py-8 text-center text-slate-400 text-sm animate-pulse">Loading…</td></tr>';

    try {
        const auditParams = new URLSearchParams();
        if (month) auditParams.set('month', month);
        if (currentUser?.id) auditParams.set('user_id', currentUser.id);
        const res = await fetch(`/api/admin/audit-log?${auditParams.toString()}`);
        const data = await res.json();
        const logs = data.logs || [];

        // Populate month filter options
        if (monthSel && data.months?.length) {
            const cur = monthSel.value;
            monthSel.innerHTML = '<option value="">All Time</option>' +
                data.months.map(m => `<option value="${m}" ${m===cur?'selected':''}>${m}</option>`).join('');
        }

        if (!logs.length) {
            tbody.innerHTML = '<tr><td colspan="4" class="py-8 text-center text-slate-400 text-sm">No audit events found.</td></tr>';
            return;
        }

        const eventColors = {
            'register':           'bg-emerald-100 text-emerald-700',
            'login':              'bg-blue-100 text-blue-700',
            'admin_action':       'bg-amber-100 text-amber-700',
            'upload':             'bg-violet-100 text-violet-700',
            'delete':             'bg-rose-100 text-rose-700',
            'account_deleted':    'bg-rose-100 text-rose-700',
            'model_upload':       'bg-sky-100 text-sky-700',
            'retrain':            'bg-orange-100 text-orange-700',
            'account_type_change':'bg-violet-100 text-violet-700',
            'role_change':        'bg-amber-100 text-amber-700',
            'security':           'bg-rose-100 text-rose-700',
        };

        tbody.innerHTML = logs.map(l => {
            const dt = new Date(l.logged_at || '');
            const timeStr = isNaN(dt) ? (l.logged_at||'').slice(0,16) :
                dt.toLocaleDateString('en-SG',{day:'numeric',month:'short',year:'numeric'}) + ' ' +
                dt.toLocaleTimeString('en-SG',{hour:'2-digit',minute:'2-digit'});
            const badge = eventColors[l.event_type] || 'bg-slate-100 text-slate-600';
            return `
            <tr class="hover:bg-slate-50 transition-colors">
                <td class="py-3 pl-4 text-xs text-slate-500 whitespace-nowrap">${timeStr}</td>
                <td class="py-3 text-sm font-medium text-slate-700">${l.user_name || 'System'}</td>
                <td class="py-3">
                    <span class="text-xs px-2 py-1 rounded-full font-semibold ${badge}">${(l.event_type||'').replace(/_/g,' ')}</span>
                </td>
                <td class="py-3 pr-4 text-sm text-slate-500 max-w-xs truncate">${l.action || ''}</td>
            </tr>`;
        }).join('');
    } catch (e) {
        tbody.innerHTML = `<tr><td colspan="4" class="py-8 text-center text-rose-400 text-sm">Failed to load: ${e.message}</td></tr>`;
    }
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
        tbody.innerHTML = '<tr><td colspan="4" class="py-8 text-center text-slate-400 text-sm">No users found</td></tr>';
        return;
    }

    tbody.innerHTML = users.map(user => {
        const initials  = user.full_name.split(' ').map(n => n[0]).join('').toUpperCase().slice(0, 2);
        const isAdmin   = user.role === 'admin';
        const acctType  = user.account_type || 'homeowner';
        const isAgent   = acctType === 'agent';
        const acctBadge = isAgent
            ? '<span class="px-2 py-0.5 bg-violet-100 text-violet-700 rounded text-[10px] font-bold">Agent</span>'
            : '<span class="px-2 py-0.5 bg-slate-100 text-slate-500 rounded text-[10px] font-bold">Homeowner</span>';
        const acctToggleBtn = isAgent
            ? `<button onclick="adminToggleAccountType(${user.id},'homeowner')"
                class="text-[10px] px-2 py-0.5 rounded border border-slate-200 text-slate-500 hover:border-rose-300 hover:text-rose-600 transition-colors" title="Downgrade to Homeowner">
                ↓ Downgrade
               </button>`
            : `<button onclick="adminToggleAccountType(${user.id},'agent')"
                class="text-[10px] px-2 py-0.5 rounded border border-slate-200 text-slate-500 hover:border-violet-300 hover:text-violet-600 transition-colors" title="Upgrade to Agent">
                ↑ Make Agent
               </button>`;
        return `
            <tr class="group hover:bg-slate-50 transition-colors">
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
                <td class="py-5">
                    <div class="flex items-center gap-2 flex-wrap">
                        ${acctBadge}
                        ${acctToggleBtn}
                    </div>
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

async function upgradeToAgent() {
    if (!currentUser) return;
    if (!confirm('Upgrade your account to Agent? You will gain access to MOP Leads, Gap Analysis and other agent tools.')) return;
    const res  = await fetch(`/api/profile/${currentUser.id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            full_name: currentUser.full_name,
            email: currentUser.email,
            phone: currentUser.phone || '',
            account_type: 'agent',
        }),
    });
    const data = await res.json();
    if (data.error) { showToast(data.error, true); return; }
    currentUser = { ...currentUser, ...data.user };
    saveCurrentUser();
    updateAuthUI();
    loadProfileForm();
    showToast('Account upgraded to Agent');
}

async function adminToggleAccountType(userId, newType) {
    const label = newType === 'agent' ? 'upgrade to Agent' : 'downgrade to Homeowner';
    if (!confirm(`${label.charAt(0).toUpperCase() + label.slice(1)} this user?`)) return;
    const res  = await fetch(`/api/users/${userId}/account-type`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ account_type: newType }),
    });
    const data = await res.json();
    if (data.error) { showToast(data.error, true); return; }
    showToast(`Account type updated to ${newType}`);
    loadAdminUsers();
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
        if (!response.ok) throw new Error(`Stats API ${response.status}`);
        const data = await response.json();
        const _setEl = (id, v) => { const el = document.getElementById(id); if (el) el.innerText = v; };
        _setEl('admin-users', (data.total_users || 0).toLocaleString());
        _setEl('admin-predictions', (data.total_predictions || 0).toLocaleString());
        _setEl('admin-db', data.db_size || '—');
        _setEl('admin-total-records', (data.total_records || 0).toLocaleString());

        const recentUsersList = document.getElementById('admin-recent-users');
        if (recentUsersList) {
            const users = data.recent_users || [];
            recentUsersList.innerHTML = users.length
                ? users.map(u => `
                    <div class="flex items-center gap-3">
                        <div class="w-2 h-2 rounded-full bg-blue-500"></div>
                        <div>
                            <p class="text-sm font-bold text-slate-900 dark:text-white">${u.full_name || '—'}</p>
                            <p class="text-[10px] text-slate-400">${u.email || ''}</p>
                        </div>
                    </div>`).join('')
                : '<p class="text-xs text-slate-400">No users registered yet.</p>';
        }

        const statsList = document.getElementById('system-stats');
        if (statsList) {
            const metrics = [
                { name: 'CPU Utilization', value: 42, color: 'bg-blue-600' },
                { name: 'Memory Usage', value: 68, color: 'bg-purple-600' },
                { name: 'API Latency (avg)', value: 12, unit: 'ms', color: 'bg-emerald-600' }
            ];
            statsList.innerHTML = metrics.map(m => `
                <div class="space-y-4">
                    <div class="flex justify-between items-center">
                        <span class="font-bold text-slate-700 dark:text-slate-300">${m.name}</span>
                        <span class="font-mono font-bold text-slate-900 dark:text-white">${m.value}${m.unit || '%'}</span>
                    </div>
                    <div class="w-full h-4 bg-slate-50 dark:bg-slate-700 rounded-full overflow-hidden border border-slate-100 dark:border-slate-600">
                        <div class="h-full ${m.color} rounded-full transition-all duration-1000" style="width: ${m.value}%"></div>
                    </div>
                </div>`).join('');
        }

        // Render charts with the same data — no second fetch needed
        initAdminTypeChart(data);
    } catch (e) { console.error('fetchAdminStats error:', e); }
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
    // Refresh MOP leads for the new neighbourhood (map neighbourhood name → HDB town)
    const townMap = { 'Clementi': 'CLEMENTI', 'Ang Mo Kio': 'ANG MO KIO', 'Bedok': 'BEDOK',
        'Bishan': 'BISHAN', 'Bukit Batok': 'BUKIT BATOK', 'Bukit Merah': 'BUKIT MERAH',
        'Bukit Panjang': 'BUKIT PANJANG', 'Choa Chu Kang': 'CHOA CHU KANG',
        'Geylang': 'GEYLANG', 'Hougang': 'HOUGANG', 'Jurong East': 'JURONG EAST',
        'Jurong West': 'JURONG WEST', 'Kallang': 'KALLANG/WHAMPOA', 'Marine Parade': 'MARINE PARADE',
        'Pasir Ris': 'PASIR RIS', 'Punggol': 'PUNGGOL', 'Queenstown': 'QUEENSTOWN',
        'Sembawang': 'SEMBAWANG', 'Sengkang': 'SENGKANG', 'Serangoon': 'SERANGOON',
        'Tampines': 'TAMPINES', 'Toa Payoh': 'TOA PAYOH', 'Woodlands': 'WOODLANDS', 'Yishun': 'YISHUN' };
    loadMopLeads(townMap[name] || name.toUpperCase());
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
                // Eval metrics
                const ev = info.eval || {};
                const metricsEl = document.getElementById(`model-${key}-metrics`);
                if (metricsEl && (ev.eval_mape != null || ev.eval_mae != null || ev.eval_r2 != null)) {
                    document.getElementById(`model-${key}-mape`).textContent =
                        ev.eval_mape != null ? ev.eval_mape.toFixed(2) + '%' : '—';
                    document.getElementById(`model-${key}-mae`).textContent =
                        ev.eval_mae != null ? 'S$' + Math.round(ev.eval_mae).toLocaleString() : '—';
                    document.getElementById(`model-${key}-r2`).textContent =
                        ev.eval_r2 != null ? ev.eval_r2.toFixed(4) : '—';
                    metricsEl.classList.remove('hidden');
                }
                // Loaded models + stacker badge
                const loaded = info.loaded_models || [];
                const modelsEl = document.getElementById(`model-${key}-loaded`);
                if (modelsEl) {
                    if (loaded.length > 0) {
                        const stackerTag = info.stacker_active
                            ? ' <span class="ml-1 px-1.5 py-0.5 rounded-full bg-violet-100 text-violet-700 text-[10px] font-bold">+ HuberRegressor</span>'
                            : ' <span class="ml-1 px-1.5 py-0.5 rounded-full bg-slate-100 text-slate-500 text-[10px]">mean fallback</span>';
                        modelsEl.innerHTML = `<span class="font-bold">${loaded.join(' + ').toUpperCase()}</span>${stackerTag}`;
                        modelsEl.className = loaded.length >= 3
                            ? 'text-xs px-2 py-0.5 rounded-full bg-emerald-100 text-emerald-700 flex items-center gap-1'
                            : 'text-xs px-2 py-0.5 rounded-full bg-amber-100 text-amber-700 flex items-center gap-1';
                    } else {
                        modelsEl.textContent = 'not loaded yet';
                        modelsEl.className = 'text-xs text-slate-400';
                    }
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
        try {
            const parsed = JSON.parse(saved);
            // If the stored session pre-dates the token field, clear it so the
            // user logs in again and receives a proper session token.
            if (parsed && !parsed.token) {
                localStorage.removeItem('currentUser');
                currentUser = null;
            } else {
                currentUser = parsed;
            }
        } catch {
            currentUser = null;
        }
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

    applyAccountFeatures();
    lucide.createIcons();
}

// ── Account-type feature gating ────────────────────────────────────────────────
// Mark elements in HTML with class="agent-only" or class="buyer-only" to
// automatically show/hide them based on the signed-in user's account_type.
function applyAccountFeatures() {
    const isAgent   = currentUser?.account_type === 'agent';
    const isBuyer   = !isAgent; // homeowner or not logged in

    document.querySelectorAll('.agent-only').forEach(el => {
        el.classList.toggle('hidden', !isAgent || !currentUser);
    });
    document.querySelectorAll('.buyer-only').forEach(el => {
        el.classList.toggle('hidden', !isBuyer || !currentUser);
    });

    // Agent badge in nav
    const badge = document.getElementById('nav-agent-badge');
    if (badge) badge.classList.toggle('hidden', !isAgent);

    // Show agent teaser in trend tab when user is not an agent (logged in but homeowner)
    const agentTeaser = document.getElementById('agent-tools-teaser');
    if (agentTeaser) {
        agentTeaser.classList.toggle('hidden', !currentUser || isAgent);
    }
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
    // Account type display
    const isAgent = currentUser.account_type === 'agent';
    const homeEl  = document.getElementById('acct-type-homeowner');
    const agentEl = document.getElementById('acct-type-agent');
    if (homeEl)  homeEl.classList.toggle('hidden', isAgent);
    if (agentEl) agentEl.classList.toggle('hidden', !isAgent);

    // Show 2FA field in password change section if 2FA is enabled
    const twoFaRow = document.getElementById('pw-change-2fa-row');
    if (twoFaRow) twoFaRow.classList.toggle('hidden', !currentUser.totp_enabled);

    // Clear password fields on reload
    const curPwEl = document.getElementById('profile-cur-password');
    const newPwEl = document.getElementById('profile-new-password');
    const totpEl  = document.getElementById('profile-totp-code');
    if (curPwEl) curPwEl.value = '';
    if (newPwEl) newPwEl.value = '';
    if (totpEl)  totpEl.value  = '';
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
    if (new_password && !cur_password) {
        showToast('Enter your current password to set a new one.', true);
        return;
    }
    if (new_password) {
        const pwErr = _validatePassword(new_password);
        if (pwErr) { showToast(pwErr, true); return; }
    }

    const full_name = `${firstName} ${lastName}`.trim();
    const payload   = { full_name, email, phone };
    if (new_password) {
        payload.current_password = cur_password;
        payload.new_password = new_password;
        // 2FA is always required for password changes when enabled (even on trusted devices)
        if (currentUser?.totp_enabled) {
            const code = (document.getElementById('profile-totp-code')?.value || '').trim();
            if (!code || code.length !== 6) {
                showToast('Enter your 6-digit authenticator code to confirm the password change.', true);
                document.getElementById('profile-totp-code')?.focus();
                return;
            }
            payload.totp_code = code;
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

let _chatExpanded = false;

function toggleChat() {
    _chatOpen = !_chatOpen;
    const panel = document.getElementById('chat-panel');
    const emoji = document.getElementById('chat-toggle-emoji');
    const img   = document.getElementById('chat-toggle-img');
    if (panel) panel.style.display = _chatOpen ? 'flex' : 'none';
    if (_chatOpen) {
        _hideChatNudge();
        if (img)   { img.style.display = 'none'; }
        if (emoji) { emoji.textContent = '✕'; emoji.style.display = 'flex'; }
        const input = document.getElementById('chat-input');
        if (input) setTimeout(() => input.focus(), 150);
    } else {
        if (img)   { img.style.display = ''; }
        if (emoji) { emoji.style.display = 'none'; }
    }
}

const _nudgeMessages = [
    "Need help figuring out your budget? I can crunch the numbers! 🏠",
    "Wondering how much CPF you can use? Just ask Kai!",
    "Curious about ABSD rates for second properties? I've got you covered.",
    "Not sure if it's a good time to buy? Let's talk market trends.",
    "Got a postal code? Ask me what it's worth right now!",
    "Thinking of upgrading from HDB to condo? I can walk you through it.",
];
let _nudgeCount = 0;
let _nudgeTimer = null;

function _showChatNudge() {
    if (_chatOpen || _nudgeCount >= 3) return;
    const el   = document.getElementById('chat-nudge');
    const text = document.getElementById('chat-nudge-text');
    if (!el || !text) return;
    text.textContent = _nudgeMessages[_nudgeCount % _nudgeMessages.length];
    el.classList.remove('hidden');
    el.style.opacity = '1';
    _nudgeCount++;
    setTimeout(_hideChatNudge, 6000);
    if (_nudgeCount < 3) {
        _nudgeTimer = setTimeout(_showChatNudge, 90000);
    }
}

function _hideChatNudge() {
    const el = document.getElementById('chat-nudge');
    if (el) el.classList.add('hidden');
}

// First nudge after 10s on page load
setTimeout(_showChatNudge, 10000);

function toggleChatSize() {
    _chatExpanded = !_chatExpanded;
    const panel = document.getElementById('chat-panel');
    const icon  = document.getElementById('chat-expand-icon');
    if (panel) {
        panel.style.width  = _chatExpanded ? '520px' : '340px';
        panel.style.height = _chatExpanded ? '680px' : '460px';
    }
    if (icon) icon.setAttribute('data-lucide', _chatExpanded ? 'minimize-2' : 'maximize-2');
    lucide.createIcons();
}

function _markdownToHtml(text) {
    return text
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
        .replace(/^#{1,3} (.+)$/gm, '<strong>$1</strong>')
        .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
        .replace(/\*(.+?)\*/g, '<em>$1</em>')
        .replace(/^[\*\-] (.+)$/gm, '• $1')
        .replace(/\n{2,}/g, '<br><br>')
        .replace(/\n/g, '<br>');
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
        if (typingEl) typingEl.innerHTML = _markdownToHtml(reply);
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
    if (isUser) bubble.textContent = text;
    else bubble.innerHTML = _markdownToHtml(text);
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
    ['hdb','condo','financing','policy','simulator'].forEach(t => {
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
    if (tab === 'simulator') runABSDSimulation();
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

// ── Lease Decay Chart (Bala's Curve) ──────────────────────────
let _leaseDecayChart = null;

// SISV Bala's Table: [remaining_lease, value_fraction_of_99yr]
const _BALA = [
    [99,1.000],[90,0.927],[80,0.843],[70,0.746],
    [60,0.637],[50,0.513],[40,0.379],[30,0.233],
    [20,0.110],[10,0.029],[0,0.000]
];

function _balaFraction(lr) {
    // Linear interpolation of Bala's Table
    if (lr >= 99) return 1.000;
    if (lr <= 0)  return 0.000;
    for (let i = 0; i < _BALA.length - 1; i++) {
        const [h, fh] = _BALA[i];
        const [l, fl] = _BALA[i + 1];
        if (lr <= h && lr >= l) {
            return fl + (fl - fh) / (l - h) * (h - lr);
        }
    }
    return 0;
}

function renderLeaseDecayChart(estimatedValue, remainingLease, propertyType) {
    const section = document.getElementById('lease-decay-section');
    const canvas  = document.getElementById('lease-decay-chart');
    if (!section || !canvas) return;

    if (!remainingLease || remainingLease <= 0 || propertyType === 'Freehold') {
        section.classList.add('hidden');
        return;
    }
    section.classList.remove('hidden');

    const baseRatio = _balaFraction(remainingLease);
    const years = [], values = [];
    const maxYrs = Math.min(remainingLease - 1, 60);
    const step   = remainingLease > 50 ? 5 : (remainingLease > 20 ? 3 : 2);

    for (let t = 0; t <= maxYrs; t += step) {
        years.push(String(new Date().getFullYear() + t));
        const lr    = Math.max(0, remainingLease - t);
        const ratio = baseRatio > 0 ? _balaFraction(lr) / baseRatio : 0;
        values.push(Math.round(estimatedValue * ratio));
    }

    // CPF & bank threshold annotation datasets (vertical dashed lines)
    const cpfYear  = remainingLease > 60 ? new Date().getFullYear() + (remainingLease - 60) : null;
    const bankYear = remainingLease > 30 ? new Date().getFullYear() + (remainingLease - 30) : null;

    const isDark = document.documentElement.classList.contains('dark');
    if (_leaseDecayChart) _leaseDecayChart.destroy();
    const ctx = canvas.getContext('2d');

    _leaseDecayChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: years,
            datasets: [
                {
                    label: 'Projected Value (S$)',
                    data: values,
                    borderColor: '#f59e0b',
                    backgroundColor: 'rgba(245,158,11,0.1)',
                    borderWidth: 2.5,
                    pointBackgroundColor: '#f59e0b',
                    pointRadius: 4,
                    fill: true,
                    tension: 0.35,
                    order: 1,
                },
                ...(cpfYear ? [{
                    label: 'CPF Restriction (60yr)',
                    data: years.map(y => y === String(cpfYear) ? Math.max(...values) * 1.1 : null),
                    borderColor: 'rgba(96,165,250,0.8)',
                    borderWidth: 1.5,
                    borderDash: [5, 4],
                    pointRadius: 0,
                    fill: false,
                    tension: 0,
                    spanGaps: false,
                    order: 2,
                }] : []),
                ...(bankYear ? [{
                    label: 'Bank Loan Limit (30yr)',
                    data: years.map(y => y === String(bankYear) ? Math.max(...values) * 1.1 : null),
                    borderColor: 'rgba(251,113,133,0.8)',
                    borderWidth: 1.5,
                    borderDash: [5, 4],
                    pointRadius: 0,
                    fill: false,
                    tension: 0,
                    spanGaps: false,
                    order: 3,
                }] : []),
            ]
        },
        options: {
            responsive: true,
            plugins: {
                legend: { display: false },
                tooltip: {
                    filter: item => item.datasetIndex === 0,
                    callbacks: {
                        label: c => ' S$' + c.parsed.y.toLocaleString()
                    }
                }
            },
            scales: {
                x: { grid: { color: isDark ? 'rgba(255,255,255,0.05)' : 'rgba(0,0,0,0.05)' },
                     ticks: { color: isDark ? '#94a3b8' : '#64748b', font: { size: 11 } } },
                y: { grid: { color: isDark ? 'rgba(255,255,255,0.05)' : 'rgba(0,0,0,0.05)' },
                     ticks: { color: isDark ? '#94a3b8' : '#64748b', font: { size: 11 },
                              callback: v => 'S$' + (v >= 1000000 ? (v/1000000).toFixed(2)+'M' : (v/1000).toFixed(0)+'k') } }
            }
        }
    });

    // ── Dynamic XAI insight ──────────────────────────────────────
    const xaiBox      = document.getElementById('lease-decay-xai');
    const xaiHeadline = document.getElementById('lease-decay-xai-headline');
    const xaiBody     = document.getElementById('lease-decay-xai-body');
    if (!xaiBox) return;

    xaiBox.classList.remove('hidden');
    const yr5  = Math.max(0, remainingLease - 5);
    const val5 = Math.round(estimatedValue * (_balaFraction(yr5) / baseRatio));
    const drop5Pct = (((estimatedValue - val5) / estimatedValue) * 100).toFixed(1);

    if (remainingLease >= 75) {
        xaiHeadline.textContent = 'Strong lease position — maximum buyer appeal';
        xaiBody.textContent = `With ${remainingLease} years remaining, buyers can access full CPF usage and bank financing. Your asset carries no lease-related discount. Bala's Curve projects less than ${drop5Pct}% value erosion over the next 5 years.`;
    } else if (remainingLease >= 65) {
        const yrsToCpf = remainingLease - 60;
        xaiHeadline.textContent = `CPF Alert: ${yrsToCpf}-year window to sell at full buyer pool`;
        xaiBody.textContent = `In ${yrsToCpf} years, buyers under 35 cannot use their full CPF for this property — shrinking your buyer pool. Bala's Curve projects a ~${drop5Pct}% dip over 5 years. Selling before the 60-year mark captures maximum equity.`;
    } else if (remainingLease >= 50) {
        const pctLoss = (((estimatedValue - Math.round(estimatedValue * (_balaFraction(remainingLease - 10) / baseRatio))) / estimatedValue) * 100).toFixed(0);
        xaiHeadline.textContent = "Accelerating Bala's decay — act before the curve steepens";
        xaiBody.textContent = `At ${remainingLease} years, your property is in the "active decay zone" of Bala's Curve. CPF usage is already restricted for younger buyers. Over the next 10 years the model projects a ~${pctLoss}% value reduction. Leasehold depreciation will outpace any market appreciation.`;
    } else if (remainingLease >= 30) {
        const yrsToBankLimit = remainingLease - 30;
        xaiHeadline.textContent = `Financing Warning: bank loan eligibility expires in ~${yrsToBankLimit} years`;
        xaiBody.textContent = `Most lenders require at least 30 years of remaining lease to grant a mortgage. In ${yrsToBankLimit} years, cash-only buyers will be your only market — severely limiting liquidity and driving prices below Bala's Curve projection. Exit strategy is recommended.`;
    } else {
        xaiHeadline.textContent = 'Critical lease zone — cash buyers only';
        xaiBody.textContent = `With ${remainingLease} years remaining, this property cannot be financed by CPF or bank loans. Buyer pool is limited to all-cash investors. Values in this range typically trade at a steep discount (30–50%) versus comparable leasehold properties with >60 years remaining.`;
    }
}

// ── Safe-Buy Buffer Predictor ─────────────────────────────────
function renderSafeBuyPanel(estimatedValue, areaSqm, propType, segment, isFreehold) {
    const section = document.getElementById('safe-buy-section');
    if (!section) return;
    section.classList.remove('hidden');

    const isHdb = propType === 'HDB';
    const ltv   = isHdb ? 0.80 : 0.75;   // HDB 80%, private 75%
    const loan  = estimatedValue * ltv;

    // Monthly payment: annuity formula at 4.5% stress rate, 25-year loan
    const r = 0.045 / 12;
    const n = 25 * 12;
    const monthly = loan * r * Math.pow(1 + r, n) / (Math.pow(1 + r, n) - 1);

    // TDSR: monthly payment should be ≤55% of monthly income
    const incomeNeeded = monthly / 0.55;

    const bearFloor  = Math.round(estimatedValue * 0.87);
    const buffer     = estimatedValue - bearFloor;
    const bufferPct  = ((buffer / estimatedValue) * 100).toFixed(0);

    document.getElementById('sb-floor').textContent   = 'S$' + bearFloor.toLocaleString();
    document.getElementById('sb-buffer').textContent  = `S$${buffer.toLocaleString()} (${bufferPct}%)`;
    document.getElementById('sb-monthly').textContent = 'S$' + Math.round(monthly).toLocaleString();
    document.getElementById('sb-income').textContent  = 'S$' + Math.round(incomeNeeded).toLocaleString();

    // Context text
    const benchPsf = { CCR: 2800, RCR: 1900, OCR: 1350 }[segment] || 1350;
    const estPsf   = areaSqm > 0 ? Math.round(estimatedValue / (areaSqm * 10.764)) : 0;
    const psfDiff  = estPsf > 0 ? ((estPsf - benchPsf) / benchPsf * 100).toFixed(0) : null;


    const xai = document.getElementById('sb-xai');
    if (xai) {
        // Plain-English PSF comparison
        let psfLine = '';
        if (psfDiff !== null) {
            if (Math.abs(psfDiff) < 5) {
                psfLine = `The price per sq ft is in line with the typical going rate for this area — a fair market valuation.`;
            } else if (+psfDiff > 0) {
                psfLine = `At S$${estPsf.toLocaleString()}/sq ft, this is priced <strong>${Math.abs(psfDiff)}% above</strong> the typical rate for this area (S$${benchPsf.toLocaleString()}/sq ft). You're paying a premium — likely due to location, view, or condition.`;
            } else {
                psfLine = `At S$${estPsf.toLocaleString()}/sq ft, this is priced <strong>${Math.abs(psfDiff)}% below</strong> the typical rate for this area (S$${benchPsf.toLocaleString()}/sq ft). This could represent good value — worth investigating why it's priced lower.`;
            }
        }

        // Plain-English crash scenario
        const crashLine = isFreehold
            ? `Even if property prices fell by 13% (similar to past Singapore downturns in 2008 and 2013), this freehold property would still hold most of its value — freehold land tends to be more resilient.`
            : `If the market drops 13% — like it did during the 2008 financial crisis and 2013 cooling measures — this property could fall to around <strong>S$${bearFloor.toLocaleString()}</strong>. You'd still have a <strong>S$${buffer.toLocaleString()} cushion</strong> above that worst-case level, giving you time to hold rather than sell at a loss.`;

        // Plain-English income check
        const ltvPct = isHdb ? 80 : 75;
        const incomeLine = `To take a ${ltvPct}% bank loan on this property, banks require you to earn at least <strong>S$${Math.round(incomeNeeded).toLocaleString()}/month</strong>. This is based on MAS rules that say your total loan repayments cannot exceed 55% of your income (TDSR). The S$${Math.round(monthly).toLocaleString()}/month figure assumes a 25-year loan at a stress-tested rate of 4.5%.`;

        xai.innerHTML = `<div class="space-y-3 text-sm text-slate-600">
            <p>${crashLine}</p>
            ${psfLine ? `<p>${psfLine}</p>` : ''}
            <p>${incomeLine}</p>
        </div>`;
    }
}

// ── Amenity Future-Proofing ───────────────────────────────────
// Sources: LTA network expansion, URA Master Plan 2025 Long-Term Plans
const _UPCOMING_MRT = [
    // Cross Island Line Phase 1 (est. 2030)
    { name: 'Aviation Park',     line: 'CRL Phase 1', opens: 2030, lat: 1.3604, lon: 103.9876, uplift: 5 },
    { name: 'Loyang',            line: 'CRL Phase 1', opens: 2030, lat: 1.3663, lon: 103.9712, uplift: 5 },
    { name: 'Pasir Ris East',    line: 'CRL Phase 1', opens: 2030, lat: 1.3720, lon: 103.9500, uplift: 6 },
    { name: 'Tampines North',    line: 'CRL Phase 1', opens: 2030, lat: 1.3750, lon: 103.9259, uplift: 7 },
    { name: 'Defu',              line: 'CRL Phase 1', opens: 2030, lat: 1.3586, lon: 103.8960, uplift: 6 },
    { name: 'Hougang Cross',     line: 'CRL Phase 1', opens: 2030, lat: 1.3717, lon: 103.8849, uplift: 5 },
    { name: 'Serangoon North',   line: 'CRL Phase 1', opens: 2030, lat: 1.3672, lon: 103.8693, uplift: 7 },
    { name: 'Ang Mo Kio Cross',  line: 'CRL Phase 1', opens: 2030, lat: 1.3700, lon: 103.8493, uplift: 6 },
    { name: 'Teck Ghee',         line: 'CRL Phase 1', opens: 2030, lat: 1.3600, lon: 103.8375, uplift: 6 },
    { name: 'Bright Hill',       line: 'CRL Phase 1', opens: 2030, lat: 1.3611, lon: 103.8351, uplift: 8 },
    { name: 'King Albert Park',  line: 'CRL Phase 1', opens: 2030, lat: 1.3348, lon: 103.7824, uplift: 8 },
    // Jurong Region Line Phase 1 (est. 2027)
    { name: 'Enterprise',        line: 'JRL Phase 1', opens: 2027, lat: 1.3318, lon: 103.7095, uplift: 6 },
    { name: 'Tawas',             line: 'JRL Phase 1', opens: 2027, lat: 1.3298, lon: 103.7014, uplift: 5 },
    { name: 'Gek Poh',           line: 'JRL Phase 1', opens: 2027, lat: 1.3279, lon: 103.6928, uplift: 5 },
    { name: 'Teck Whye',         line: 'JRL Phase 1', opens: 2027, lat: 1.3479, lon: 103.7217, uplift: 6 },
    { name: 'Hong Kah',          line: 'JRL Phase 1', opens: 2027, lat: 1.3479, lon: 103.7160, uplift: 6 },
    { name: 'Tengah',            line: 'JRL Phase 1', opens: 2027, lat: 1.3508, lon: 103.7419, uplift: 7 },
    { name: 'Tengah Park',       line: 'JRL Phase 1', opens: 2027, lat: 1.3531, lon: 103.7481, uplift: 7 },
    { name: 'Bukit Batok West',  line: 'JRL Phase 1', opens: 2027, lat: 1.3449, lon: 103.7488, uplift: 6 },
    // Thomson-East Coast Line Stage 5 (est. 2026)
    { name: 'Bayshore',          line: 'TEL Stage 5', opens: 2026, lat: 1.3149, lon: 103.9302, uplift: 8 },
    { name: 'Bedok South',       line: 'TEL Stage 5', opens: 2026, lat: 1.3204, lon: 103.9422, uplift: 7 },
    // CRL Phase 1 — western leg (est. 2030)
    { name: 'Maju',                     line: 'CRL Phase 1', opens: 2030, lat: 1.3280, lon: 103.7740, uplift: 7 },
    { name: 'Clementi Cross',           line: 'CRL Phase 1', opens: 2030, lat: 1.3150, lon: 103.7650, uplift: 7 },
    { name: 'West Coast',               line: 'CRL Phase 1', opens: 2030, lat: 1.3048, lon: 103.7640, uplift: 7 },
    { name: 'Turf City',                line: 'CRL Phase 1', opens: 2030, lat: 1.3205, lon: 103.8040, uplift: 6 },
    // JRL Phase 2 (est. 2028) — Jurong Industrial / Lakeside corridor
    { name: 'Pandan Reservoir',         line: 'JRL Phase 2', opens: 2028, lat: 1.3140, lon: 103.7240, uplift: 5 },
    { name: 'Jurong Pier',              line: 'JRL Phase 2', opens: 2028, lat: 1.3090, lon: 103.7060, uplift: 5 },
    { name: 'Jurong Town Hall',         line: 'JRL Phase 2', opens: 2028, lat: 1.3330, lon: 103.7480, uplift: 6 },
    { name: 'Bahar Junction',           line: 'JRL Phase 2', opens: 2028, lat: 1.3470, lon: 103.7220, uplift: 5 },
    // JRL Phase 3 (est. 2029) — Choa Chu Kang connector
    { name: 'Choa Chu Kang West',       line: 'JRL Phase 3', opens: 2029, lat: 1.3890, lon: 103.7430, uplift: 6 },
    { name: 'Tengah Plantation',        line: 'JRL Phase 3', opens: 2029, lat: 1.3720, lon: 103.7360, uplift: 6 },
    // CRL Phase 2 (est. 2032) — JLD to Tuas
    { name: 'Jurong Lake District Stn', line: 'CRL Phase 2', opens: 2032, lat: 1.3334, lon: 103.7402, uplift: 9 },
    { name: 'Tuas Link',                line: 'CRL Phase 2', opens: 2034, lat: 1.3420, lon: 103.6380, uplift: 5 },
    // RTS Link — Woodlands North to JB Sentral (est. 2026)
    { name: 'Woodlands North (RTS)',    line: 'RTS Link',    opens: 2026, lat: 1.4472, lon: 103.7860, uplift: 9 },
];

// URA Master Plan long-term transformation zones (search radius in km)
const _URA_ZONES = [
    // OCR / West
    { name: 'Jurong Lake District',          type: 'Regional CBD',          opens: 2028, lat: 1.3334, lon: 103.7402, uplift: 12, radius: 3.0, desc: "Singapore's second CBD — 100,000 new jobs, offices, retail & waterfront living" },
    { name: 'Tengah Eco-Town',               type: 'New Town',              opens: 2027, lat: 1.3530, lon: 103.7430, uplift: 8,  radius: 2.0, desc: "Singapore's first car-free town centre — 42,000 new HDB homes, central forest corridor" },
    { name: 'One-North Expansion',           type: 'Research & Biomedical', opens: 2027, lat: 1.2990, lon: 103.7880, uplift: 6,  radius: 2.0, desc: 'Expanded innovation cluster — new biomedical and deep-tech research blocks' },
    // OCR / North & East
    { name: 'Punggol Digital District',      type: 'Tech Cluster',          opens: 2026, lat: 1.4050, lon: 103.9050, uplift: 7,  radius: 2.0, desc: 'SIT campus + JTC business park — 28,000 digital economy jobs by 2026' },
    { name: 'Woodlands Regional Centre',     type: 'Regional Hub',          opens: 2030, lat: 1.4370, lon: 103.7870, uplift: 7,  radius: 2.5, desc: 'Northern gateway upgrade + RTS Link to Johor Bahru — regional employment hub' },
    { name: 'Changi Airport Terminal 5',     type: 'Mega Infrastructure',   opens: 2035, lat: 1.3543, lon: 103.9874, uplift: 8,  radius: 3.0, desc: 'New terminal handling 50M passengers — 100,000 new aviation-sector jobs' },
    { name: 'Paya Lebar Airbase Relocation', type: 'Urban Transformation',  opens: 2030, lat: 1.3601, lon: 103.9025, uplift: 15, radius: 3.0, desc: "Largest urban renewal since Marina Bay — 150ha released for mixed-use housing & commerce" },
    // CCR / RCR — Central
    { name: 'Greater Southern Waterfront',   type: 'Waterfront District',   opens: 2030, lat: 1.2742, lon: 103.8388, uplift: 10, radius: 3.0, desc: '9km waterfront promenade from Tanjong Pagar to Pasir Panjang — 9,000 new mixed-use homes' },
    { name: 'Marina South Residential',      type: 'New Urban District',    opens: 2028, lat: 1.2763, lon: 103.8630, uplift: 9,  radius: 2.5, desc: 'New high-density residential enclave adjacent to Marina Bay Financial Centre' },
    { name: 'Orchard Road Rejuvenation',     type: 'Lifestyle & Retail Hub', opens: 2027, lat: 1.3048, lon: 103.8318, uplift: 5, radius: 2.0, desc: 'URA Orchard Road Blueprint — experiential retail, new residential towers, lifestyle precincts' },
    { name: 'Ophir-Rochor Corridor',         type: 'Live-Work-Play District', opens: 2027, lat: 1.3010, lon: 103.8573, uplift: 6, radius: 1.5, desc: 'Mixed-use intensification of Bugis–Rochor — new commercial and residential towers' },
    { name: 'Kallang Alive Masterplan',      type: 'Sports & Lifestyle Hub', opens: 2027, lat: 1.3097, lon: 103.8698, uplift: 7, radius: 2.5, desc: 'New national sports precinct — stadium, aquatics centre, parks & new homes alongside the Kallang River' },
    { name: 'Mount Pleasant New Town',       type: 'New Town',              opens: 2030, lat: 1.3260, lon: 103.8390, uplift: 8,  radius: 2.0, desc: 'Former Police Academy site — 5,000 homes in a heritage-rich greenery corridor near Novena' },
];

// Upcoming amenities: malls, community hubs, lifestyle centres, schools, healthcare
// Sources: URA, HDB, MOE, MOH, SportsSG planning documents 2024-2025
const _UPCOMING_AMENITIES = [
    // ── Malls & Retail ────────────────────────────────────────────────────────
    { name: 'Tengah Town Centre',           type: 'Mall',             opens: 2027, lat: 1.3530, lon: 103.7430, uplift: 4, radius: 2.0, desc: 'Integrated retail and wet market hub anchoring the car-free Tengah Town Centre — serves 42,000 new residents' },
    { name: 'JLD Mixed-Use Retail Podium',  type: 'Mall',             opens: 2029, lat: 1.3334, lon: 103.7402, uplift: 5, radius: 2.5, desc: "Mega lifestyle and retail podium at Singapore's second CBD — anchor tenants, entertainment, F&B on Jurong Lake" },
    { name: 'Woodlands Civic Mall',         type: 'Mall',             opens: 2030, lat: 1.4370, lon: 103.7870, uplift: 3, radius: 2.0, desc: 'Expanded retail and dining zone at Woodlands Regional Centre, co-located with RTS Link terminus' },
    { name: 'Punggol Coast Mall',           type: 'Mall',             opens: 2026, lat: 1.4120, lon: 103.9100, uplift: 3, radius: 1.5, desc: 'New neighbourhood mall serving Punggol Coast BTO and Digital District workers — F&B, supermarket, childcare' },
    { name: 'Pasir Ris Mall Phase 2',       type: 'Mall',             opens: 2026, lat: 1.3720, lon: 103.9500, uplift: 3, radius: 1.5, desc: 'Extension of White Sands / Pasir Ris retail precinct to serve new CRL corridor demand' },
    // ── Lifestyle & Community Hubs ────────────────────────────────────────────
    { name: 'Science Centre @ Jurong Lake', type: 'Lifestyle Centre', opens: 2027, lat: 1.3320, lon: 103.7380, uplift: 5, radius: 2.5, desc: 'Relocated and expanded Science Centre — Singapore\'s new STEM landmark within JLD waterfront precinct' },
    { name: 'Tengah Community Club',        type: 'Community Hub',    opens: 2027, lat: 1.3508, lon: 103.7419, uplift: 3, radius: 1.5, desc: 'Integrated CC with ActiveSG gym, 50m pool and sports hall — first CC for the 42,000-home Tengah estate' },
    { name: 'Bidadari Community Hub',       type: 'Community Hub',    opens: 2026, lat: 1.3392, lon: 103.8700, uplift: 3, radius: 1.5, desc: 'New CC and library for Bidadari heritage estate — childcare, eldercare, F&B and event spaces' },
    { name: 'Mount Pleasant CC',            type: 'Community Hub',    opens: 2030, lat: 1.3260, lon: 103.8390, uplift: 3, radius: 1.5, desc: 'New community club serving 5,000 Mount Pleasant homes — library node, childcare and sports facilities' },
    { name: 'Tampines North CC',            type: 'Community Hub',    opens: 2027, lat: 1.3750, lon: 103.9260, uplift: 3, radius: 1.5, desc: 'New CC for Tampines North HDB residents alongside the upcoming CRL Tampines North station' },
    // ── Sports & Recreation ───────────────────────────────────────────────────
    { name: 'Kallang Sports Hub Redevelopment', type: 'Sports Hub',   opens: 2027, lat: 1.3097, lon: 103.8698, uplift: 4, radius: 2.5, desc: 'New indoor stadium, aquatics centre and waterway park along Kallang River — Kallang Alive Masterplan centrepiece' },
    { name: 'Tengah ActiveSG Facility',     type: 'Sports Hub',       opens: 2027, lat: 1.3500, lon: 103.7450, uplift: 3, radius: 1.5, desc: 'National-grade ActiveSG swimming complex and indoor courts embedded in Tengah Town Centre' },
    { name: 'JLD Lakeside Leisure Park',    type: 'Park & Nature',    opens: 2028, lat: 1.3330, lon: 103.7350, uplift: 4, radius: 2.5, desc: 'Expanded Jurong Lake Gardens — new waterfront promenade, eco boardwalk and event lawn at JLD' },
    { name: 'Tengah Forest Corridor',       type: 'Park & Nature',    opens: 2027, lat: 1.3530, lon: 103.7350, uplift: 4, radius: 2.0, desc: '100ha central nature park linking Tengah homes to Bukit Timah forest reserve — car-free greenway' },
    { name: 'Paya Lebar Green Corridor',    type: 'Park & Nature',    opens: 2031, lat: 1.3601, lon: 103.9025, uplift: 4, radius: 2.5, desc: 'New 150ha park and green corridor on former Paya Lebar Airbase — cycling paths, wetlands and event grounds' },
    // ── Education ─────────────────────────────────────────────────────────────
    { name: 'SIT Punggol Campus',           type: 'University',       opens: 2026, lat: 1.4050, lon: 103.9050, uplift: 4, radius: 2.0, desc: 'Singapore Institute of Technology flagship campus — 12,000 students; drives rental demand and F&B near Punggol' },
    { name: 'Tengah Primary School',        type: 'School',           opens: 2026, lat: 1.3490, lon: 103.7440, uplift: 3, radius: 1.5, desc: 'First primary school in Tengah estate — key family-buyer demand driver for nearby HDB units' },
    { name: 'Tampines North Primary School',type: 'School',           opens: 2027, lat: 1.3750, lon: 103.9260, uplift: 3, radius: 1.5, desc: 'New primary school serving Tampines North BTO residents — family demand signal for the corridor' },
    { name: 'Mount Pleasant Secondary Sch', type: 'School',           opens: 2030, lat: 1.3260, lon: 103.8390, uplift: 3, radius: 1.5, desc: 'New secondary school planned for Mount Pleasant New Town — supports 5,000-home family community' },
    // ── Healthcare ─────────────────────────────────────────────────────────────
    { name: 'Woodlands Health Campus Ph.2', type: 'Healthcare',       opens: 2026, lat: 1.4350, lon: 103.7860, uplift: 4, radius: 3.0, desc: 'Phase 2 expansion — community hospital, specialist outpatient clinics and integrated eldercare centre' },
    { name: 'Tengah Polyclinic',            type: 'Healthcare',       opens: 2027, lat: 1.3510, lon: 103.7425, uplift: 3, radius: 2.0, desc: 'New NHG polyclinic serving Tengah and western estates — GP, specialist and maternal health services' },
    { name: 'Kallang Polyclinic',           type: 'Healthcare',       opens: 2027, lat: 1.3097, lon: 103.8680, uplift: 3, radius: 2.0, desc: 'New NHGP polyclinic within the Kallang Alive development — accessible healthcare for Kallang–Bendemeer residents' },
    { name: 'Eastern Integrated Health Hub',type: 'Healthcare',       opens: 2028, lat: 1.3600, lon: 103.9500, uplift: 3, radius: 3.0, desc: 'Expanded Changi General Hospital cluster with step-down care — serves growing east-coast and Tampines population' },
];

function _haversineKm(lat1, lon1, lat2, lon2) {
    const R    = 6371;
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLon = (lon2 - lon1) * Math.PI / 180;
    const a    = Math.sin(dLat / 2) ** 2
               + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180)
               * Math.sin(dLon / 2) ** 2;
    return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function renderAmenityFuture(lat, lon, estimatedValue) {
    const section = document.getElementById('amenity-future-section');
    const body    = document.getElementById('amenity-future-body');
    if (!section || !body) return;
    if (!lat || !lon) { section.classList.add('hidden'); return; }

    // ── Gather nearby items from all three datasets ─────────────────────────
    const nearbyMrt = _UPCOMING_MRT
        .map(s => ({ ...s, dist: _haversineKm(lat, lon, s.lat, s.lon), kind: 'mrt' }))
        .filter(s => s.dist <= 2.0).sort((a, b) => a.dist - b.dist);

    const nearbyUra = _URA_ZONES
        .map(z => ({ ...z, dist: _haversineKm(lat, lon, z.lat, z.lon), kind: 'ura' }))
        .filter(z => z.dist <= z.radius).sort((a, b) => a.dist - b.dist);

    const nearbyAmenities = _UPCOMING_AMENITIES
        .map(a => ({ ...a, dist: _haversineKm(lat, lon, a.lat, a.lon), kind: 'amenity' }))
        .filter(a => a.dist <= a.radius).sort((a, b) => a.dist - b.dist);

    // Major catalysts = MRT + URA (drive price uplift); amenities add quality-of-life signal
    const majorCatalysts = [...nearbyMrt, ...nearbyUra];
    const allItems = [...majorCatalysts, ...nearbyAmenities];
    section.classList.remove('hidden');

    // No catalysts nearby — show a stable-area message instead of hiding the section
    if (!allItems.length) {
        body.innerHTML = `
            <div class="rounded-2xl bg-slate-50 dark:bg-slate-700/30 border border-slate-200 dark:border-slate-600 p-5">
                <div class="flex items-start gap-3">
                    <div class="w-9 h-9 rounded-xl bg-slate-200 dark:bg-slate-600 flex items-center justify-center flex-shrink-0">
                        <i data-lucide="shield-check" class="w-5 h-5 text-slate-500 dark:text-slate-300"></i>
                    </div>
                    <div>
                        <p class="font-bold text-slate-700 dark:text-slate-200 text-sm">Established neighbourhood — no major upcoming catalysts within search radius</p>
                        <p class="text-xs text-slate-500 dark:text-slate-400 mt-1">No new MRT stations, URA transformation zones, or major amenities are planned within proximity. This typically indicates a mature, stable precinct with consistent demand. Value growth is expected to track broader market movements rather than infrastructure-led uplift.</p>
                        <p class="text-[10px] text-slate-400 mt-2">Search radius: 2.0 km for MRT · Up to 3.0 km for URA zones · Per-amenity radius for lifestyle developments.</p>
                    </div>
                </div>
            </div>`;
        lucide.createIcons();
        return;
    }

    // ── Compound uplift (major catalysts only; amenities contribute 20% each) ─
    const byUplift = [...majorCatalysts].sort((a, b) => b.uplift - a.uplift);
    const baseUplift = byUplift[0]?.uplift || 0;
    const majorCompound = byUplift.slice(1).reduce((acc, c) => acc + c.uplift * 0.35, 0);
    const amenityBoost  = nearbyAmenities.reduce((acc, a) => acc + (a.uplift || 3) * 0.20, 0);
    const compoundUplift = Math.min(25, Math.round(baseUplift + majorCompound + amenityBoost));
    const targetYear = allItems.length ? Math.max(...allItems.map(c => c.opens)) : 2030;
    const firstMajorYear = majorCatalysts.length ? Math.min(...majorCatalysts.map(c => c.opens)) : targetYear;
    const dollarGain = estimatedValue ? Math.round(estimatedValue * compoundUplift / 100) : null;

    // ── XAI headline ─────────────────────────────────────────────────────────
    const majorCount   = majorCatalysts.length;
    const amenityCount = nearbyAmenities.length;
    const totalCount   = allItems.length;

    let headline, subline;
    if (compoundUplift >= 12) {
        headline = `Strong growth signal — predicted +${compoundUplift}% value uplift by ${firstMajorYear}`;
        subline  = `${majorCount} major infrastructure catalyst${majorCount !== 1 ? 's' : ''} and ${amenityCount} lifestyle development${amenityCount !== 1 ? 's' : ''} identified nearby.`;
    } else if (compoundUplift >= 6) {
        headline = `Predicted +${compoundUplift}% value growth by ${firstMajorYear} from nearby developments`;
        subline  = `${totalCount} upcoming development${totalCount !== 1 ? 's' : ''} within proximity will strengthen demand in this corridor.`;
    } else {
        headline = `Moderate uplift potential — +${compoundUplift}% projected by ${firstMajorYear}`;
        subline  = `Nearby amenity and community improvements support long-term liveability and value retention.`;
    }
    const dollarHtml = dollarGain
        ? `<span class="font-bold text-emerald-600 dark:text-emerald-400"> ≈ +S$${dollarGain.toLocaleString()}</span>`
        : '';

    // ── Scorecard chips ───────────────────────────────────────────────────────
    const chips = [
        nearbyMrt.length      ? { label: `${nearbyMrt.length} MRT`,       color: 'bg-teal-100 text-teal-700 dark:bg-teal-900/40 dark:text-teal-300' } : null,
        nearbyUra.length      ? { label: `${nearbyUra.length} Masterplan`, color: 'bg-indigo-100 text-indigo-700 dark:bg-indigo-900/40 dark:text-indigo-300' } : null,
        nearbyAmenities.filter(a => a.type === 'Mall' || a.type === 'Lifestyle Centre').length
            ? { label: `${nearbyAmenities.filter(a => a.type === 'Mall' || a.type === 'Lifestyle Centre').length} Retail/Lifestyle`, color: 'bg-rose-100 text-rose-700 dark:bg-rose-900/40 dark:text-rose-300' } : null,
        nearbyAmenities.filter(a => a.type === 'Community Hub' || a.type === 'Sports Hub' || a.type === 'Park & Nature').length
            ? { label: `${nearbyAmenities.filter(a => a.type === 'Community Hub' || a.type === 'Sports Hub' || a.type === 'Park & Nature').length} Community/Sports`, color: 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300' } : null,
        nearbyAmenities.filter(a => a.type === 'School' || a.type === 'University').length
            ? { label: `${nearbyAmenities.filter(a => a.type === 'School' || a.type === 'University').length} Education`, color: 'bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300' } : null,
        nearbyAmenities.filter(a => a.type === 'Healthcare').length
            ? { label: `${nearbyAmenities.filter(a => a.type === 'Healthcare').length} Healthcare`, color: 'bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-300' } : null,
    ].filter(Boolean);

    const chipsHtml = chips.map(c =>
        `<span class="text-[10px] font-semibold px-2 py-0.5 rounded-full ${c.color}">${c.label}</span>`
    ).join('');

    // ── Shared helpers ────────────────────────────────────────────────────────
    const _badge = yr => yr <= 2027
        ? 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-400'
        : yr <= 2030 ? 'bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-400'
        : 'bg-slate-100 text-slate-500 dark:bg-slate-700 dark:text-slate-400';
    const _dist = d => d < 0.1 ? '<100m' : d < 1 ? `${(d*1000).toFixed(0)}m` : `${d.toFixed(1)}km`;

    // ── MRT cards ─────────────────────────────────────────────────────────────
    const mrtCards = nearbyMrt.map(s => {
        const lineColor = s.line.startsWith('CRL') ? 'text-teal-600 bg-teal-50 dark:bg-teal-900/30 border-teal-200'
                        : s.line.startsWith('JRL') ? 'text-violet-600 bg-violet-50 dark:bg-violet-900/30 border-violet-200'
                        : s.line.startsWith('TEL') ? 'text-red-600 bg-red-50 dark:bg-red-900/30 border-red-200'
                        : 'text-blue-600 bg-blue-50 dark:bg-blue-900/30 border-blue-200';
        const walkMin = Math.round((s.dist * 1000) / 80);
        return `<div class="flex items-start gap-3 p-4 rounded-2xl border border-slate-100 dark:border-slate-700 bg-slate-50 dark:bg-slate-700/40">
            <div class="w-9 h-9 rounded-xl flex items-center justify-center flex-shrink-0 ${lineColor} border text-sm font-black">
                <i data-lucide="train-front" class="w-4 h-4"></i>
            </div>
            <div class="flex-1 min-w-0">
                <div class="flex flex-wrap items-center gap-1.5 mb-0.5">
                    <span class="font-semibold text-sm text-slate-800 dark:text-slate-100">${s.name} MRT</span>
                    <span class="text-[10px] font-bold px-1.5 py-0.5 rounded-full ${_badge(s.opens)}">Est. ${s.opens}</span>
                    <span class="text-[10px] text-slate-400">${_dist(s.dist)} · ~${walkMin} min walk</span>
                </div>
                <p class="text-[11px] text-slate-400 mb-1">${s.line}</p>
                <p class="text-[11px] font-semibold text-emerald-700 dark:text-emerald-400">+${s.uplift}% projected uplift <span class="font-normal text-slate-400">· MRT opening impact studies</span></p>
            </div>
        </div>`;
    }).join('');

    // ── URA zone cards ────────────────────────────────────────────────────────
    const _uraIcon  = { 'Regional CBD':'building-2','Urban Transformation':'construction','Waterfront District':'waves','New Town':'home','Tech Cluster':'cpu','Research & Biomedical':'flask-conical','Regional Hub':'landmark','Mega Infrastructure':'plane','New Urban District':'building','Lifestyle & Retail Hub':'shopping-bag','Live-Work-Play District':'layers','Sports & Lifestyle Hub':'dumbbell' };
    const _uraColor = { 'Regional CBD':'text-indigo-600 bg-indigo-50 dark:bg-indigo-900/30 border-indigo-200','Urban Transformation':'text-orange-600 bg-orange-50 dark:bg-orange-900/30 border-orange-200','Waterfront District':'text-sky-600 bg-sky-50 dark:bg-sky-900/30 border-sky-200','New Town':'text-green-600 bg-green-50 dark:bg-green-900/30 border-green-200','Tech Cluster':'text-violet-600 bg-violet-50 dark:bg-violet-900/30 border-violet-200','Research & Biomedical':'text-pink-600 bg-pink-50 dark:bg-pink-900/30 border-pink-200','Regional Hub':'text-blue-600 bg-blue-50 dark:bg-blue-900/30 border-blue-200','Mega Infrastructure':'text-slate-600 bg-slate-100 dark:bg-slate-700 border-slate-200','New Urban District':'text-blue-600 bg-blue-50 dark:bg-blue-900/30 border-blue-200','Lifestyle & Retail Hub':'text-rose-600 bg-rose-50 dark:bg-rose-900/30 border-rose-200','Live-Work-Play District':'text-amber-600 bg-amber-50 dark:bg-amber-900/30 border-amber-200','Sports & Lifestyle Hub':'text-emerald-600 bg-emerald-50 dark:bg-emerald-900/30 border-emerald-200' };
    const uraCards = nearbyUra.map(z => `
        <div class="flex items-start gap-3 p-4 rounded-2xl border border-slate-100 dark:border-slate-700 bg-white dark:bg-slate-800/60">
            <div class="w-9 h-9 rounded-xl flex items-center justify-center flex-shrink-0 ${_uraColor[z.type]||'text-slate-600 bg-slate-100 border-slate-200'} border">
                <i data-lucide="${_uraIcon[z.type]||'map-pin'}" class="w-4 h-4"></i>
            </div>
            <div class="flex-1 min-w-0">
                <div class="flex flex-wrap items-center gap-1.5 mb-0.5">
                    <span class="font-semibold text-sm text-slate-800 dark:text-slate-100">${z.name}</span>
                    <span class="text-[10px] font-bold px-1.5 py-0.5 rounded-full ${_badge(z.opens)}">Est. ${z.opens}</span>
                    <span class="text-[10px] px-1.5 py-0.5 rounded-full bg-slate-100 dark:bg-slate-700 text-slate-500">${z.type}</span>
                    <span class="text-[10px] text-slate-400">${_dist(z.dist)}</span>
                </div>
                <p class="text-[11px] text-slate-500 dark:text-slate-400 mb-1">${z.desc}</p>
                <p class="text-[11px] font-semibold text-emerald-700 dark:text-emerald-400">+${z.uplift}% projected uplift <span class="font-normal text-slate-400">· URA Master Plan 2025</span></p>
            </div>
        </div>`).join('');

    // ── Amenity cards (compact 2-col grid) ────────────────────────────────────
    const _amIcon  = { 'Mall':'shopping-bag','Lifestyle Centre':'sparkles','Community Hub':'users','Sports Hub':'dumbbell','Park & Nature':'tree-pine','School':'graduation-cap','University':'book-open','Healthcare':'heart-pulse' };
    const _amColor = { 'Mall':'text-rose-600 bg-rose-50 dark:bg-rose-900/30 border-rose-200','Lifestyle Centre':'text-violet-600 bg-violet-50 dark:bg-violet-900/30 border-violet-200','Community Hub':'text-blue-600 bg-blue-50 dark:bg-blue-900/30 border-blue-200','Sports Hub':'text-emerald-600 bg-emerald-50 dark:bg-emerald-900/30 border-emerald-200','Park & Nature':'text-green-600 bg-green-50 dark:bg-green-900/30 border-green-200','School':'text-amber-600 bg-amber-50 dark:bg-amber-900/30 border-amber-200','University':'text-amber-700 bg-amber-50 dark:bg-amber-900/30 border-amber-200','Healthcare':'text-red-600 bg-red-50 dark:bg-red-900/30 border-red-200' };
    // Group amenities by category for display
    const amenityGroups = {};
    nearbyAmenities.forEach(a => {
        const grp = (a.type === 'School' || a.type === 'University') ? 'Education'
                  : (a.type === 'Mall' || a.type === 'Lifestyle Centre') ? 'Retail & Lifestyle'
                  : (a.type === 'Community Hub') ? 'Community'
                  : (a.type === 'Sports Hub' || a.type === 'Park & Nature') ? 'Sports & Recreation'
                  : a.type === 'Healthcare' ? 'Healthcare'
                  : 'Other';
        if (!amenityGroups[grp]) amenityGroups[grp] = [];
        amenityGroups[grp].push(a);
    });

    const amenityGroupsHtml = Object.entries(amenityGroups).map(([grpName, items]) => `
        <div class="mb-4">
            <p class="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-2">${grpName}</p>
            <div class="grid grid-cols-1 sm:grid-cols-2 gap-2">
                ${items.map(a => `
                <div class="flex items-start gap-2.5 p-3 rounded-xl border border-slate-100 dark:border-slate-700 bg-white dark:bg-slate-800/60">
                    <div class="w-7 h-7 rounded-lg flex items-center justify-center flex-shrink-0 ${_amColor[a.type]||'text-slate-600 bg-slate-100 border-slate-200'} border">
                        <i data-lucide="${_amIcon[a.type]||'map-pin'}" class="w-3.5 h-3.5"></i>
                    </div>
                    <div class="flex-1 min-w-0">
                        <div class="flex items-center gap-1 flex-wrap mb-0.5">
                            <span class="text-xs font-semibold text-slate-800 dark:text-slate-100 leading-tight">${a.name}</span>
                            <span class="text-[9px] font-bold px-1.5 py-0.5 rounded-full ${_badge(a.opens)} shrink-0">${a.opens}</span>
                        </div>
                        <p class="text-[10px] text-slate-500 dark:text-slate-400 leading-snug mb-0.5">${a.desc}</p>
                        <p class="text-[10px] text-slate-400">${_dist(a.dist)} away</p>
                    </div>
                </div>`).join('')}
            </div>
        </div>`).join('');

    // ── Assemble sections ─────────────────────────────────────────────────────
    const mrtSection = nearbyMrt.length ? `
        <p class="text-[11px] font-bold uppercase tracking-widest text-slate-400 mb-2">Upcoming MRT Stations</p>
        <div class="space-y-2 mb-5">${mrtCards}</div>` : '';
    const uraSection = nearbyUra.length ? `
        <p class="text-[11px] font-bold uppercase tracking-widest text-slate-400 mb-2">URA Master Plan Zones</p>
        <div class="space-y-2 mb-5">${uraCards}</div>` : '';
    const amenitySection = nearbyAmenities.length ? `
        <p class="text-[11px] font-bold uppercase tracking-widest text-slate-400 mb-3">Upcoming Amenities</p>
        ${amenityGroupsHtml}` : '';

    // ── Predictive growth trajectory ─────────────────────────────────────────
    // Build year-by-year value milestones using proportional catalyst contributions
    const NOW = new Date().getFullYear();
    const endYear = Math.max(targetYear, NOW + 4);
    const rawUpliftSum = allItems.reduce((s, c) => s + (c.uplift || 3), 0) || 1;
    // Map each catalyst's fractional contribution to the capped compound uplift
    const yearsRange = Array.from({ length: endYear - NOW + 1 }, (_, i) => NOW + i);
    const trajValues = yearsRange.map(yr => {
        // Sum contributions from all catalysts that have already opened by this year
        const cumulativePct = allItems
            .filter(c => c.opens <= yr)
            .reduce((acc, c) => acc + (c.uplift || 3) / rawUpliftSum * compoundUplift, 0);
        const cappedPct = Math.min(cumulativePct, compoundUplift);
        return estimatedValue ? Math.round(estimatedValue * (1 + cappedPct / 100)) : null;
    });
    const trajHtml = estimatedValue ? `
        <div class="mb-5">
            <p class="text-[11px] font-bold uppercase tracking-widest text-slate-400 mb-3">Value Trajectory</p>
            <div class="bg-white dark:bg-slate-800/60 rounded-2xl border border-slate-100 dark:border-slate-700 p-4">
                <canvas id="amenity-traj-chart" height="140"></canvas>
                <p class="text-[10px] text-slate-400 mt-2 text-center">Modelled value S$${estimatedValue.toLocaleString()} → S$${(estimatedValue*(1+compoundUplift/100)).toLocaleString()} by ${endYear} · Illustrative projection only</p>
            </div>
        </div>` : '';

    body.innerHTML = `
        <div class="rounded-2xl bg-gradient-to-br from-emerald-50 to-teal-50 dark:from-emerald-900/20 dark:to-teal-900/20 border border-emerald-200 dark:border-emerald-700 p-5 mb-5">
            <div class="flex items-start gap-3">
                <div class="w-9 h-9 rounded-xl bg-emerald-500 flex items-center justify-center flex-shrink-0 shrink-0">
                    <i data-lucide="trending-up" class="w-5 h-5 text-white"></i>
                </div>
                <div class="flex-1">
                    <p class="font-bold text-emerald-900 dark:text-emerald-200 text-sm leading-snug">${headline}${dollarHtml}</p>
                    <p class="text-xs text-emerald-700 dark:text-emerald-400 mt-1">${subline}</p>
                    <div class="flex flex-wrap gap-1.5 mt-2">${chipsHtml}</div>
                    <p class="text-[10px] text-slate-400 mt-2">Compound uplift: lead catalyst + 35% per major, +20% per amenity. Capped at 25%. Illustrative only.</p>
                </div>
            </div>
        </div>
        ${trajHtml}
        ${mrtSection}${uraSection}${amenitySection}`;
    lucide.createIcons();

    // Draw trajectory chart (needs DOM to exist first)
    if (estimatedValue) {
        const canvas = document.getElementById('amenity-traj-chart');
        if (canvas && typeof Chart !== 'undefined') {
            // Destroy previous instance if any
            const prev = Chart.getChart(canvas);
            if (prev) prev.destroy();

            // Collect annotation points for catalyst opens years
            const milestoneYears = [...new Set(allItems.map(c => c.opens))].filter(y => y >= NOW && y <= endYear);
            const isDark = document.documentElement.classList.contains('dark');
            const gridCol = isDark ? 'rgba(255,255,255,0.07)' : 'rgba(0,0,0,0.06)';
            const textCol = isDark ? '#94a3b8' : '#64748b';

            new Chart(canvas, {
                type: 'line',
                data: {
                    labels: yearsRange,
                    datasets: [{
                        label: 'Est. Value',
                        data: trajValues,
                        borderColor: '#10b981',
                        backgroundColor: 'rgba(16,185,129,0.10)',
                        fill: true,
                        tension: 0.42,
                        pointRadius: yearsRange.map(y => milestoneYears.includes(y) ? 5 : 2),
                        pointBackgroundColor: yearsRange.map(y =>
                            milestoneYears.includes(y) ? '#f59e0b' : '#10b981'),
                        pointBorderWidth: 0,
                        borderWidth: 2,
                    }],
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            callbacks: {
                                label: ctx => `S$${ctx.parsed.y.toLocaleString()}`,
                                afterLabel: ctx => {
                                    const yr = ctx.label;
                                    const opens = allItems.filter(c => c.opens == yr).map(c => c.name);
                                    return opens.length ? `Opens: ${opens.join(', ')}` : '';
                                },
                            },
                        },
                    },
                    scales: {
                        x: { ticks: { color: textCol, font: { size: 10 } }, grid: { color: gridCol } },
                        y: {
                            ticks: {
                                color: textCol, font: { size: 10 },
                                callback: v => `S$${(v/1000).toFixed(0)}k`,
                            },
                            grid: { color: gridCol },
                        },
                    },
                },
            });
        }
    }
}

// ── Gap Analysis (Agent Tool) ────────────────────────────────
async function loadGapAnalysis() {
    const section = document.getElementById('gap-analysis-section');
    const body    = document.getElementById('gap-analysis-body');
    if (!section || !body) return;
    if (!currentUser || currentUser.account_type !== 'agent') return;
    section.classList.remove('hidden');

    body.innerHTML = '<p class="text-slate-400 text-sm animate-pulse">Loading gap data…</p>';
    try {
        const res  = await fetch('/api/agent/gap-analysis', {
            headers: { Authorization: `Bearer ${currentUser.token}` },
        });
        const data = await res.json();
        if (data.error) { body.innerHTML = `<p class="text-rose-500 text-sm">${data.error}</p>`; return; }
        const gaps = data.gaps || [];

        if (!gaps.length) {
            body.innerHTML = '<p class="text-slate-400 text-sm">No gap data available. Upload URA and HDB transaction data first.</p>';
            return;
        }

        const HIST = gaps[0]?.hist_gap || 45;
        body.innerHTML = `
        <table class="w-full text-sm">
            <thead>
                <tr class="text-left text-xs text-slate-400 border-b border-slate-100 dark:border-slate-700">
                    <th class="pb-3">Town</th>
                    <th class="pb-3">Segment</th>
                    <th class="pb-3 text-right">HDB Avg</th>
                    <th class="pb-3 text-right">Resale Condo Equiv</th>
                    <th class="pb-3 text-right">Gap %</th>
                    <th class="pb-3 text-right">vs Norm (${HIST}%)</th>
                    <th class="pb-3 pl-3">Signal</th>
                </tr>
            </thead>
            <tbody class="divide-y divide-slate-50 dark:divide-slate-700/50">
                ${gaps.map(g => {
                    const wide    = g.deviation > 10;
                    const narrow  = g.deviation < -10;
                    const badge   = wide   ? '<span class="text-xs font-bold text-emerald-700 bg-emerald-100 px-2 py-0.5 rounded-full">Seller Leverage ↑</span>'
                                  : narrow ? '<span class="text-xs font-bold text-rose-600 bg-rose-100 px-2 py-0.5 rounded-full">Gap Tight</span>'
                                  : '<span class="text-xs text-slate-400 bg-slate-100 px-2 py-0.5 rounded-full">Neutral</span>';
                    const gapCls  = wide ? 'text-emerald-600 font-bold' : narrow ? 'text-rose-500' : 'text-slate-600 dark:text-slate-300';
                    const devSign = g.deviation >= 0 ? '+' : '';
                    return `
                    <tr class="hover:bg-violet-50/40 dark:hover:bg-violet-900/10 transition-colors">
                        <td class="py-3 font-semibold text-slate-700 dark:text-slate-200">${g.town.replace(/_/g,' ').split(' ').map(w=>w[0]+w.slice(1).toLowerCase()).join(' ')}</td>
                        <td class="py-3"><span class="text-xs font-bold px-2 py-0.5 rounded ${g.segment==='CCR'?'bg-rose-100 text-rose-700':g.segment==='RCR'?'bg-amber-100 text-amber-700':'bg-sky-100 text-sky-700'}">${g.segment}</span></td>
                        <td class="py-3 text-right text-slate-600 dark:text-slate-300">S$${Math.round(g.hdb_avg/1000)}k</td>
                        <td class="py-3 text-right text-slate-600 dark:text-slate-300">S$${Math.round(g.rc_total/1000)}k</td>
                        <td class="py-3 text-right ${gapCls}">${g.gap_pct}%</td>
                        <td class="py-3 text-right text-xs ${g.deviation>=0?'text-emerald-600':'text-rose-500'}">${devSign}${g.deviation}%</td>
                        <td class="py-3 pl-3">${badge}</td>
                    </tr>`;
                }).join('')}
            </tbody>
        </table>
        <p class="text-xs text-slate-400 mt-3">Resale Condo Equiv = Resale condo avg PSF (same market segment) × HDB avg floor area. Gap % = how much more a comparable resale condo costs. "Seller Leverage" means the upgrade premium exceeds the historical norm (~60%) — use this to pitch sellers on their HDB's relative value vs the private resale market.</p>`;
    } catch (e) {
        body.innerHTML = `<p class="text-rose-500 text-sm">Failed to load: ${e.message}</p>`;
    }
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
                }, 800);
            });
        });
    }, 5500);
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

// ── SHAP Feature Contributions Chart ─────────────────────────────────────────
let _shapChart = null;
function renderShapChart(contributions) {
    const section = document.getElementById('shap-section');
    const canvas  = document.getElementById('shap-chart');
    if (!section || !canvas || !contributions || !contributions.length) return;

    section.classList.remove('hidden');
    const ctx = canvas.getContext('2d');
    if (_shapChart) { _shapChart.destroy(); _shapChart = null; }

    const isDark = document.documentElement.classList.contains('dark');
    const labels = contributions.map(c => c.name);
    const values = contributions.map(c => c.value);
    const colors = values.map(v => v >= 0 ? 'rgba(16,185,129,0.85)' : 'rgba(239,68,68,0.85)');
    const borderColors = values.map(v => v >= 0 ? '#059669' : '#dc2626');

    _shapChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                label: 'SHAP Value',
                data: values,
                backgroundColor: colors,
                borderColor: borderColors,
                borderWidth: 1.5,
                borderRadius: 6,
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        label: (ctx) => {
                            const v = ctx.parsed.x;
                            const impact = Math.abs(v * 100).toFixed(1);
                            return v > 0
                                ? `  ▲ Adds ~${impact}% to the price`
                                : `  ▼ Reduces price by ~${impact}%`;
                        },
                    },
                    backgroundColor: '#0f172a', padding: 12, cornerRadius: 10,
                    titleFont: { size: 11 }, bodyFont: { size: 12 },
                },
            },
            scales: {
                x: {
                    grid: { color: isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.05)' },
                    ticks: { font: { size: 10 }, color: isDark ? '#94a3b8' : '#64748b',
                             callback: v => v > 0 ? `+${v.toFixed(3)}` : v.toFixed(3) },
                },
                y: {
                    grid: { display: false },
                    ticks: { font: { size: 11 }, color: isDark ? '#e2e8f0' : '#334155' },
                },
            }
        }
    });

    // Plain-English explanation
    const explEl = document.getElementById('shap-explanation');
    if (explEl && contributions.length) {
        const _LABELS = {
            'floor_area_sqm':              'Floor area (size)',
            'floor_area':                  'Floor area (size)',
            'storey_mid':                  'Floor level',
            'floor_level':                 'Floor level',
            'remaining_lease_years':       'Remaining lease',
            'flat_age_years':              'Flat age',
            'block_rolling_psf_24m':       'Recent block transactions',
            'market_rolling_psf_12m':      'Overall market trend',
            'town_rolling_psf_12m':        'Town-level price trend',
            'town_flat_type_median_psf':   'Typical price in your area',
            'geo_rolling_psf_24m':         'Street-level price trend',
            'lat':                         'Location (north–south)',
            'lon':                         'Location (east–west)',
            'year':                        'Year of valuation',
            'sora':                        'Interest rate (SORA)',
            'town':                        'Town',
            'flat_type':                   'Flat type',
            'flat_model':                  'Flat model',
            'storey_psf_interaction':      'Floor–price interaction',
            'lease_psf_interaction':       'Lease–price interaction',
            'dist_nearest_mrt_km':         'Distance to MRT',
            'dist_nearest_school_km':      'Distance to school',
            'dist_nearest_hawker_km':      'Distance to hawker centre',
            'dist_nearest_health_km':      'Distance to clinic/hospital',
            'dist_nearest_park_km':        'Distance to park',
            'dist_nearest_community_km':   'Distance to community club',
        };
        const top = contributions.slice(0, 5);
        const upFactors   = top.filter(c => c.value > 0.002);
        const downFactors = top.filter(c => c.value < -0.002);

        const fmt = c => {
            const label = _LABELS[c.name] || c.name.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
            const pct   = (Math.abs(c.value) * 100).toFixed(1);
            return `<strong>${label}</strong> (~${pct}%)`;
        };

        let html = '';
        if (upFactors.length) {
            html += `<span class="text-emerald-600 font-semibold">↑ Pushing price up:</span> ${upFactors.map(fmt).join(', ')}. `;
        }
        if (downFactors.length) {
            html += `<span class="text-red-500 font-semibold">↓ Pushing price down:</span> ${downFactors.map(fmt).join(', ')}. `;
        }
        if (!html) {
            html = 'Multiple factors are contributing roughly equally to this valuation.';
        } else {
            html += 'The percentages show each factor\'s estimated impact on the final price.';
        }
        explEl.innerHTML = html;
    }
}

// ── What-If Sliders ───────────────────────────────────────────────────────────
let _whatIfBase = null;
function initWhatIfSliders({ floor, lease, basePrice, isCondo, isFreehold, maxFloor }) {
    const section = document.getElementById('whatif-section');
    if (!section) return;
    section.classList.remove('hidden');

    const effectiveMaxFloor = maxFloor || (isCondo ? 50 : 50);
    _whatIfBase = { floor, lease, basePrice, isCondo, isFreehold };

    const floorEl    = document.getElementById('wi-floor');
    const leaseEl    = document.getElementById('wi-lease');
    const leaseRow   = document.getElementById('wi-lease-row');
    const slidersDiv = document.getElementById('wi-sliders');
    const freeholdMsg = document.getElementById('wi-freehold-msg');

    // Show freehold message and hide sliders if freehold
    const showFreehold = isFreehold;
    if (freeholdMsg) freeholdMsg.classList.toggle('hidden', !showFreehold);
    if (slidersDiv)  slidersDiv.classList.toggle('hidden', showFreehold);

    if (!showFreehold) {
        // Floor slider — set max from actual building data
        if (floorEl) {
            floorEl.max   = effectiveMaxFloor;
            floorEl.value = Math.min(Math.round(floor), effectiveMaxFloor);
            document.getElementById('wi-floor-val').textContent = floorEl.value;
        }
        const floorMaxEl = document.getElementById('wi-floor-max');
        if (floorMaxEl) floorMaxEl.textContent = `(max: ${effectiveMaxFloor})`;

        // Lease slider — only for HDB leasehold
        const showLease = !isCondo && !isFreehold;
        if (leaseRow) leaseRow.style.display = showLease ? '' : 'none';
        if (leaseEl && showLease) {
            leaseEl.value = Math.round(lease);
            document.getElementById('wi-lease-val').textContent = Math.round(lease);
        }
    }

    document.getElementById('whatif-result').classList.add('hidden');
}

async function fetchFloorComps(block, town, flatType, road) {
    const section = document.getElementById('wi-floor-comps');
    const body    = document.getElementById('wi-floor-comps-body');
    const note    = document.getElementById('wi-floor-comps-note');
    if (!section || !body) return;
    try {
        const params = new URLSearchParams({ block, town, flat_type: flatType, road: road || '' });
        const res  = await fetch(`/api/floor-comps?${params}`);
        const data = await res.json();
        const comps = data.comps || [];
        if (!comps.length) return;

        section.classList.remove('hidden');
        const maxPsf = Math.max(...comps.map(c => c.avg_psf));
        body.innerHTML = comps.map(c => {
            const barW = Math.round((c.avg_psf / maxPsf) * 100);
            return `<div class="flex items-center gap-3 text-xs">
                <span class="w-16 text-slate-500 shrink-0">${c.storey}</span>
                <div class="flex-1 h-4 bg-slate-100 rounded-full overflow-hidden">
                    <div class="h-full bg-sky-400 rounded-full transition-all" style="width:${barW}%"></div>
                </div>
                <span class="w-20 text-right font-medium text-slate-700">S$${c.avg_psf.toLocaleString()}/sqft</span>
                <span class="w-12 text-right text-slate-400">${c.count} txn${c.count > 1 ? 's' : ''}</span>
            </div>`;
        }).join('');

        const src = data.source === 'sibling' ? 'nearby blocks in this estate' : 'this block';
        if (note) note.textContent = `Avg PSF by floor — data from ${src} (last 3 years). Higher floors typically transact at a premium.`;
    } catch { /* silent */ }
}

function updateWhatIf() {
    if (!_whatIfBase) return;

    const floor = parseInt(document.getElementById('wi-floor').value);
    const lease = parseInt(document.getElementById('wi-lease').value);

    document.getElementById('wi-floor-val').textContent = floor;
    if (document.getElementById('wi-lease-val')) {
        document.getElementById('wi-lease-val').textContent = lease;
    }

    // Extrapolation zone indicator
    const noteEl = document.getElementById('wi-extrapolation-note');
    const noteTextEl = document.getElementById('wi-extrapolation-text');
    const maxTf = window._maxTransactedFloor;
    if (noteEl) {
        const isExtrap = maxTf && floor > maxTf;
        noteEl.classList.toggle('hidden', !isExtrap);
        if (isExtrap && noteTextEl) {
            noteTextEl.textContent = `Beyond floor ${maxTf} — no recorded transactions at this level in this estate. Estimate is based on floor premium trend only.`;
        }
    }

    const baseFloor  = _whatIfBase.floor || 10;
    const baseLease  = _whatIfBase.lease || 65;
    const isCondo    = _whatIfBase.isCondo;
    const isFreehold = _whatIfBase.isFreehold;

    const floorRate = isCondo ? 0.012 : 0.006;
    const floorAdj  = 1 + floorRate * (floor - baseFloor);

    const showLease = !isCondo && !isFreehold;
    const leaseAdj  = showLease ? (1 + 0.008 * (lease - baseLease)) : 1;

    const adjusted = Math.round(_whatIfBase.basePrice * floorAdj * leaseAdj);
    const delta    = adjusted - _whatIfBase.basePrice;
    const pct      = ((delta / _whatIfBase.basePrice) * 100).toFixed(1);

    const resultEl = document.getElementById('whatif-result');
    resultEl.classList.remove('hidden');
    document.getElementById('wi-price').textContent = `S$${adjusted.toLocaleString()}`;
    const deltaEl = document.getElementById('wi-delta');
    deltaEl.textContent = `${delta >= 0 ? '+' : ''}S$${Math.abs(delta).toLocaleString()} (${delta >= 0 ? '+' : ''}${pct}%)`;
    deltaEl.className   = delta >= 0 ? 'text-lg font-semibold text-emerald-600' : 'text-lg font-semibold text-rose-500';
}

// ── MOP Leads (Agent Tool) ────────────────────────────────────────────────────
async function loadMopLeads(town) {
    const section = document.getElementById('mop-leads-section');
    const body    = document.getElementById('mop-leads-body');
    if (!section || !body) return;

    // Only show for agent accounts
    if (!currentUser || currentUser.account_type !== 'agent') return;
    section.classList.remove('hidden');

    const t = town || (document.getElementById('trend-town')?.value || '');
    body.innerHTML = '<p class="text-slate-400 text-sm animate-pulse">Loading MOP leads…</p>';

    try {
        const params = t ? `?town=${encodeURIComponent(t)}` : '';
        const res  = await fetch(`/api/agent/mop-leads${params}`, {
            headers: { Authorization: `Bearer ${currentUser.token}` },
        });
        const data = await res.json();
        if (data.error) { body.innerHTML = `<p class="text-rose-500 text-sm">${data.error}</p>`; return; }

        const leads = data.leads || [];
        if (!leads.length) {
            body.innerHTML = '<p class="text-slate-400 text-sm">No MOP leads found for this area.</p>';
            return;
        }

        // Propensity scoring: eligible + more units = higher score
        const month = new Date().getMonth() + 1;
        const seasonBonus = (month >= 4 && month <= 9) ? 1 : 0; // Q2–Q3 peak season
        const scoredLeads = leads.map(l => {
            let score = 0;
            if (l.status === 'eligible') score += 3;
            else if (l.months_to_mop <= 6) score += 2;
            else score += 1;
            if (l.unit_count >= 10) score += 2;
            else if (l.unit_count >= 5) score += 1;
            score += seasonBonus;
            return { ...l, propensity: score >= 5 ? 'High' : score >= 3 ? 'Medium' : 'Low' };
        });
        scoredLeads.sort((a, b) => (b.propensity === 'High' ? 3 : b.propensity === 'Medium' ? 2 : 1)
                                 - (a.propensity === 'High' ? 3 : a.propensity === 'Medium' ? 2 : 1));

        const highCount = scoredLeads.filter(l => l.propensity === 'High').length;

        body.innerHTML = `
        ${highCount > 0 ? `<div class="mb-4 text-xs bg-emerald-50 dark:bg-emerald-900/20 border border-emerald-100 dark:border-emerald-800/40 rounded-xl p-3 text-emerald-700 dark:text-emerald-300">
            <strong>${highCount} High-propensity lead${highCount > 1 ? 's' : ''}</strong> found — eligible now with strong unit count. Prioritise these for HDB-to-Condo upgrader pitches.
        </div>` : ''}
        <table class="w-full text-left text-sm">
            <thead>
                <tr class="text-slate-400 text-xs font-bold uppercase tracking-widest border-b border-slate-100 dark:border-slate-700">
                    <th class="pb-3 pl-2">Block / Street</th>
                    <th class="pb-3">Town</th>
                    <th class="pb-3">Flat Type</th>
                    <th class="pb-3">Units</th>
                    <th class="pb-3">MOP Year</th>
                    <th class="pb-3">Propensity</th>
                    <th class="pb-3 pr-2 text-right">Status</th>
                </tr>
            </thead>
            <tbody class="divide-y divide-slate-50 dark:divide-slate-700">
                ${scoredLeads.map(l => {
                    const propClass = l.propensity === 'High'
                        ? 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300'
                        : l.propensity === 'Medium'
                        ? 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300'
                        : 'bg-slate-100 text-slate-500 dark:bg-slate-700 dark:text-slate-400';
                    return `
                    <tr class="hover:bg-slate-50 dark:hover:bg-slate-700/30 transition-colors">
                        <td class="py-3 pl-2">
                            <p class="font-semibold dark:text-white">${l.block} ${l.street_name}</p>
                            <p class="text-xs text-slate-400">${l.storey_range}</p>
                        </td>
                        <td class="py-3 text-slate-600 dark:text-slate-300 text-xs">${l.town}</td>
                        <td class="py-3 text-slate-600 dark:text-slate-300">${l.flat_type}</td>
                        <td class="py-3 text-slate-600 dark:text-slate-300">${l.unit_count}</td>
                        <td class="py-3 text-slate-600 dark:text-slate-300">${l.mop_year}</td>
                        <td class="py-3">
                            <span class="text-xs px-2 py-1 rounded-full font-semibold ${propClass}">${l.propensity}</span>
                        </td>
                        <td class="py-3 pr-2 text-right">
                            <span class="text-xs px-2 py-1 rounded-full font-semibold ${l.status === 'eligible'
                                ? 'bg-violet-100 text-violet-700 dark:bg-violet-900/30 dark:text-violet-300'
                                : 'bg-slate-100 text-slate-500 dark:bg-slate-700 dark:text-slate-400'}">
                                ${l.status === 'eligible' ? 'Eligible Now' : `${l.months_to_mop}mo left`}
                            </span>
                        </td>
                    </tr>`;
                }).join('')}
            </tbody>
        </table>
        <p class="text-xs text-slate-400 mt-4">Showing ${leads.length} units. Propensity = eligibility × unit density × selling season. MOP = 5 years from lease commencement.</p>
        `;
    } catch (e) {
        body.innerHTML = `<p class="text-rose-500 text-sm">Failed to load leads: ${e.message}</p>`;
    }
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
        // Load trusted devices when 2FA is enabled
        loadTrustedDevices();
    } else {
        icon.className  = 'w-12 h-12 rounded-full bg-slate-200 dark:bg-slate-700 flex items-center justify-center';
        icon.innerHTML  = '<i data-lucide="shield" class="w-6 h-6 text-slate-400"></i>';
        title.textContent = '2FA Not Enabled';
        title.className   = 'font-bold text-slate-900 dark:text-white';
        desc.textContent  = 'Your account is protected by password only.';
        btnEn.classList.remove('hidden');
        btnDis.classList.add('hidden');
        document.getElementById('trusted-devices-section')?.classList.add('hidden');
    }
    lucide.createIcons();
}

async function loadTrustedDevices() {
    const section = document.getElementById('trusted-devices-section');
    const list    = document.getElementById('trusted-devices-list');
    if (!section || !list || !currentUser) return;
    section.classList.remove('hidden');
    list.innerHTML = '<p class="text-xs text-slate-400 animate-pulse">Loading…</p>';
    try {
        const res  = await fetch(`/api/2fa/trusted-devices?user_id=${currentUser.id}`);
        const data = await res.json();
        if (data.error || !data.devices) {
            list.innerHTML = '<p class="text-xs text-slate-400">Unable to load devices.</p>';
            return;
        }
        if (!data.devices.length) {
            list.innerHTML = '<p class="text-xs text-slate-400">No trusted devices — you haven\'t checked "Remember this device" after a 2FA login yet.</p>';
            return;
        }
        list.innerHTML = data.devices.map(d => {
            const added   = d.created_at ? new Date(d.created_at).toLocaleDateString('en-SG', { day: 'numeric', month: 'short', year: 'numeric' }) : '—';
            const expires = d.expires_at ? new Date(d.expires_at).toLocaleDateString('en-SG', { day: 'numeric', month: 'short', year: 'numeric' }) : '—';
            return `<div class="flex items-center justify-between p-3 rounded-xl bg-slate-50 dark:bg-slate-700/50 border border-slate-100 dark:border-slate-700">
                <div class="flex items-center gap-2.5">
                    <i data-lucide="monitor" class="w-4 h-4 text-slate-400 flex-shrink-0"></i>
                    <div>
                        <p class="text-xs font-semibold text-slate-700 dark:text-slate-200">${d.ip_address}</p>
                        <p class="text-[10px] text-slate-400">Added ${added} · Expires ${expires}</p>
                    </div>
                </div>
                <span class="text-[10px] font-bold px-2 py-0.5 rounded-full bg-emerald-100 dark:bg-emerald-900/40 text-emerald-700 dark:text-emerald-300">Active</span>
            </div>`;
        }).join('');
        lucide.createIcons();
    } catch {
        list.innerHTML = '<p class="text-xs text-rose-500">Failed to load devices.</p>';
    }
}

async function revokeAllDevices() {
    if (!currentUser) return;
    if (!confirm('Revoke all trusted devices? You will need to re-verify with 2FA on all devices next time you sign in.')) return;
    try {
        const res  = await fetch('/api/2fa/revoke-all', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ user_id: currentUser.id }),
        });
        const data = await res.json();
        if (data.error) { showToast(data.error, true); return; }
        showToast(`${data.revoked || 0} trusted device${data.revoked !== 1 ? 's' : ''} revoked.`);
        loadTrustedDevices();
    } catch {
        showToast('Failed to revoke devices.', true);
    }
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


