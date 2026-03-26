// View Router
function showView(viewId) {
    const target = document.getElementById('view-' + viewId);
    if (!target) return;
    localStorage.setItem('currentView', viewId);

    // block admin page if user is not admin
    if (viewId === 'admin') {
        const isAdmin = currentUser && currentUser.role === 'admin';
        if (!isAdmin) {
            alert('Access denied. Admins only.');
            showView(currentUser ? 'home' : 'signin');
            return;
        }
    }

    // block settings page if not logged in
    if (viewId === 'setting' && !currentUser) {
        alert('Please sign in first.');
        showView('signin');
        return;
    }

    // Hide all views
    document.querySelectorAll('.view-section').forEach(s => s.classList.remove('active'));
    
    // Show target view
    target.classList.add('active');
    
    // Update Navigation
    document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
    const activeLink = document.getElementById('nav-' + viewId);
    if (activeLink) activeLink.classList.add('active');

    // View specific init
    if (viewId === 'trend') {
        setTimeout(() => { initTrendChart(); renderTrendNews(currentNeighbourhood); runABSDSimulation(); }, 100);
    }
    if (viewId === 'map') {
        if (lastMapPostal) {
            document.getElementById('map-postal-input').value = lastMapPostal;
            setTimeout(() => initMapForPostal(lastMapPostal), 100);
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
    const postal = document.getElementById('input-postal').value;
    if (!postal || postal.length < 6) {
        alert('Please enter a valid 6-digit postal code');
        return;
    }

    const placeholder = document.getElementById('postal-placeholder');
    const details = document.getElementById('postal-details');
    
    // Mock search
    placeholder.classList.add('hidden');
    details.classList.remove('hidden');
    
    // Mock data based on postal
    if (postal === '238801') {
        document.getElementById('display-address').innerText = "1 St. Martin's Drive";
        document.getElementById('display-building').innerText = "The Sail @ Marina Bay";
    } else if (postal === '560123') {
        document.getElementById('display-address').innerText = "123 Ang Mo Kio Ave 3";
        document.getElementById('display-building').innerText = "AMK Hub Residences";
    } else {
        document.getElementById('display-address').innerText = "342 Clementi Ave 5";
        document.getElementById('display-building').innerText = "Clementi Heights";
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
        // Point to local Python backend
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

        // --- Main price ---
        document.getElementById('output-price').innerText = `S$${data.estimated_value.toLocaleString()}`;
        document.getElementById('output-confidence').innerText = `${data.confidence}%`;

        // --- Trend badge ---
        const trendEl    = document.getElementById('output-trend');
        const trendBadge = document.getElementById('output-trend-badge');
        const trendIcon  = document.getElementById('output-trend-icon');
        if (trendEl) trendEl.innerText = data.market_trend || '+2.4%';
        if (trendBadge && trendIcon) {
            const isUp = (data.trend_direction || 'up') === 'up';
            trendBadge.className = `flex items-center gap-1 px-3 py-1 rounded-lg text-sm font-bold ${isUp ? 'bg-emerald-50 text-emerald-600' : 'bg-rose-50 text-rose-600'}`;
            trendIcon.setAttribute('data-lucide', isUp ? 'trending-up' : 'trending-down');
        }

        // --- Market state & date ---
        const mktEl = document.getElementById('output-market-state');
        if (mktEl) mktEl.innerText = data.market_state || 'Active';
        const dateEl = document.getElementById('output-valuation-date');
        if (dateEl) {
            const now = new Date();
            dateEl.innerText = now.toLocaleString('en-SG', { month: 'short', year: 'numeric' });
        }

        // --- AI insight & recommendation ---
        const insightEl = document.getElementById('output-insight');
        if (insightEl) insightEl.innerText = data.insight || '';
        const recEl = document.getElementById('output-recommendation');
        if (recEl) recEl.innerText = data.recommendation || '';

        // --- Map title ---
        const mapTitle = document.getElementById('map-title');
        if (mapTitle) mapTitle.innerText = document.getElementById('display-address').innerText;

        // --- Factors ---
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

        // Animate bars + re-init icons after DOM update
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
        // Fallback for demo if backend is not running
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

function loadMap() {
    const postal = document.getElementById('map-postal-input').value.trim().replace(/\s/g,'').padStart(6,'0');
    if (!postal || postal.length < 6 || !/^\d{6}$/.test(postal)) {
        alert('Please enter a valid 6-digit Singapore postal code.');
        return;
    }
    lastMapPostal = postal;
    initMapForPostal(postal);
}

async function initMapForPostal(postal) {
    const placeholder = document.getElementById('map-placeholder');
    const mapDiv      = document.getElementById('leaflet-map');
    const addrBar     = document.getElementById('map-address-bar');

    placeholder.innerHTML = `<div class="flex flex-col items-center gap-3 text-slate-400">
        <svg class="w-8 h-8 animate-spin" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"/><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z"/></svg>
        <p class="text-sm font-medium">Geocoding postal code...</p></div>`;
    placeholder.classList.remove('hidden');
    mapDiv.classList.add('hidden');

    try {
        // OneMap Singapore API — accurate postal code geocoding
        const geo = await fetch(`https://www.onemap.gov.sg/api/common/elastic/search?searchVal=${postal}&returnGeom=Y&getAddrDetails=Y&pageNum=1`);
        const geoData = await geo.json();

        if (!geoData.results || !geoData.results.length) throw new Error('Not found');

        const result = geoData.results[0];
        const lat = parseFloat(result.LATITUDE);
        const lng = parseFloat(result.LONGITUDE);
        const displayName = result.ADDRESS || result.BUILDING || postal;

        document.getElementById('map-address-text').innerText = displayName;
        document.getElementById('map-district-text').innerText = `Postal Code: ${postal} · Singapore`;
        addrBar.classList.remove('hidden');
        addrBar.classList.add('flex');

        placeholder.classList.add('hidden');
        mapDiv.classList.remove('hidden');

        if (!mapInstance) {
            mapInstance = L.map('leaflet-map', { zoomControl: true });
            // OneMap SG official tiles
            L.tileLayer('https://maps-{s}.onemap.sg/v3/maps/Default/{z}/{x}/{y}.png', {
                attribution: '<img src="https://www.onemap.gov.sg/web-assets/images/logo/om_logo.png" style="height:20px;width:20px;"/> <a href="https://www.onemap.gov.sg/" target="_blank">OneMap</a> &copy; contributors, <a href="https://www.sla.gov.sg/" target="_blank">Singapore Land Authority</a>',
                maxZoom: 19, subdomains: 'abcd'
            }).addTo(mapInstance);
            // Delay setView so the div has time to render with dimensions
            setTimeout(() => { mapInstance.invalidateSize(); mapInstance.setView([lat, lng], 16); }, 200);
        } else {
            mapLayers.forEach(l => mapInstance.removeLayer(l));
            mapLayers = [];
            setTimeout(() => { mapInstance.invalidateSize(); mapInstance.setView([lat, lng], 16); }, 200);
        }

        // Property marker
        const propIcon = L.divIcon({
            html: `<div style="width:20px;height:20px;background:#2563eb;border:3px solid white;border-radius:50%;box-shadow:0 2px 8px rgba(37,99,235,0.5)"></div>`,
            iconSize: [20, 20], iconAnchor: [10, 10], className: ''
        });
        const propMarker = L.marker([lat, lng], { icon: propIcon })
            .bindPopup(`<b>📍 Subject Property</b><br>${displayName}`)
            .addTo(mapInstance);
        mapLayers.push(propMarker);

        // Load amenities via backend (OneMap + Overpass, cached in DB)
        await loadAmenities(lat, lng, postal);

    } catch (err) {
        placeholder.innerHTML = `<div class="text-center text-slate-400 text-sm"><p class="font-medium">Could not locate postal code</p><p class="text-xs mt-1">Try a different postal code</p></div>`;
        placeholder.classList.remove('hidden');
        mapDiv.classList.add('hidden');
    }
}

async function loadAmenities(lat, lng, postal) {
    const amenityCards = document.getElementById('amenity-cards');
    amenityCards.innerHTML = `<div class="bg-white rounded-2xl border border-slate-200 p-6 text-center text-slate-400 text-sm shadow-sm">
        <p class="font-medium">Loading nearby amenities...</p></div>`;

    try {
        const params = new URLSearchParams({ lat, lng });
        if (postal) params.append('postal', postal);
        const res = await fetch(`/api/amenities?${params}`);
        if (!res.ok) throw new Error('API error');
        const data = await res.json();
        const cats = data.categories;

        // Add markers to map
        Object.values(cats).forEach(cat => {
            cat.items.forEach(item => {
                const marker = L.circleMarker([item.lat, item.lng], {
                    radius: 7, fillColor: cat.color, color: '#fff',
                    weight: 2, opacity: 1, fillOpacity: 0.9
                }).bindPopup(`<b>${cat.icon} ${item.name}</b><br>${item.dist} km · ${item.travel}`);
                marker.addTo(mapInstance);
                mapLayers.push(marker);
            });
        });

        // Render sidebar cards
        const hasAny = Object.values(cats).some(c => c.items.length > 0);
        if (!hasAny) {
            amenityCards.innerHTML = `<div class="bg-white rounded-2xl border border-slate-200 p-6 text-center shadow-sm">
                <p class="text-slate-400 text-sm">No amenities found nearby</p></div>`;
            return;
        }

        amenityCards.innerHTML = Object.values(cats).filter(c => c.items.length > 0).map(cat => `
            <div class="bg-white rounded-2xl border border-slate-200 p-5 shadow-sm">
                <div class="flex items-center gap-3 mb-4">
                    <div class="w-8 h-8 rounded-xl flex items-center justify-center" style="background:${cat.color}22">
                        <i data-lucide="${cat.lucide}" class="w-4 h-4" style="color:${cat.color}"></i>
                    </div>
                    <h3 class="text-sm font-bold">${cat.label}</h3>
                </div>
                <div class="space-y-3">
                    ${cat.items.map(item => `
                        <div class="flex justify-between items-start gap-2">
                            <div>
                                <p class="text-xs font-semibold leading-snug">${item.name}</p>
                                <p class="text-[10px] text-slate-400 mt-0.5">${item.travel}</p>
                            </div>
                            <span class="text-[10px] font-bold text-slate-400 shrink-0">${item.dist} km</span>
                        </div>
                    `).join('')}
                </div>
            </div>
        `).join('');

        lucide.createIcons();
    } catch (err) {
        amenityCards.innerHTML = `<div class="bg-white rounded-2xl border border-slate-200 p-6 text-center shadow-sm">
            <p class="text-slate-400 text-sm">Could not load amenities</p></div>`;
    }
}

function getDistKm(lat1, lng1, lat2, lng2) {
    const R = 6371;
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLng = (lng2 - lng1) * Math.PI / 180;
    const a = Math.sin(dLat/2)**2 + Math.cos(lat1*Math.PI/180)*Math.cos(lat2*Math.PI/180)*Math.sin(dLng/2)**2;
    return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
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

function renderTrendNews(neighbourhood) {
    const list = document.getElementById('news-list');
    const subtitle = document.getElementById('news-subtitle');
    if (subtitle) subtitle.innerText = `Latest headlines for ${neighbourhood}`;
    const articles = NEIGHBOURHOOD_NEWS[neighbourhood] || [];
    const tagColors = {
        blue: 'bg-blue-50 text-blue-600',
        emerald: 'bg-emerald-50 text-emerald-600',
        purple: 'bg-purple-50 text-purple-600',
        amber: 'bg-amber-50 text-amber-600',
        rose: 'bg-rose-50 text-rose-600',
    };
    list.innerHTML = articles.map(a => `
        <a href="${a.url}" target="_blank" rel="noopener noreferrer" class="flex items-start gap-4 p-4 rounded-2xl border border-slate-100 hover:bg-slate-50 transition-colors cursor-pointer group no-underline block">
            <span class="px-2 py-1 rounded-lg text-[10px] font-bold shrink-0 ${tagColors[a.color] || tagColors.blue}">${a.tag}</span>
            <div class="flex-1 min-w-0">
                <p class="text-sm font-semibold leading-snug group-hover:text-blue-600 transition-colors">${a.headline}</p>
                <div class="flex items-center gap-2 mt-1">
                    <p class="text-xs text-slate-400">${a.source} · ${a.date}</p>
                    <i data-lucide="external-link" class="w-3 h-3 text-slate-300 group-hover:text-blue-400 transition-colors"></i>
                </div>
            </div>
        </a>
    `).join('');
    lucide.createIcons();
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
    // Singapore Buyer's Stamp Duty (2023 revised)
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

// Charts
let trendChart;
let currentRange = '6m';

// Neighbourhood base prices for chart simulation
const NEIGHBOURHOOD_BASE = {
    'Clementi':   { base: 430000, growth: 0.018 },
    'Queenstown': { base: 520000, growth: 0.022 },
    'Hougang':    { base: 380000, growth: 0.015 },
    'Toa Payoh':  { base: 460000, growth: 0.020 },
    'Marina Bay': { base: 1800000, growth: 0.031 },
};

function generateNeighbourhoodPrices(neighbourhood, range) {
    const cfg = NEIGHBOURHOOD_BASE[neighbourhood] || NEIGHBOURHOOD_BASE['Clementi'];
    const months = range === '6m' ? 6 : range === '1y' ? 12 : 24;
    const labels = [], prices = [];
    const now = new Date();
    let p = cfg.base * Math.pow(1 - cfg.growth / 12, months);
    for (let i = months; i >= 0; i--) {
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
        const res = await fetch(`/api/trend`);
        if (!res.ok) throw new Error();
        const data = await res.json();
        if (data.trend_data && data.trend_data.length) {
            // Blend API data with neighbourhood scaling
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

    const tickColor = isDark ? '#93C5FD' : '#64748B';
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
const HOME_NEWS = [
    { headline: 'HDB resale prices climb 2.5% in Q1 2026, led by mature estates and million-dollar flats', source: 'Straits Times', date: 'Mar 2026', tag: 'Resale', color: 'blue', url: 'https://www.straitstimes.com/search?q=hdb+resale+prices+2026' },
    { headline: 'Singapore private home prices up 1.8% in Q1 2026 amid firm demand and limited supply', source: 'Business Times', date: 'Mar 2026', tag: 'Private', color: 'purple', url: 'https://www.businesstimes.com.sg/search?q=singapore+private+home+prices+2026' },
    { headline: 'MAS holds property cooling measures steady; analysts forecast gradual appreciation', source: 'CNA', date: 'Feb 2026', tag: 'Policy', color: 'amber', url: 'https://www.channelnewsasia.com/search?q=singapore+property+cooling+measures+2026' },
    { headline: 'BTO supply in 2026 to reach 19,600 flats across 9 towns — HDB confirms schedule', source: 'EdgeProp', date: 'Feb 2026', tag: 'BTO', color: 'emerald', url: 'https://www.edgeprop.sg/property-news?q=hdb+bto+2026+launch' },
    { headline: 'Rental market softens as supply surges — rents expected to ease 5–8% through 2026', source: 'PropertyGuru', date: 'Jan 2026', tag: 'Rental', color: 'rose', url: 'https://www.propertyguru.com.sg/property-guides?q=singapore+rental+market+2026' },
    { headline: 'Greater Southern Waterfront masterplan could unlock S$100B in new developments', source: '99.co', date: 'Jan 2026', tag: 'Planning', color: 'blue', url: 'https://www.99.co/singapore/insider/greater-southern-waterfront' },
];

function renderHomeNews() {
    const list = document.getElementById('home-news-list');
    if (!list) return;
    const tagColors = {
        blue: 'bg-blue-50 text-blue-700',
        emerald: 'bg-emerald-50 text-emerald-700',
        purple: 'bg-purple-50 text-purple-700',
        amber: 'bg-amber-50 text-amber-700',
        rose: 'bg-rose-50 text-rose-700',
    };
    list.innerHTML = HOME_NEWS.map(a => `
        <a href="${a.url}" target="_blank" rel="noopener noreferrer"
           class="flex items-start gap-4 p-4 rounded-2xl border border-slate-100 hover:bg-slate-50 transition-colors cursor-pointer group no-underline block">
            <span class="px-2 py-1 rounded-lg text-[10px] font-bold shrink-0 ${tagColors[a.color] || tagColors.blue}">${a.tag}</span>
            <div class="flex-1 min-w-0">
                <p class="text-sm font-semibold leading-snug group-hover:text-blue-600 transition-colors">${a.headline}</p>
                <div class="flex items-center gap-2 mt-1">
                    <p class="text-xs text-slate-400">${a.source} · ${a.date}</p>
                    <i data-lucide="external-link" class="w-3 h-3 text-slate-300 group-hover:text-blue-400 transition-colors"></i>
                </div>
            </div>
        </a>
    `).join('');
    lucide.createIcons();
}

document.addEventListener('DOMContentLoaded', () => {
    initTrendChart();
    renderTrendNews(currentNeighbourhood);
    runABSDSimulation();
    renderHomeNews();
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

        // fallback
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



// Admin Stats
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

// Auth Logic
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
    const savedTheme = localStorage.getItem('theme') ?? 'dark';
    const toggle = document.getElementById('darkModeToggle');

    if (savedTheme !== 'light') {
        document.documentElement.classList.add('dark');
        if (toggle) toggle.checked = true;
    } else {
        document.documentElement.classList.remove('dark');
        if (toggle) toggle.checked = false;
    }
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

function clearAuthForms() {
    document.querySelectorAll('#view-signin input, #view-register input').forEach(input => {
        input.value = '';
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

        // show admin buttons only if admin
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

// ===========================
// Profile Photo Upload
// ===========================

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

    // if saved page is settings but user is logged out, go signin
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



