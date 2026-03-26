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
    if (viewId === 'trend') setTimeout(initTrendChart, 100);

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

// Charts
let trendChart;
let currentRange = '6m'; 

async function initTrendChart(range = currentRange) {
    const canvas = document.getElementById('trendChart');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    if (trendChart) trendChart.destroy();

    const gradient = ctx.createLinearGradient(0, 0, 0, 400);
    gradient.addColorStop(0, 'rgba(37, 99, 235, 0.1)');
    gradient.addColorStop(1, 'rgba(37, 99, 235, 0)');

    let labels = [];
    let prices = [];

    try {
        const res = await fetch(`/api/trend?range=${range}`);
        if (!res.ok) throw new Error('Trend API failed');
        const data = await res.json();
        

        labels = data.trend_data.map(item => item.month);
        prices = data.trend_data.map(item => item.price);
        
        loadComparableTable(data);
        loadNearestSale(data);
        
    } catch (err) {
        console.error('Trend API failed, using fallback:', err);

        // fallback
        labels = ['Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb'];
        prices = [415000, 428000, 432000, 445000, 458000, 465000];
    }

    trendChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'Avg Price (S$)',
                data: prices,
                borderColor: '#0F172A',
                borderWidth: 4,
                fill: true,
                backgroundColor: gradient,
                tension: 0.4,
                pointRadius: 0,
                pointHoverRadius: 8,
                pointHoverBackgroundColor: '#2563EB',
                pointHoverBorderColor: '#FFF',
                pointHoverBorderWidth: 4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { intersect: false, mode: 'index' },
            plugins: { 
                legend: { display: false },
                tooltip: {
                    backgroundColor: '#0F172A',
                    padding: 16,
                    titleFont: { size: 14, weight: 'bold' },
                    bodyFont: { size: 14 },
                    cornerRadius: 12,
                    displayColors: false
                }
            },
            scales: {
                y: { 
                    beginAtZero: false, 
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

document.addEventListener('DOMContentLoaded', () => {
    initTrendChart();
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
    const savedTheme = localStorage.getItem('theme');
    const toggle = document.getElementById('darkModeToggle');

    if (savedTheme === 'dark') {
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



