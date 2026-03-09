// View Router
function showView(viewId) {
    const target = document.getElementById('view-' + viewId);
    if (!target) return;

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
    
    window.scrollTo({ top: 0, behavior: 'smooth' });
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
        
        // Update UI
        document.getElementById('output-price').innerText = `S$${data.estimated_value.toLocaleString()}`;
        document.getElementById('output-confidence').innerText = `${data.confidence}%`;
        document.getElementById('map-title').innerText = document.getElementById('display-address').innerText;
        
        // Inject factors
        const list = document.getElementById('factors-list');
        list.innerHTML = data.factors.map(f => `
            <div class="space-y-3">
                <div class="flex items-center justify-between font-bold">
                    <span class="text-slate-700">${f.name}</span>
                    <span class="text-blue-600">${f.score}%</span>
                </div>
                <div class="w-full h-3 bg-slate-100 rounded-full overflow-hidden">
                    <div class="h-full bg-blue-600 rounded-full transition-all duration-1000" style="width: 0%" id="bar-${f.name.replace(/\s+/g, '')}"></div>
                </div>
            </div>
        `).join('');
        
        togglePredictView('output');
        
        // Animate bars
        setTimeout(() => {
            data.factors.forEach(f => {
                const bar = document.getElementById(`bar-${f.name.replace(/\s+/g, '')}`);
                if (bar) bar.style.width = f.score + '%';
            });
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

    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
    content.classList.add('active');
    
    document.querySelectorAll('.setting-tab').forEach(t => t.classList.remove('text-slate-900', 'border-slate-900'));
    document.querySelectorAll('.setting-tab').forEach(t => t.classList.add('text-slate-400'));
    
    btn.classList.remove('text-slate-400');
    btn.classList.add('text-slate-900', 'border-slate-900');
}

function showAdminTab(tabId) {
    const content = document.getElementById('admin-tab-' + tabId);
    const btn = document.getElementById('admin-tab-btn-' + tabId);
    if (!content || !btn) return;

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
    
    lucide.createIcons();
}

// Charts
let trendChart;
function initTrendChart() {
    const canvas = document.getElementById('trendChart');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    if (trendChart) trendChart.destroy();
    
    const gradient = ctx.createLinearGradient(0, 0, 0, 400);
    gradient.addColorStop(0, 'rgba(37, 99, 235, 0.1)');
    gradient.addColorStop(1, 'rgba(37, 99, 235, 0)');

    trendChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: ['Sep 25', 'Oct 25', 'Nov 25', 'Dec 25', 'Jan 26', 'Feb 26'],
            datasets: [{
                label: 'Avg Price (S$)',
                data: [415000, 428000, 432000, 445000, 458000, 465000],
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

let adminTypeChart;
function initAdminTypeChart() {
    const canvas = document.getElementById('adminTypeChart');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    if (adminTypeChart) adminTypeChart.destroy();

    adminTypeChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['HDB 4-Room', 'Condo', 'HDB 5-Room', 'Landed', 'HDB 3-Room'],
            datasets: [{
                label: 'Predictions',
                data: [8420, 6540, 5210, 4100, 3274],
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

// Admin Stats
async function fetchAdminStats() {
    try {
        const response = await fetch('/api/stats');
        const data = await response.json();
        document.getElementById('admin-users').innerText = data.total_users.toLocaleString();
        document.getElementById('admin-predictions').innerText = data.total_predictions.toLocaleString();
        document.getElementById('admin-db').innerText = data.db_size;
        
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

function handleSignIn(e) {
    if (e) e.preventDefault();
    // Mock sign in
    currentUser = {
        name: "John Tan",
        avatar: "https://picsum.photos/seed/johntan/100/100"
    };
    updateAuthUI();
    showView('home');
}

function handleRegister(e) {
    if (e) e.preventDefault();
    // Mock register
    const nameInput = document.querySelector('#view-register input[placeholder="Full Name"]');
    const name = nameInput ? nameInput.value : "New User";
    currentUser = {
        name: name,
        avatar: `https://picsum.photos/seed/${name}/100/100`
    };
    updateAuthUI();
    showView('home');
}

function handleLogout() {
    currentUser = null;
    updateAuthUI();
    showView('home');
}

function updateAuthUI() {
    const authButtons = document.getElementById('nav-auth-buttons');
    const userProfile = document.getElementById('nav-user-profile');
    const userName = document.getElementById('nav-user-name');
    const userAvatar = document.getElementById('nav-user-avatar');

    if (currentUser) {
        authButtons.classList.add('hidden');
        userProfile.classList.remove('hidden');
        userProfile.classList.add('flex');
        userName.innerText = currentUser.name;
        userAvatar.src = currentUser.avatar;
    } else {
        authButtons.classList.remove('hidden');
        userProfile.classList.add('hidden');
        userProfile.classList.remove('flex');
    }
    lucide.createIcons();
}

// Initial Icons
lucide.createIcons();
