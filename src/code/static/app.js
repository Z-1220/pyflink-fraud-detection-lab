const ws = new WebSocket(`ws://${location.host}/ws`);

let totalAmount = 0, totalCount = 0, alarmCount = 0;
const categoryMap = new Map();
const trendData = [];
const alarmList = [];

const categoryChart = echarts.init(document.getElementById('categoryChart'));
const trendChart = echarts.init(document.getElementById('trendChart'));

ws.onopen = () => console.log('WebSocket 已连接');
ws.onerror = (err) => console.error('WebSocket 错误', err);
ws.onclose = () => console.warn('WebSocket 已断开');

ws.onmessage = (event) => {
    try {
        const msg = JSON.parse(event.data);
        switch (msg.topic) {
            case 'total_amount_and_count_events':
                updateTotals(msg.data);
                break;
            case 'window_count_and_amount_events':
                updateTrend(msg.data);
                break;
            case 'category_aggregated_events':
                updateCategory(msg.data);
                break;
            case 'alarm_events':
                addAlarm(msg.data);
                break;
        }
    } catch (e) {
        console.error('消息处理错误', e);
    }
};

function updateTotals(data) {
    totalAmount = data.total_amount || 0;
    totalCount = data.transaction_count || 0;
    document.getElementById('totalAmount').innerText = totalAmount.toFixed(2);
    document.getElementById('totalCount').innerText = totalCount;
}

function updateTrend(data) {
    document.getElementById('windowAmount').innerText = data.total_amount?.toFixed(2) || 0;
    trendData.push({
        time: new Date(data.window_start).toLocaleTimeString(),
        amount: data.total_amount,
        count: data.transaction_count
    });
    if (trendData.length > 60) trendData.shift();
    renderTrendChart();
}

function updateCategory(data) {
    const prev = categoryMap.get(data.category) || { amount: 0, count: 0 };
    categoryMap.set(data.category, {
        amount: prev.amount + data.total_amount,
        count: prev.count + data.transaction_count
    });
    renderCategoryChart();
}

function addAlarm(data) {
    alarmCount++;
    document.getElementById('alarmCount').innerText = alarmCount;
    alarmList.unshift(data);
    if (alarmList.length > 200) alarmList.pop();
    document.getElementById('filterCount').innerText = alarmList.length + ' 条';
    renderAlarmTable(alarmList);
}

/* ========== 筛选 ========== */
async function applyFilter() {
    const keyword = document.getElementById('filterInput').value.trim();
    const type = document.getElementById('typeFilter').value;

    let url = `/api/alerts/history?limit=500`;
    if (type) url += `&alert_type=${encodeURIComponent(type)}`;
    if (keyword) url += `&keyword=${encodeURIComponent(keyword)}`;

    try {
        const resp = await fetch(url);
        const data = await resp.json();
        if (Array.isArray(data)) {
            document.getElementById('filterCount').innerText = data.length + ' 条';
            renderAlarmTable(data);
        } else if (data && data.error) {
            document.getElementById('filterCount').innerText = '查询失败';
        }
    } catch (e) {
        console.error('筛选查询失败', e);
        document.getElementById('filterCount').innerText = '查询失败';
    }
}

function alertRowClass(alertType) {
    switch (alertType) {
        case 'HIGH_FREQUENCY':       return 'high-freq';
        case 'LARGE_AMOUNT':         return 'large-amount';
        case 'CONTINUOUS_INCREASE':  return 'increase';
        case 'FAILED_SURGE':         return 'failed-surge';
        case 'IP_SHARING':           return 'ip-sharing';
        default:                     return '';
    }
}

function renderAlarmTable(alarms) {
    const tbody = document.getElementById('alarmBody');
    const rows = alarms || alarmList;
    tbody.innerHTML = rows.slice(0, 200).map(a => {
        const time = a.alert_time ? new Date(a.alert_time).toLocaleTimeString() : '';
        const value = a.amount != null ? Number(a.amount).toFixed(2)
                    : (a.transaction_count || a.user_count || '-');
        return `<tr class="${alertRowClass(a.alert_type)}">
            <td>${time}</td><td>${a.alert_type}</td><td>${a.user_id}</td>
            <td>${a.transaction_id || '-'}</td><td>${value}</td><td>${escHtml(a.details || '')}</td>
        </tr>`;
    }).join('');
}

/* ========== 风险用户排行 ========== */
async function fetchTopRiskyUsers() {
    const container = document.getElementById('topUsersList');
    try {
        const resp = await fetch('/api/top-risky-users?limit=5');
        const data = await resp.json();
        if (data && data.error) {
            container.innerHTML = `<span class="placeholder">查询失败: ${escHtml(data.error)}</span>`;
            return;
        }
        if (!Array.isArray(data)) {
            container.innerHTML = '<span class="placeholder">数据格式异常</span>';
            return;
        }
        if (data.length === 0) {
            container.innerHTML = '<span class="placeholder">暂无告警数据</span>';
            return;
        }
        container.innerHTML = data.map((u, i) => {
            const rankClass = i === 0 ? 'r1' : i === 1 ? 'r2' : i === 2 ? 'r3' : 'rn';
            return `<div class="user-rank-item">
                <span class="rank ${rankClass}">#${i + 1}</span>
                <span class="uname">${escHtml(u.user_name)}</span>
                <span class="ucount">${u.alert_count} 次告警</span>
            </div>`;
        }).join('');
    } catch (e) {
        console.error('获取风险用户排行失败', e);
        container.innerHTML = '<span class="placeholder">网络请求失败，15秒后重试</span>';
    }
}

function escHtml(s) {
    const div = document.createElement('div');
    div.textContent = s;
    return div.innerHTML;
}

/* ========== 图表 ========== */
function renderCategoryChart() {
    const data = Array.from(categoryMap.entries()).map(([k, v]) => ({ name: k, value: v.amount }));
    categoryChart.setOption({
        title: { text: '商品类别交易分布', left: 'center', top: 0, textStyle: { color: '#fff', fontSize: 13 } },
        tooltip: { trigger: 'item' },
        series: [{
            type: 'pie', radius: ['35%', '65%'], center: ['50%', '55%'],
            data, label: { color: '#aac', fontSize: 10, formatter: '{b}' },
            emphasis: { label: { fontSize: 14 } }
        }]
    });
}

function renderTrendChart() {
    trendChart.setOption({
        title: { text: '近5分钟窗口趋势', left: 'center', textStyle: { color: '#fff', fontSize: 13 } },
        tooltip: { trigger: 'axis' },
        legend: { data: ['金额', '笔数'], textStyle: { color: '#aac' }, top: 22 },
        grid: { top: 60, right: 50, left: 60, bottom: 30 },
        xAxis: { type: 'category', data: trendData.map(d => d.time), axisLabel: { color: '#fff', fontSize: 10 } },
        yAxis: [
            { type: 'value', name: '金额(¥)', nameTextStyle: { color: '#aac' },
              axisLabel: { color: '#aac', formatter: v => v >= 1000 ? (v/1000).toFixed(1)+'k' : v } },
            { type: 'value', name: '笔数', nameTextStyle: { color: '#aac' },
              axisLabel: { color: '#aac' } }
        ],
        series: [
            { name: '金额', type: 'line', data: trendData.map(d => d.amount), smooth: true,
              yAxisIndex: 0, itemStyle: { color: '#00d4ff' } },
            { name: '笔数', type: 'line', data: trendData.map(d => d.count), smooth: true,
              yAxisIndex: 1, itemStyle: { color: '#ffa500' } }
        ]
    });
}

/* ========== 初始化 ========== */
document.getElementById('filterInput').addEventListener('keydown', function(e) {
    if (e.key === 'Enter') applyFilter();
});
applyFilter();
fetchTopRiskyUsers();
setInterval(fetchTopRiskyUsers, 15000);

window.onresize = () => { categoryChart.resize(); trendChart.resize(); };
