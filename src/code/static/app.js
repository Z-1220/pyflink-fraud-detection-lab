let ws;
let wsReconnectDelay = 1000;
const WS_MAX_DELAY = 30000;

function connectWebSocket() {
    ws = new WebSocket(`ws://${location.host}/ws`);
    ws.onopen = () => {
        console.log('WebSocket 已连接');
        wsReconnectDelay = 1000;
    };
    ws.onerror = (err) => console.error('WebSocket 错误', err);
    ws.onclose = () => {
        console.warn('WebSocket 已断开，%d秒后重连', wsReconnectDelay / 1000);
        setTimeout(connectWebSocket, wsReconnectDelay);
        wsReconnectDelay = Math.min(wsReconnectDelay * 2, WS_MAX_DELAY);
    };
    ws.onmessage = (event) => {
        try {
            const msg = JSON.parse(event.data);
            switch (msg.topic) {
                case 'snapshot':
                    applySnapshot(msg.data);
                    break;
                case 'total_amount_and_count_events':
                    updateTotals(msg.data);
                    break;
                case 'window_count_and_amount_events':
                    updateTrend(msg.data);
                    break;
                case 'category_aggregated_events':
                    updateCategory(msg.data);
                    break;
                case 'region_aggregated_events':
                    updateRegion(msg.data);
                    break;
                case 'user_risk_scores':
                    fetchRiskScores();
                    break;
                case 'alarm_events':
                    addAlarm(msg.data);
                    break;
            }
        } catch (e) {
            console.error('消息处理错误', e);
        }
    };
    }


let totalAmount = 0, totalCount = 0, alarmCount = 0;
let filterActive = false;
const categoryMap = new Map();
const regionMap = new Map();
const regionAlertMap = new Map();
const trendData = [];
const alarmList = [];

// ECharts 实例
const categoryChart = echarts.init(document.getElementById('categoryChart'));
const trendChart = echarts.init(document.getElementById('trendChart'));
const riskScoreChart = echarts.init(document.getElementById('riskScoreChart'));
const chinaMapChart = echarts.init(document.getElementById('chinaMap'));
let mapMode = 'amount';  // 'amount' | 'alert'

// 5 个仪表盘实例
const gaugeCharts = {
    LARGE_AMOUNT:        echarts.init(document.getElementById('gaugeLarge')),
    HIGH_FREQUENCY:       echarts.init(document.getElementById('gaugeFreq')),
    CONTINUOUS_INCREASE:  echarts.init(document.getElementById('gaugeIncrease')),
    FAILED_SURGE:         echarts.init(document.getElementById('gaugeFailed')),
    IP_SHARING:           echarts.init(document.getElementById('gaugeIp')),
    TOTAL:                echarts.init(document.getElementById('gaugeTotal')),
};

const GAUGE_CONFIG = {
    LARGE_AMOUNT:        { name: '大额交易',   color: '#D45252' },
    HIGH_FREQUENCY:       { name: '高频交易',   color: '#E88A3A' },
    CONTINUOUS_INCREASE:  { name: '连续递增',   color: '#D4A037' },
    FAILED_SURGE:         { name: '失败飙升',   color: '#C04878' },
    IP_SHARING:           { name: 'IP共用',     color: '#3A8AE8' },
    TOTAL:                { name: '告警占比',    color: '#D6E4F0' },
};

// 初始化所有仪表盘为空
Object.keys(gaugeCharts).forEach(key => {
    const cfg = GAUGE_CONFIG[key];
    gaugeCharts[key].setOption({
        series: [{
            type: 'gauge', startAngle: 210, endAngle: -30,
            center: ['50%', '62%'], radius: '68%',
            min: 0, max: 100,
            axisLine: { show: true, lineStyle: { width: 5, color: [
                [0.2, cfg.color], [1, 'rgba(255,255,255,0.10)']
            ] } },
            axisTick: { show: false },
            splitLine: { show: false },
            axisLabel: { show: false },
            detail: { offsetCenter: [0, 20], valueAnimation: true,
                      formatter: '{value}%',
                      fontSize: 16, color: cfg.color },
            title: { offsetCenter: [0, '92%'], fontSize: 13, color: cfg.color },
            data: [{ value: 0, name: cfg.name }]
        }]
    });
});

/* ========== 状态快照（刷新恢复） ========== */
function applySnapshot(data) {
    if (data.total_amount !== undefined) totalAmount = data.total_amount;
    if (data.total_count !== undefined) totalCount = data.total_count;
    if (data.alarm_count !== undefined) alarmCount = data.alarm_count;
    document.getElementById('totalAmount').innerText = totalAmount.toFixed(2);
    document.getElementById('totalCount').innerText = totalCount;
    document.getElementById('alarmCount').innerText = alarmCount;
    if (data.region_map) {
        regionMap.clear();
        Object.entries(data.region_map).forEach(([k, v]) => regionMap.set(k, v));
    }
    updateTotalGauge();
}

/* ========== 指标卡 ========== */
function updateTotalGauge() {
    const ratio = totalCount > 0 ? Math.min(Math.ceil(alarmCount / totalCount * 100), 100) : 0;
    _setTotalGaugeValue(ratio);
}

function _setTotalGaugeValue(ratio) {
    const cfg = GAUGE_CONFIG.TOTAL;
    gaugeCharts.TOTAL.setOption({
        series: [{
            axisLine: { lineStyle: { width: 6, color: [
                [ratio / 100, cfg.color],
                [1, 'rgba(255,255,255,0.10)']
            ] } },
            detail: { formatter: '{value}%', color: cfg.color },
            title: { color: cfg.color },
            data: [{ value: ratio, name: cfg.name }]
        }]
    });
}

function updateTotals(data) {
    totalAmount = data.total_amount || 0;
    totalCount = data.transaction_count || 0;
    document.getElementById('totalAmount').innerText = totalAmount.toFixed(2);
    document.getElementById('totalCount').innerText = totalCount;
    updateTotalGauge();
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
    categoryMap.set(data.category, { amount: data.total_amount, count: data.transaction_count });
    renderCategoryChart();
}

function updateRegion(data) {
    const prov = data.province;
    if (!prov) return;
    regionMap.set(prov, { amount: data.total_amount, count: data.transaction_count });
    renderChinaMap();
}

function addAlarm(data) {
    alarmCount++;
    document.getElementById('alarmCount').innerText = alarmCount;
    alarmList.unshift(data);
    if (alarmList.length > 200) alarmList.pop();
    if (!filterActive) {
        document.getElementById('filterCount').innerText = alarmList.length + ' 条';
        renderAlarmTable(alarmList);
    }
    updateTotalGauge();
}

/* ========== 筛选 ========== */
async function applyFilter() {
    const keyword = document.getElementById('filterInput').value.trim();
    const type = document.getElementById('typeFilter').value;

    if (!keyword && !type) {
        filterActive = false;
        document.getElementById('filterCount').innerText = alarmList.length + ' 条';
        renderAlarmTable(alarmList);
        return;
    }

    filterActive = true;
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

/* ========== 仪表盘快照（合并轮询） ========== */
async function fetchSnapshot() {
    try {
        const resp = await fetch('/api/dashboard/snapshot');
        const data = await resp.json();
        if (!data || data.error) return;
        if (data.top_risky_users) renderTopRiskyUsers(data.top_risky_users);
        if (data.risk_scores && data.risk_scores.length > 0) renderRiskScoreChart(data.risk_scores);
        if (data.alert_stats) renderGauges(data.alert_stats);
        if (data.region_alerts) {
            regionAlertMap.clear();
            data.region_alerts.forEach(r => {
                if (r.province && r.alert_count > 0) regionAlertMap.set(r.province, r.alert_count);
            });
            renderChinaMap();
        }
    } catch (e) {
        console.error('获取仪表盘快照失败', e);
    }
}

/* ========== 风险评分榜 ========== */
let _riskScoreTimer = null;

function fetchRiskScores() {
    // WebSocket 触发时仅刷新风险评分（轻量），5 秒内去重
    if (_riskScoreTimer) return;
    _riskScoreTimer = setTimeout(() => {
        _riskScoreTimer = null;
        _doFetchRiskScores();
    }, 5000);
}

async function _doFetchRiskScores() {
    try {
        const resp = await fetch('/api/user-risk-scores?limit=10');
        const data = await resp.json();
        if (Array.isArray(data) && data.length > 0) {
            renderRiskScoreChart(data);
        }
    } catch (e) {
        console.error('获取风险评分失败', e);
    }
}

function renderRiskScoreChart(data) {
    const hasData = Array.isArray(data) && data.length > 0;
    const names = hasData ? data.map(d => d.user_name).reverse() : [];
    const scores = hasData ? data.map(d => d.risk_score).reverse() : [];
    riskScoreChart.setOption({
        animationDuration: 600,
        animationEasing: 'cubicOut',
        animationDurationUpdate: 500,
        animationEasingUpdate: 'cubicInOut',
        title: { text: '用户风险评分 Top 10', left: 'center', top: 4,
                 textStyle: { color: '#D6E4F0', fontSize: 18 } },
        tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' },
                   valueFormatter: v => (typeof v === 'number') ? v.toFixed(4) : v },
        grid: { top: 40, right: 60, left: 170, bottom: 20 },
        xAxis: { type: 'value', name: '风险分', min: 0,
                 nameTextStyle: { color: '#8AA4C0', fontSize: 13 },
                 axisLabel: { color: '#8AA4C0', fontSize: 13,
                   formatter: v => v.toFixed(2) },
                 splitLine: { lineStyle: { color: 'rgba(255,255,255,0.06)' } } },
        yAxis: { type: 'category', data: names,
                 axisLabel: { color: '#D6E4F0', fontSize: 13, interval: 0 },
                 axisLine: { lineStyle: { color: 'rgba(255,255,255,0.10)' } } },
        series: [{
            type: 'bar',
            data: hasData ? scores.map((v, i) => {
                const ratio = i / (scores.length - 1 || 1);
                return {
                    value: v,
                    itemStyle: {
                        color: ratio < 0.3 ? '#D45252'
                             : ratio < 0.55 ? '#E88A3A'
                             : ratio < 0.8 ? '#D4A037'
                             : '#3A8AE8',
                        borderRadius: [0, 3, 3, 0]
                    }
                };
            }) : [],
            label: { show: hasData, position: 'right', color: '#8AA4C0', fontSize: 14,
                     formatter: p => p.value.toFixed(3) },
        }]
    });
    riskScoreChart.resize();
}

/* ========== 告警仪表盘 ========== */
function fetchAlertStats() {
    fetchSnapshot();
}

function renderGauges(byType) {
    const total = byType.reduce((s, t) => s + t.count, 0) || 1;
    byType.forEach(item => {
        const chart = gaugeCharts[item.alert_type];
        if (!chart) return;
        const cfg = GAUGE_CONFIG[item.alert_type];
        const pct = item.count > 0 ? Math.max(1, Math.ceil(item.count / total * 100)) : 0;
        chart.setOption({
            series: [{
                axisLine: { lineStyle: { width: 8, color: [
                    [pct / 100, cfg.color],
                    [1, 'rgba(255,255,255,0.10)']
                ] } },
                detail: { color: cfg.color },
                title: { color: cfg.color },
                data: [{ value: pct, name: cfg.name + ' ' + item.count }]
            }]
        });
    });
    // 总告警占比仪表盘（告警总数 / 交易总数）
    const ratio = totalCount > 0 ? Math.min(Math.max(1, Math.ceil(total / totalCount * 100)), 100) : 0;
    _setTotalGaugeValue(ratio);
}

/* ========== 中国地图热力图 ========== */
let chinaGeoLoaded = false;

async function loadChinaGeo() {
    try {
        const [geoResp, regionResp] = await Promise.all([
            fetch('/static/china.json?v=1'),
            fetch('/api/region-stats')
        ]);
        const geo = await geoResp.json();
        echarts.registerMap('china', geo);
        chinaGeoLoaded = true;
        // 加载初始省份交易数据
        const regions = await regionResp.json();
        if (Array.isArray(regions)) {
            regions.forEach(r => {
                if (r.province) {
                    regionMap.set(r.province, { amount: r.total_amount, count: r.transaction_count });
                }
            });
        }
        renderChinaMap();
    } catch (e) {
        console.error('加载中国地图失败', e);
        document.getElementById('chinaMap').innerHTML = '<span style="color:#5A7A96;display:flex;align-items:center;justify-content:center;height:100%;">地图加载失败</span>';
    }
}

function renderChinaMap() {
    if (!chinaGeoLoaded) return;
    const isAmount = mapMode === 'amount';
    const sourceMap = isAmount ? regionMap : regionAlertMap;
    const data = [];
    if (isAmount) {
        sourceMap.forEach((v, k) => data.push({ name: k, value: v.amount }));
    } else {
        sourceMap.forEach((v, k) => data.push({ name: k, value: v }));
    }
    const maxVal = data.length > 0 ? Math.max(...data.map(d => d.value)) : 1;
    const colors = isAmount
        ? ['#1E5A7A', '#2B7BE4', '#48A8F0', '#D4A037', '#D45252']
        : ['#2E1A3E', '#6B2B5A', '#C84878', '#D45252', '#E88A3A'];
    chinaMapChart.setOption({
        tooltip: {
            trigger: 'item',
            formatter: p => p.name
                ? (isAmount ? `${p.name}<br/>交易额: ¥${(p.value || 0).toFixed(2)}`
                            : `${p.name}<br/>告警数: ${p.value || 0}`)
                : '暂无数据'
        },
        visualMap: {
            min: 0, max: maxVal, left: -8, bottom: 10,
            text: ['高', '低'], textStyle: { color: '#8AA4C0' },
            inRange: { color: colors },
            calculable: false
        },
        geo: {
            map: 'china', roam: false, zoom: 1.15,
            center: [105, 36],
            label: { show: false },
            itemStyle: {
                areaColor: '#1A3A5C',
                borderColor: 'rgba(255,255,255,0.15)',
                borderWidth: 0.5
            },
            emphasis: {
                label: { show: true, color: '#D6E4F0', fontSize: 14 },
                itemStyle: { areaColor: '#2A5A8C' }
            }
        },
        series: [{
            type: 'map', map: 'china', geoIndex: 0,
            data: data
        }]
    });
}

function switchMap(mode) {
    mapMode = mode;
    document.getElementById('btnAmount').className = 'toggle-btn' + (mode === 'amount' ? ' active' : '');
    document.getElementById('btnAlert').className = 'toggle-btn' + (mode === 'alert' ? ' active' : '');
    renderChinaMap();
}

/* ========== 导出 ========== */
function exportAlerts() {
    const type = document.getElementById('typeFilter').value;
    const keyword = document.getElementById('filterInput').value.trim();
    let url = '/api/export/alerts?';
    if (type) url += `alert_type=${encodeURIComponent(type)}&`;
    if (keyword) url += `keyword=${encodeURIComponent(keyword)}&`;
    window.open(url, '_blank');
}

function exportStats() {
    window.open('/api/export/stats?', '_blank');
}

/* ========== 图表渲染 ========== */
function renderCategoryChart() {
    const data = Array.from(categoryMap.entries()).map(([k, v]) => ({ name: k, value: v.amount }));
    categoryChart.setOption({
        color: ['#D45252','#D4A037','#3CAB6E','#3A8AE8','#E88A3A',
                '#7C6BC4','#D4808A','#48B8B0','#CC8A5C','#7A9CC0'],
        title: { text: '商品类别交易分布', left: 'center', top: 0,
                 textStyle: { color: '#D6E4F0', fontSize: 18 } },
        tooltip: { trigger: 'item' },
        series: [{
            type: 'pie', radius: ['35%', '65%'], center: ['50%', '55%'],
            data, label: { color: '#8AA4C0', fontSize: 12, formatter: '{b}' },
            emphasis: { label: { fontSize: 16 } }
        }]
    });
}

function renderTrendChart() {
    trendChart.setOption({
        title: { text: '近5分钟交易趋势', left: 'center',
                 textStyle: { color: '#D6E4F0', fontSize: 18 } },
        tooltip: { trigger: 'axis' },
        legend: { data: ['金额', '笔数'], textStyle: { color: '#8AA4C0', fontSize: 13 }, top: 24 },
        grid: { top: 60, right: 50, left: 70, bottom: 30 },
        xAxis: { type: 'category', data: trendData.map(d => d.time),
                 axisLabel: { color: '#8AA4C0', fontSize: 12 } },
        yAxis: [
            { type: 'value', name: '金额(¥)', nameTextStyle: { color: '#8AA4C0' },
              axisLabel: { color: '#8AA4C0',
                formatter: v => v >= 1000 ? (v/1000).toFixed(1)+'k' : v },
              splitLine: { lineStyle: { color: 'rgba(255,255,255,0.06)' } } },
            { type: 'value', name: '笔数', nameTextStyle: { color: '#8AA4C0' },
              axisLabel: { color: '#8AA4C0' },
              splitLine: { show: false },
              min: 0, max: (v) => Math.max(v.max * 4, 50) }
        ],
        series: [
            { name: '金额', type: 'line', data: trendData.map(d => d.amount),
              smooth: true, yAxisIndex: 0, itemStyle: { color: '#2B7BE4' } },
            { name: '笔数', type: 'line', data: trendData.map(d => d.count),
              smooth: true, yAxisIndex: 1, itemStyle: { color: '#D4A037' } }
        ]
    });
}

/* ========== 风险用户排行 ========== */
function fetchTopRiskyUsers() {
    fetchSnapshot();
}

function renderTopRiskyUsers(data) {
    const container = document.getElementById('topUsersList');
    if (!Array.isArray(data) || data.length === 0) {
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
}

function escHtml(s) {
    const div = document.createElement('div');
    div.textContent = s;
    return div.innerHTML;
}

/* ========== 初始化 ========== */
document.getElementById('filterInput').addEventListener('keydown', function(e) {
    if (e.key === 'Enter') applyFilter();
});
connectWebSocket();
applyFilter();
loadChinaGeo();
fetchSnapshot();

// 定时轮询（单一接口，15 秒）
setInterval(fetchSnapshot, 15000);

window.onresize = () => {
    categoryChart.resize();
    trendChart.resize();
    riskScoreChart.resize();
    chinaMapChart.resize();
    Object.values(gaugeCharts).forEach(c => c.resize());
};
