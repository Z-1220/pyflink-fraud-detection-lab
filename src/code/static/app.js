// WebSocket 连接
const ws = new WebSocket(`ws://${location.host}/ws`);

// 数据缓存
let totalAmount = 0, totalCount = 0, alarmCount = 0;
const categoryMap = new Map();
const trendData = [];
const alarmList = [];

// 初始化 ECharts
const categoryChart = echarts.init(document.getElementById('categoryChart'));
const trendChart = echarts.init(document.getElementById('trendChart'));

ws.onopen = () => console.log('WebSocket 已连接');
ws.onerror = (err) => console.error('WebSocket 错误', err);
ws.onclose = () => console.warn('WebSocket 已断开');

ws.onmessage = (event) => {
    try {
        const msg = JSON.parse(event.data);
        console.log('收到消息:', msg.topic, msg.data);
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
            default:
                console.log('未知主题', msg.topic);
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
    const cat = data.category;
    categoryMap.set(cat, { amount: data.total_amount, count: data.transaction_count });
    renderCategoryChart();
}

function addAlarm(data) {
    alarmCount++;
    document.getElementById('alarmCount').innerText = alarmCount;
    alarmList.unshift(data);
    if (alarmList.length > 50) alarmList.pop();
    renderAlarmTable();
}

// 图表渲染
function renderCategoryChart() {
    const data = Array.from(categoryMap.entries()).map(([k, v]) => ({ name: k, value: v.amount }));
    categoryChart.setOption({
        title: { text: '商品类别交易分布', left: 'center', textStyle: { color: '#fff' } },
        tooltip: { trigger: 'item' },
        series: [{ type: 'pie', radius: '70%', data, label: { color: '#fff' } }]
    });
}

function renderTrendChart() {
    trendChart.setOption({
        title: { text: '近5分钟窗口趋势', left: 'center', textStyle: { color: '#fff' } },
        tooltip: { trigger: 'axis' },
        xAxis: { type: 'category', data: trendData.map(d => d.time), axisLabel: { color: '#fff' } },
        yAxis: { type: 'value', axisLabel: { color: '#fff' } },
        series: [
            { name: '金额', type: 'line', data: trendData.map(d => d.amount), smooth: true },
            { name: '笔数', type: 'line', data: trendData.map(d => d.count), smooth: true }
        ]
    });
}

function renderAlarmTable() {
    const tbody = document.getElementById('alarmBody');
    tbody.innerHTML = alarmList.slice(0, 20).map(a => {
        let className = '';
        if (a.alert_type === 'HIGH_FREQUENCY') className = 'high-freq';
        else if (a.alert_type === 'LARGE_AMOUNT') className = 'large-amount';
        else if (a.alert_type === 'CONTINUOUS_INCREASE') className = 'increase';
        const time = a.alert_time ? new Date(a.alert_time).toLocaleTimeString() : '';
        const value = a.amount ? a.amount.toFixed(2) : (a.transaction_count || '');
        return `<tr class="${className}">
            <td>${time}</td><td>${a.alert_type}</td><td>${a.user_id}</td>
            <td>${a.transaction_id || '-'}</td><td>${value}</td><td>${a.details || ''}</td>
        </tr>`;
    }).join('');
}

window.onresize = () => { categoryChart.resize(); trendChart.resize(); };