<template>
  <div ref="klineChartRef" class="kline-chart-container"></div>
</template>

<script setup>
/**
 * RiskKlineChart.vue
 * 风险预警K线图组件 —— 叠加呼吸灯脉动动画的高危信号拦截可视化
 *
 * 功能说明：
 * 1. 渲染标准 Candlestick（K线）图，X轴为交易日期，Y轴为股票价格
 * 2. 在后端判定为"高风险预警"的日期对应K线上方，叠加红色圆形标记
 * 3. 利用 ECharts 5.3+ 的 keyframeAnimation 实现呼吸灯脉动动画（无限循环）
 *
 * Props:
 *   tsCode   - 股票代码，用于标题展示
 *   klineData - K线数据数组，格式: [{ date, open, close, low, high }]
 *   alertDates - 高风险预警日期数组，格式: ['2023-10-10', '2023-10-12']
 *
 * 当不传入 klineData 时，组件内部使用模拟数据进行演示
 */
import { ref, shallowRef, onMounted, onBeforeUnmount, watch } from 'vue'
import * as echarts from 'echarts'

const props = defineProps({
  tsCode: {
    type: String,
    default: '600519.SH'
  },
  klineData: {
    type: Array,
    default: null
  },
  alertDates: {
    type: Array,
    default: null
  }
})

const klineChartRef = ref(null)
const chartInstance = shallowRef(null)

// 缓存已生成的数据，避免 resize 时重新随机生成
let cachedDates = null
let cachedOhlcData = null
let cachedAlertDates = null

// ==========================================
// 模拟数据生成器（当外部未传入真实数据时使用）
// ==========================================
const generateMockData = () => {
  const dates = []
  const ohlcData = []
  const mockAlertDates = []

  // 生成 60 个交易日的模拟数据
  let basePrice = 1680
  const startDate = new Date('2023-08-01')

  for (let i = 0; i < 60; i++) {
    const currentDate = new Date(startDate)
    currentDate.setDate(startDate.getDate() + i)

    // 跳过周末
    const dayOfWeek = currentDate.getDay()
    if (dayOfWeek === 0 || dayOfWeek === 6) continue

    const dateStr = currentDate.toISOString().slice(0, 10)
    dates.push(dateStr)

    // 随机波动生成 OHLC
    const change = (Math.random() - 0.48) * 30
    const open = basePrice
    const close = basePrice + change
    const high = Math.max(open, close) + Math.random() * 15
    const low = Math.min(open, close) - Math.random() * 15

    // ECharts K线数据格式: [open, close, low, high]
    ohlcData.push([
      parseFloat(open.toFixed(2)),
      parseFloat(close.toFixed(2)),
      parseFloat(low.toFixed(2)),
      parseFloat(high.toFixed(2))
    ])

    basePrice = close

    // 模拟：在某些日期触发高风险预警（跌幅超过一定阈值）
    if (change < -15) {
      mockAlertDates.push(dateStr)
    }
  }

  // 确保至少有 3 个预警点用于演示
  if (mockAlertDates.length < 3) {
    const extraIndices = [10, 25, 38]
    for (const idx of extraIndices) {
      if (idx < dates.length && !mockAlertDates.includes(dates[idx])) {
        mockAlertDates.push(dates[idx])
      }
    }
  }

  return { dates, ohlcData, mockAlertDates }
}

// ==========================================
// 构建 graphic 呼吸灯预警标记
// ==========================================
const buildAlertGraphics = (chartInst, dates, ohlcData, alertDates) => {
  const graphicElements = []

  alertDates.forEach((alertDate) => {
    const dateIndex = dates.indexOf(alertDate)
    if (dateIndex === -1) return

    // 获取该日K线的最高价，标记放在最高价上方
    const highPrice = ohlcData[dateIndex][3]

    // 将数据坐标转换为像素坐标
    const pixelPos = chartInst.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [dateIndex, highPrice])
    if (!pixelPos) return

    // 外圈：呼吸灯脉动圆环（带 keyframeAnimation）
    graphicElements.push({
      type: 'circle',
      position: [pixelPos[0], pixelPos[1] - 20],
      shape: { r: 10 },
      style: {
        fill: 'rgba(255, 59, 48, 0.3)',
        stroke: '#FF3B30',
        lineWidth: 2
      },
      // ECharts 5.3+ keyframeAnimation 呼吸灯动画
      keyframeAnimation: [{
        duration: 2000,
        loop: true,
        keyframes: [
          {
            percent: 0,
            scaleX: 1,
            scaleY: 1,
            style: { opacity: 1 }
          },
          {
            percent: 0.5,
            scaleX: 1.8,
            scaleY: 1.8,
            style: { opacity: 0.3 }
          },
          {
            percent: 1,
            scaleX: 1,
            scaleY: 1,
            style: { opacity: 1 }
          }
        ]
      }]
    })

    // 内圈：实心红色圆点（静态，作为视觉锚点）
    graphicElements.push({
      type: 'circle',
      position: [pixelPos[0], pixelPos[1] - 20],
      shape: { r: 4 },
      style: {
        fill: '#FF3B30'
      }
    })
  })

  return graphicElements
}

// ==========================================
// 渲染 K 线图主逻辑
// ==========================================
const renderChart = () => {
  if (!klineChartRef.value) return

  if (!chartInstance.value) {
    chartInstance.value = echarts.init(klineChartRef.value)
  }

  // 决定使用外部数据还是模拟数据（使用缓存避免 resize 时数据变化）
  let dates, ohlcData, alertDates

  if (props.klineData && props.klineData.length > 0) {
    dates = props.klineData.map(item => item.date)
    ohlcData = props.klineData.map(item => [item.open, item.close, item.low, item.high])
    alertDates = props.alertDates || []
  } else {
    // 只在首次调用时生成模拟数据，后续复用缓存
    if (!cachedDates) {
      const mock = generateMockData()
      cachedDates = mock.dates
      cachedOhlcData = mock.ohlcData
      cachedAlertDates = mock.mockAlertDates
    }
    dates = cachedDates
    ohlcData = cachedOhlcData
    alertDates = cachedAlertDates
  }

  // 基础 K 线图 Option
  const option = {
    title: {
      text: `${props.tsCode} 风险预警K线图`,
      subtext: '红色脉动标记 = 高风险预警信号',
      left: 'center',
      top: 5,
      textStyle: { color: '#303133', fontSize: 16 },
      subtextStyle: { color: '#909399', fontSize: 12 }
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross' },
      formatter: function (params) {
        if (!params || params.length === 0) return ''
        const p = params[0]
        const data = p.data
        const dateStr = dates[p.dataIndex]
        const isAlert = alertDates.includes(dateStr)
        return `
          <div style="font-size:13px;">
            <strong>${dateStr}</strong> ${isAlert ? '<span style="color:#FF3B30;">⚠ 高风险预警</span>' : ''}<br/>
            开盘: ${data[1]}<br/>
            收盘: ${data[2]}<br/>
            最低: ${data[3]}<br/>
            最高: ${data[4]}
          </div>
        `
      }
    },
    legend: {
      data: ['K线'],
      bottom: 5
    },
    grid: {
      left: '10%',
      right: '10%',
      top: 70,
      bottom: 50
    },
    xAxis: {
      type: 'category',
      data: dates,
      axisLabel: {
        rotate: 45,
        fontSize: 10,
        color: '#606266'
      },
      splitLine: { show: false }
    },
    yAxis: {
      type: 'value',
      scale: true,
      axisLabel: {
        color: '#606266',
        formatter: '{value}'
      },
      splitArea: {
        show: true,
        areaStyle: {
          color: ['#fafafa', '#f5f5f5']
        }
      }
    },
    dataZoom: [
      {
        type: 'inside',
        xAxisIndex: 0,
        start: 0,
        end: 100
      },
      {
        type: 'slider',
        xAxisIndex: 0,
        bottom: 25,
        height: 20,
        start: 0,
        end: 100
      }
    ],
    series: [
      {
        name: 'K线',
        type: 'candlestick',
        data: ohlcData,
        itemStyle: {
          color: '#EF5350',        // 阳线填充（收盘 > 开盘）
          color0: '#26A69A',       // 阴线填充（收盘 < 开盘）
          borderColor: '#EF5350',  // 阳线边框
          borderColor0: '#26A69A'  // 阴线边框
        },
        // 在预警日期的K线上方添加 markPoint 作为辅助标注
        markPoint: {
          symbol: 'pin',
          symbolSize: 35,
          data: alertDates.map(alertDate => {
            const idx = dates.indexOf(alertDate)
            if (idx === -1) return null
            return {
              name: '预警',
              coord: [idx, ohlcData[idx][3]],
              value: '⚠',
              itemStyle: { color: 'rgba(255, 59, 48, 0.8)' },
              label: {
                color: '#fff',
                fontSize: 12,
                fontWeight: 'bold'
              }
            }
          }).filter(Boolean)
        }
      }
    ],
    // graphic 组件：呼吸灯动画标记（需要在图表渲染完成后设置）
    graphic: { elements: [] }
  }

  // 先渲染基础图表
  chartInstance.value.setOption(option, true)

  // 图表渲染完成后，计算像素坐标并添加 graphic 呼吸灯动画
  // 使用 setTimeout 确保 ECharts 内部布局计算完成
  setTimeout(() => {
    const graphicElements = buildAlertGraphics(chartInstance.value, dates, ohlcData, alertDates)
    chartInstance.value.setOption({
      graphic: {
        elements: graphicElements
      }
    })
  }, 300)
}

// 窗口 resize 时重绘（只 resize + 重算 graphic 坐标，不重新生成数据）
const handleResize = () => {
  if (chartInstance.value) {
    chartInstance.value.resize()
    // resize 后只需重新计算 graphic 的像素坐标
    const dates = cachedDates || (props.klineData ? props.klineData.map(item => item.date) : [])
    const ohlcData = cachedOhlcData || (props.klineData ? props.klineData.map(item => [item.open, item.close, item.low, item.high]) : [])
    const alertDates = cachedAlertDates || props.alertDates || []
    
    setTimeout(() => {
      const graphicElements = buildAlertGraphics(chartInstance.value, dates, ohlcData, alertDates)
      chartInstance.value.setOption({ graphic: { elements: graphicElements } })
    }, 100)
  }
}

onMounted(() => {
  renderChart()
  window.addEventListener('resize', handleResize)
})

onBeforeUnmount(() => {
  window.removeEventListener('resize', handleResize)
  if (chartInstance.value) {
    chartInstance.value.dispose()
    chartInstance.value = null
  }
})

// 监听 props 变化，清除缓存并重新渲染
watch(() => [props.klineData, props.alertDates, props.tsCode], () => {
  cachedDates = null
  cachedOhlcData = null
  cachedAlertDates = null
  renderChart()
}, { deep: true })
</script>

<style scoped>
.kline-chart-container {
  width: 100%;
  height: 450px;
}
</style>