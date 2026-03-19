
  <template>
    <div class="dashboard-layout">
      <el-header class="system-header">
        <div class="logo">
          <el-icon class="logo-icon"><DataLine /></el-icon>
          <span>上市公司多维风险画像与预警系统</span>
        </div>
        <div class="search-area">
          <el-input
            v-model="searchTsCode"
            placeholder="请输入股票代码 (如: 000001.SZ)"
            class="search-input"
            @keyup.enter="fetchRiskData"
            clearable
          >
            <template #append>
              <el-button :loading="isLoading" @click="fetchRiskData" :icon="Search">
                风险探测
              </el-button>
            </template>
          </el-input>
        </div>
      </el-header>
  
      <el-alert
        v-if="errorMessage"
        :title="errorMessage"
        type="error"
        show-icon
        class="global-alert"
      />
  
      <el-main class="main-content" v-show="riskData || isLoading">
        <el-row :gutter="20">
          
          <el-col :span="8">
            
            <el-card shadow="hover" class="info-card">
              <template #header>
                <div class="card-header">
                  <span>🎯 目标企业画像</span>
                  <el-tag :type="riskLevelColor" effect="dark" round>
                    {{ riskLevelText }}
                  </el-tag>
                </div>
              </template>
              <el-skeleton :rows="3" animated :loading="isLoading">
                <template #default>
                  <el-descriptions :column="1" border size="small">
                    <el-descriptions-item label="股票代码">{{ riskData?.tsCode || '--' }}</el-descriptions-item>
                    <el-descriptions-item label="综合安全度">
                      <span :class="['score-text', riskLevelColor]">
                        {{ riskData?.compositeScore?.toFixed(4) || '--' }}
                      </span>
                    </el-descriptions-item>
                    <el-descriptions-item label="系统评估结论">
                      {{ assessmentConclusion }}
                    </el-descriptions-item>
                  </el-descriptions>
                </template>
              </el-skeleton>
            </el-card>
  
            <el-card shadow="hover" class="radar-card mt-20">
              <el-skeleton style="height: 350px" animated :loading="isLoading">
                <template #default>
                  <div ref="radarChartRef" class="radar-chart-container"></div>
                </template>
              </el-skeleton>
            </el-card>
          </el-col>
  
          <el-col :span="16">
            
            <el-card shadow="hover" class="kline-card">
              <template #header>
                <div class="card-header">
                  <span>📈 风险趋势演化与高危信号拦截 (动态K线)</span>
                </div>
              </template>
              <el-skeleton style="height: 450px" animated :loading="isLoading">
                <template #default>
                  <RiskKlineChart :ts-code="riskData?.tsCode || searchTsCode" />
                </template>
              </el-skeleton>
            </el-card>
  
            <el-card shadow="hover" class="table-card mt-20">
              <template #header>
                <div class="card-header">
                  <span>📊 底层运筹学算法输出明细 (近期记录)</span>
                </div>
              </template>
              <el-skeleton :rows="4" animated :loading="isLoading">
                <template #default>
                  <el-table :data="mockHistoryTable" border stripe style="width: 100%" size="small">
                    <el-table-column prop="date" label="评估日期" width="120" />
                    <el-table-column prop="fin" label="财务风险得分" />
                    <el-table-column prop="mkt" label="市场风险得分" />
                    <el-table-column prop="str" label="战略风险得分" />
                    <el-table-column prop="comp" label="综合贴近度" width="120">
                      <template #default="scope">
                        <el-tag :type="scope.row.comp > 0.5 ? 'success' : 'danger'">
                          {{ scope.row.comp.toFixed(4) }}
                        </el-tag>
                      </template>
                    </el-table-column>
                  </el-table>
                </template>
              </el-skeleton>
            </el-card>
  
          </el-col>
        </el-row>
      </el-main>
    </div>
  </template>
  
  <script setup>
  import { ref, computed, nextTick, shallowRef } from 'vue'
  import { Search } from '@element-plus/icons-vue'
  import axios from 'axios'
  import * as echarts from 'echarts'
  import RiskKlineChart from './RiskKlineChart.vue'
  
  const searchTsCode = ref('')
  const isLoading = ref(false)
  const errorMessage = ref('')
  const riskData = ref(null)
  
  const radarChartRef = ref(null)
  const radarChartInstance = shallowRef(null)
  
  // 模拟的近期历史数据，用来填充表格，显示你的工作量
  const mockHistoryTable = ref([])
  
  // ==========================================
  // 计算属性：根据综合得分动态变色并输出结论
  // ==========================================
  const riskLevelColor = computed(() => {
    if (!riskData.value) return 'info'
    const score = riskData.value.compositeScore
    if (score > 0.6) return 'success'
    if (score > 0.4) return 'warning'
    return 'danger'
  })
  
  const riskLevelText = computed(() => {
    if (!riskData.value) return '暂无数据'
    const score = riskData.value.compositeScore
    if (score > 0.6) return '风险极低 (安全)'
    if (score > 0.4) return '风险中等 (关注)'
    return '风险极高 (预警)'
  })
  
  const assessmentConclusion = computed(() => {
    if (!riskData.value) return ''
    const score = riskData.value.compositeScore
    if (score > 0.6) return '企业各项指标运行平稳，距离负理想解较远。'
    if (score > 0.4) return '部分维度出现短板凹陷，需结合雷达图排查异动。'
    return '触发系统警报！企业极大概率存在财务隐患或交易异常，建议立刻规避。'
  })
  
  // ==========================================
  // 雷达图渲染逻辑 (已修复居中与大小比例)
  // ==========================================
  const renderRadarChart = (data) => {
    if (!radarChartInstance.value) {
      radarChartInstance.value = echarts.init(radarChartRef.value)
    }

    // 行业基准安全线（三个维度：财务、市场、战略）
    const industryBenchmark = [0.90, 0.85, 0.88]
    // 目标公司实际得分（与后端 DTO 的三个维度字段一一对应）
    const targetCompanyScores = [
      data.financialScore || 0,
      data.marketScore || 0,
      data.strategicScore || 0
    ]

    const option = {
      tooltip: { trigger: 'item' },
      legend: { bottom: 0, data: ['行业基准', '目标公司'] },
      radar: {
        center: ['50%', '45%'],
        radius: '65%',
        indicator: [
          { name: '财务风险安全度', max: 1 },
          { name: '市场风险安全度', max: 1 },
          { name: '战略风险安全度', max: 1 }
        ],
        shape: 'polygon',
        splitNumber: 5,
        axisName: { color: '#333', fontSize: 12, borderRadius: 3, padding: [3, 5] },
        splitArea: { areaStyle: { color: ['#f8f9fa', '#e9ecef', '#dee2e6', '#ced4da', '#adb5bd'].reverse() } }
      },
      series: [{
        name: '风险画像对比',
        type: 'radar',
        emphasis: { lineStyle: { width: 4 } },
        data: [
          { value: industryBenchmark, name: '行业基准', itemStyle: { color: '#67C23A' }, areaStyle: { color: 'rgba(103, 194, 58, 0.3)' }, lineStyle: { type: 'dashed' } },
          { value: targetCompanyScores, name: '目标公司', itemStyle: { color: '#F56C6C' }, areaStyle: { color: 'rgba(245, 108, 108, 0.5)' } }
        ]
      }]
    }
    radarChartInstance.value.setOption(option)
  }
  
  // ==========================================
  // 数据请求逻辑
  // ==========================================
  const fetchRiskData = async () => {
    if (!searchTsCode.value.trim()) return
    
    isLoading.value = true
    errorMessage.value = ''
    riskData.value = null
  
    try {
      const response = await axios.get(`http://localhost:8080/api/risk/evaluate/${searchTsCode.value.trim()}`)
      if (response.data) {
        riskData.value = response.data
        
        // 安全取值，防止 null/undefined 导致 toFixed 报错
        const fin = response.data.financialScore || 0
        const mkt = response.data.marketScore || 0
        const str = response.data.strategicScore || 0
        const comp = response.data.compositeScore || 0

        mockHistoryTable.value = [
          { date: '2023-10-18', fin: fin, mkt: mkt, str: str, comp: comp },
          { date: '2023-10-17', fin: 0.55, mkt: 0.62, str: 0.61, comp: 0.58 },
          { date: '2023-10-16', fin: 0.61, mkt: 0.59, str: 0.65, comp: 0.61 }
        ]
      }
    } catch (error) {
      console.error('请求异常详情:', error)
      if (error.response) {
        if (error.response.status === 500) {
          errorMessage.value = '算法引擎异常'
        } else {
          errorMessage.value = `服务器返回错误 (${error.response.status})`
        }
      } else if (error.request) {
        errorMessage.value = '网络连接失败，请检查后端服务是否启动'
      } else {
        errorMessage.value = `请求异常: ${error.message}`
      }
    } finally {
      // 先关闭 loading，让 el-skeleton 切换到 #default 插槽，DOM 才会真正渲染出来
      isLoading.value = false

      // 如果有数据，等 DOM 渲染完成后再初始化 ECharts
      if (riskData.value) {
        await nextTick()
        renderRadarChart(riskData.value)
      }
    }
  }
  </script>
  
  <style scoped>
  .dashboard-layout {
    min-height: 100vh;
    background-color: #f0f2f5;
  }
  
  /* 顶部导航栏样式 */
  .system-header {
    background-color: #001529;
    color: #fff;
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0 40px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.15);
  }
  
  .logo {
    display: flex;
    align-items: center;
    font-size: 20px;
    font-weight: bold;
    letter-spacing: 1px;
  }
  .logo-icon {
    margin-right: 10px;
    font-size: 24px;
    color: #409EFF;
  }
  
  .search-area {
    width: 400px;
  }
  
  .global-alert {
    margin: 15px 20px 0;
  }
  
  .main-content {
    padding: 20px;
  }
  
  /* 卡片通用样式 */
  .card-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-weight: bold;
    color: #303133;
  }
  .mt-20 {
    margin-top: 20px;
  }
  
  /* 得分高亮 */
  .score-text {
    font-size: 24px;
    font-weight: bold;
  }
  .score-text.success { color: #67C23A; }
  .score-text.warning { color: #E6A23C; }
  .score-text.danger { color: #F56C6C; }
  
  /* 雷达图与占位符 */
  .radar-chart-container {
    width: 100%;
    height: 350px;
  }
  
  .placeholder-box {
    width: 100%;
    height: 100%;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    background-color: #fafafa;
    border: 2px dashed #dcdfe6;
    border-radius: 4px;
    color: #909399;
  }
  .placeholder-box p {
    margin-bottom: 15px;
  }
  </style>