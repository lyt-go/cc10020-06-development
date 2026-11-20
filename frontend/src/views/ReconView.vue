<template>
  <section class="page" data-module="transform_recon">
    <header class="page-head">
      <div>
        <h2>转换对账数据包</h2>
        <p class="page-desc">
          导出包含原始值、目标值与校验结果的转换复核数据；只有明确映射版本的文件可交付，
          质检结论与已下载文件版本绑定核对。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create-package" type="button" @click="showCreate = true">
          建立对账数据包
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">对账数据包</span>
        <strong class="stat-value">{{ packages.length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">可交付</span>
        <strong class="stat-value">{{ countByStatus('可交付') }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">质检中/完成</span>
        <strong class="stat-value">{{ countByStatus('质检中') + countByStatus('质检完成') }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">不可交付</span>
        <strong class="stat-value">{{ countByStatus('不可交付') }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reloadPackages">
      <label class="filter-item">
        <span>批次号</span>
        <input v-model="batchFilter" data-testid="filter-batch" placeholder="按批次号检索" />
      </label>
      <button class="btn" data-testid="query-package" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="batchFilter = ''; reloadPackages()">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th>批次号</th>
          <th>转换规则</th>
          <th>状态</th>
          <th>导出进度</th>
          <th>可交付/阻断行</th>
          <th>映射版本</th>
          <th>版本冲突</th>
          <th>已下载文件</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="pkg in packages" :key="String(pkg.id)" :data-testid="`package-${pkg.id}`" :data-status="pkg.status">
          <td>{{ pkg.batch_no }}</td>
          <td>{{ pkg.rule_no }}（#{{ pkg.rule_id }}）</td>
          <td>{{ pkg.status }}</td>
          <td>{{ pkg.exported_lines }}/{{ pkg.total_lines }}</td>
          <td>{{ pkg.delivered_lines }} / {{ pkg.blocked_lines }}</td>
          <td>{{ pkg.mapping_versions.join('、') || '—' }}</td>
          <td>
            <span v-if="pkg.version_conflicts.length" class="error-text">
              {{ pkg.version_conflicts.map((c) => c.mapping_version).join('、') }}
            </span>
            <span v-else>无</span>
          </td>
          <td>{{ pkg.download_logs.length ? pkg.download_logs.map((log) => log.file_id).join('、') : '—' }}</td>
          <td class="row-actions">
            <button class="link" type="button" data-testid="open-detail" @click="openPackage(pkg.id)">复核明细</button>
            <button class="link" type="button" data-testid="export-rows" @click="onExport(pkg.id)">续跑导出</button>
            <button class="link" type="button" data-testid="download-package" @click="onDownload(pkg.id)">下载文件</button>
          </td>
        </tr>
        <tr v-if="!packages.length">
          <td colspan="9" class="empty-state">暂无对账数据包，可先建立转换对账数据包</td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-package-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>建立转换对账数据包</h3>
        <label class="create-field">
          <span>转换规则ID</span>
          <input v-model="createForm.rule_id" data-testid="create-rule-id" required placeholder="已生效转换规则的 id" />
        </label>
        <label class="create-field">
          <span>批次号</span>
          <input v-model="createForm.batch_no" data-testid="create-batch-no" required placeholder="如 BATCH-20261120-01" />
        </label>
        <label class="create-field">
          <span>源记录（JSON 数组，按数组顺序固化来源行号）</span>
          <textarea
            v-model="createForm.records_text"
            data-testid="create-records"
            rows="8"
            required
            placeholder='[{"id":1,"name":"订单A"},{"id":2,"name":"订单B"}]'
          ></textarea>
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create-package" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认建立' }}
          </button>
        </div>
      </form>
    </div>

    <div v-if="activePackage" class="modal-backdrop" data-testid="detail-modal">
      <div class="modal modal-wide">
        <h3>复核明细 · 批次 {{ activePackage.batch_no }}</h3>
        <p class="page-desc">
          状态 {{ activePackage.status }} ｜ 映射版本 {{ activePackage.mapping_versions.join('、') || '—' }}
          ｜ 已复查 {{ activePackage.reviewed_lines }}/{{ activePackage.delivered_lines }} 行
        </p>

        <table class="data-table detail-table">
          <thead>
            <tr>
              <th>来源行号</th>
              <th>导出状态</th>
              <th>可交付</th>
              <th>映射版本/指纹</th>
              <th>质量结果</th>
              <th>已下载文件</th>
              <th>复查</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.source_line}`">
              <td>{{ row.source_line }}</td>
              <td>{{ row.export_state }}</td>
              <td>
                <span v-if="row.deliverable">是</span>
                <span v-else class="error-text">{{ row.block_reason || '否' }}</span>
              </td>
              <td>
                <div>{{ row.mapping_version || '未声明版本' }}</div>
                <small>{{ row.mapping_fingerprint }}</small>
              </td>
              <td>
                <span v-if="row.quality_passed === true">通过</span>
                <span v-else-if="row.quality_passed === false" class="error-text">不通过</span>
                <span v-else>—</span>
              </td>
              <td>{{ row.last_download_file_id || '—' }}</td>
              <td>
                <div v-if="row.deliverable && row.downloaded" class="row-actions">
                  <button class="link" type="button" @click="openReview(row.source_line, '通过')">通过</button>
                  <button class="link" type="button" @click="openReview(row.source_line, '驳回')">驳回</button>
                </div>
                <span v-else-if="!row.downloaded" class="page-desc">先下载文件</span>
                <span v-else class="error-text">不可交付</span>
              </td>
            </tr>
          </tbody>
        </table>

        <h4>失败字段（关联源记录）</h4>
        <table v-if="failures.length" class="data-table detail-table">
          <thead>
            <tr>
              <th>来源行号</th>
              <th>目标字段</th>
              <th>原始值</th>
              <th>转换</th>
              <th>校验说明</th>
              <th>映射版本</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(failure, index) in failures" :key="index">
              <td>{{ failure.source_line }}</td>
              <td>{{ failure.target_field }}</td>
              <td>{{ formatValue(failure.original_value) }}</td>
              <td>{{ failure.transform_ok ? '成功' : '失败' }}</td>
              <td>{{ failure.quality_message || failure.transform_message }}</td>
              <td>{{ failure.mapping_version }}</td>
            </tr>
          </tbody>
        </table>
        <p v-else class="page-desc">暂无失败字段</p>

        <h4>质检台账</h4>
        <table v-if="ledger.length" class="data-table detail-table">
          <thead>
            <tr>
              <th>来源行号</th>
              <th>结论</th>
              <th>复查人</th>
              <th>首次下载文件</th>
              <th>最近下载文件</th>
              <th>下载版本</th>
              <th>版本核对</th>
              <th>备注</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="entry in ledger" :key="String(entry.id)">
              <td>{{ entry.source_line }}</td>
              <td>{{ entry.conclusion }}</td>
              <td>{{ entry.reviewer }}</td>
              <td>{{ entry.first_download_file_id }}</td>
              <td>{{ entry.downloaded_file_id }}</td>
              <td>{{ entry.downloaded_mapping_version }}</td>
              <td>
                <span v-if="entry.version_match">一致</span>
                <span v-else class="error-text">下载后映射已变更</span>
              </td>
              <td>{{ entry.remark }}</td>
            </tr>
          </tbody>
        </table>
        <p v-else class="page-desc">暂无质检结论</p>

        <div class="page-actions">
          <button class="btn" type="button" @click="activePackage = null">关闭</button>
        </div>
      </div>
    </div>

    <div v-if="reviewForm.open" class="modal-backdrop" data-testid="review-modal">
      <form class="modal" @submit.prevent="submitReviewForm">
        <h3>质检复查 · 来源行 {{ reviewForm.source_line }}</h3>
        <p class="page-desc">结论将写入质检台账，并绑定已下载的数据文件版本。</p>
        <label class="create-field">
          <span>复查结论</span>
          <select v-model="reviewForm.conclusion" data-testid="review-conclusion">
            <option value="通过">通过</option>
            <option value="驳回">驳回</option>
          </select>
        </label>
        <label class="create-field">
          <span>复查人</span>
          <input v-model="reviewForm.reviewer" data-testid="review-reviewer" required />
        </label>
        <label class="create-field">
          <span>备注</span>
          <input v-model="reviewForm.remark" data-testid="review-remark" />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="reviewForm.open = false">取消</button>
          <button class="btn primary" data-testid="submit-review" type="submit">提交复查</button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ packages.length }} 个对账数据包</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  createReconPackage,
  downloadReconPackage,
  exportReconRows,
  listFailedFields,
  listLedger,
  listReconPackages,
  listReconRows,
  submitReview,
  type LedgerEntry,
  type ReconPackage,
  type ReconRow,
} from '@/api/recon-service'

const packages = ref<ReconPackage[]>([])
const rows = ref<ReconRow[]>([])
const failures = ref<Record<string, unknown>[]>([])
const ledger = ref<LedgerEntry[]>([])
const activePackage = ref<ReconPackage | null>(null)
const batchFilter = ref('')
const errorMessage = ref('')
const showCreate = ref(false)
const creating = ref(false)
const createForm = ref({ rule_id: '', batch_no: '', records_text: '' })
const reviewForm = ref({
  open: false,
  source_line: 0,
  conclusion: '通过',
  reviewer: '质检同事',
  remark: '',
})

function countByStatus(status: string): number {
  return packages.value.filter((pkg) => pkg.status === status).length
}

function formatValue(value: unknown): string {
  if (value === null || value === undefined) return '—'
  return typeof value === 'object' ? JSON.stringify(value) : String(value)
}

async function reloadPackages() {
  errorMessage.value = ''
  try {
    packages.value = await listReconPackages(batchFilter.value.trim())
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '对账数据包读取失败'
  }
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const sourceRecords = JSON.parse(createForm.value.records_text) as Record<string, unknown>[]
    if (!Array.isArray(sourceRecords)) throw new Error('源记录必须是 JSON 数组')
    const result = await createReconPackage({
      rule_id: Number(createForm.value.rule_id),
      batch_no: createForm.value.batch_no.trim(),
      source_records: sourceRecords,
    })
    showCreate.value = false
    createForm.value = { rule_id: '', batch_no: '', records_text: '' }
    await reloadPackages()
    if (result.reused) errorMessage.value = result.message
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '对账数据包建立失败'
  } finally {
    creating.value = false
  }
}

async function onExport(packageId: number) {
  errorMessage.value = ''
  try {
    const result = await exportReconRows(packageId)
    errorMessage.value = result.message
    await reloadPackages()
    if (activePackage.value?.id === packageId) await openPackage(packageId)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

async function onDownload(packageId: number) {
  errorMessage.value = ''
  try {
    await downloadReconPackage(packageId)
    await reloadPackages()
    if (activePackage.value?.id === packageId) await openPackage(packageId)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '下载失败'
  }
}

async function openPackage(packageId: number) {
  errorMessage.value = ''
  try {
    const [packageList, rowList, failureList, ledgerList] = await Promise.all([
      listReconPackages(batchFilter.value.trim()),
      listReconRows(packageId),
      listFailedFields(packageId),
      listLedger(packageId),
    ])
    packages.value = packageList
    activePackage.value = packageList.find((pkg) => pkg.id === packageId) ?? null
    rows.value = rowList
    failures.value = failureList
    ledger.value = ledgerList
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '复核明细读取失败'
  }
}

function openReview(sourceLine: number, conclusion: string) {
  reviewForm.value = {
    open: true,
    source_line: sourceLine,
    conclusion,
    reviewer: '质检同事',
    remark: '',
  }
}

async function submitReviewForm() {
  if (!activePackage.value) return
  errorMessage.value = ''
  try {
    const result = await submitReview({
      packageId: activePackage.value.id,
      sourceLine: reviewForm.value.source_line,
      conclusion: reviewForm.value.conclusion,
      reviewer: reviewForm.value.reviewer,
      remark: reviewForm.value.remark,
    })
    reviewForm.value.open = false
    await openPackage(activePackage.value.id)
    await reloadPackages()
    if (!result.version_match) errorMessage.value = result.message
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '复查提交失败'
  }
}

onMounted(reloadPackages)
</script>

<style scoped>
.modal-wide { width: min(1080px, 96vw); }
.detail-table { margin: 8px 0 16px; font-size: 12px; }
textarea { width: 100%; font-family: monospace; border: 1px solid var(--border); border-radius: 6px; padding: 8px; }
select { width: 100%; border: 1px solid var(--border); border-radius: 6px; padding: 8px; }
small { color: var(--muted); }
</style>
