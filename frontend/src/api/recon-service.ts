/** 转换对账 API：数据包导出、版本化文件下载与质检台账。 */
import { request } from './client'

export type ReconPackage = {
  id: number
  rule_id: number
  rule_no: string
  batch_no: string
  status: string
  total_lines: number
  exported_lines: number
  delivered_lines: number
  blocked_lines: number
  reviewed_lines: number
  mapping_versions: string[]
  version_conflicts: { mapping_version: string; fingerprints: string[] }[]
  download_logs: { file_id: string; downloaded_at: string; mapping_versions: string[]; source_lines: number[] }[]
  created_at: string
  updated_at: string
}

export type ReconRow = {
  id: number
  package_id: number
  source_line: number
  export_state: string
  deliverable: boolean
  block_reason: string
  mapping_version: string
  mapping_fingerprint: string
  quality_passed: boolean | null
  downloaded: boolean
  last_download_file_id: string
  failed_fields: Record<string, unknown>[]
  field_traces: Record<string, unknown>[]
  source_record: Record<string, unknown>
}

export type LedgerEntry = {
  id: number
  package_id: number
  batch_no: string
  source_line: number
  conclusion: string
  reviewer: string
  remark: string
  downloaded_file_id: string
  first_download_file_id: string
  downloaded_mapping_version: string
  downloaded_fingerprint: string
  download_history: { file_id: string; mapping_version: string; mapping_fingerprint: string; downloaded_at: string }[]
  current_rule_version: string
  current_fingerprint: string
  version_match: boolean
  reviewed_at: string
}

async function readJson<T>(response: Response, fallbackMessage: string): Promise<T> {
  if (!response.ok) {
    let detail = fallbackMessage
    try {
      detail = (await response.json()).detail ?? detail
    } catch {
      /* 保留兜底文案 */
    }
    throw new Error(detail)
  }
  return (await response.json()) as T
}

export async function listReconPackages(batchNo = ''): Promise<ReconPackage[]> {
  const query = batchNo ? `?batch_no=${encodeURIComponent(batchNo)}` : ''
  const response = await request(`/api/transform-recon/packages${query}`)
  const payload = await readJson<{ items: ReconPackage[] }>(response, '对账数据包读取失败')
  return payload.items
}

export async function createReconPackage(input: {
  rule_id: number
  batch_no: string
  source_records: Record<string, unknown>[]
}): Promise<{ package: ReconPackage; reused: boolean; message: string }> {
  const response = await request('/api/transform-recon/packages', {
    method: 'POST',
    body: JSON.stringify(input),
  })
  return readJson(response, '对账数据包建立失败')
}

export async function exportReconRows(
  packageId: number,
  limit?: number,
): Promise<{ exported_now: number; message: string; package: ReconPackage }> {
  const response = await request(`/api/transform-recon/packages/${packageId}/export`, {
    method: 'POST',
    body: JSON.stringify(limit ? { limit } : {}),
  })
  return readJson(response, '对账数据导出失败')
}

export async function listReconRows(packageId: number): Promise<ReconRow[]> {
  const response = await request(`/api/transform-recon/packages/${packageId}/rows`)
  const payload = await readJson<{ rows: ReconRow[] }>(response, '复核数据读取失败')
  return payload.rows
}

export async function listFailedFields(
  packageId: number,
): Promise<Record<string, unknown>[]> {
  const response = await request(`/api/transform-recon/packages/${packageId}/failed-fields`)
  const payload = await readJson<{ failures: Record<string, unknown>[] }>(response, '失败字段读取失败')
  return payload.failures
}

export async function downloadReconPackage(packageId: number): Promise<void> {
  const response = await request(`/api/transform-recon/packages/${packageId}/download`)
  if (!response.ok) {
    let detail = '对账文件下载失败，只有明确版本的文件可交付'
    try {
      detail = (await response.json()).detail ?? detail
    } catch {
      /* 保留兜底文案 */
    }
    throw new Error(detail)
  }
  const disposition = response.headers.get('Content-Disposition') ?? ''
  const match = disposition.match(/filename="?([^"]+)"?/)
  const filename = match ? match[1] : `recon-package-${packageId}.zip`
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = filename
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function submitReview(input: {
  packageId: number
  sourceLine: number
  conclusion: string
  reviewer: string
  remark: string
}): Promise<{ message: string; version_match: boolean }> {
  const response = await request(`/api/transform-recon/packages/${input.packageId}/reviews`, {
    method: 'POST',
    body: JSON.stringify({
      source_line: input.sourceLine,
      conclusion: input.conclusion,
      reviewer: input.reviewer,
      remark: input.remark,
    }),
  })
  return readJson(response, '质检复查提交失败')
}

export async function listLedger(packageId?: number): Promise<LedgerEntry[]> {
  const query = packageId ? `?package_id=${packageId}` : ''
  const response = await request(`/api/transform-recon/ledger${query}`)
  const payload = await readJson<{ items: LedgerEntry[] }>(response, '质检台账读取失败')
  return payload.items
}
