import { clsx, type ClassValue } from 'clsx'
import { twMerge } from 'tailwind-merge'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function formatNumber(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`
  return n.toString()
}

export function formatBytes(bytes: number): string {
  if (bytes >= 1_073_741_824) return `${(bytes / 1_073_741_824).toFixed(2)} GB`
  if (bytes >= 1_048_576) return `${(bytes / 1_048_576).toFixed(2)} MB`
  if (bytes >= 1_024) return `${(bytes / 1_024).toFixed(2)} KB`
  return `${bytes} B`
}

export function formatDate(isoString: string): string {
  if (!isoString) return '-'
  return new Date(isoString).toLocaleString()
}

export const STATE_COLORS: Record<string, string> = {
  Stable:       'bg-green-100 text-green-800',
  Empty:        'bg-yellow-100 text-yellow-800',
  Dead:         'bg-red-100 text-red-800',
  PreparingRebalance: 'bg-orange-100 text-orange-800',
  CompletingRebalance: 'bg-blue-100 text-blue-800',
  Unknown:      'bg-gray-100 text-gray-800',
}

export function stateColor(state: string): string {
  return STATE_COLORS[state] ?? 'bg-gray-100 text-gray-800'
}
