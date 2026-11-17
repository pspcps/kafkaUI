'use client'

import { useState } from 'react'
import { Check, Copy } from 'lucide-react'

export function CopyButton({ text, label }: { text: string; label: string }) {
  const [status, setStatus] = useState<'idle' | 'copied' | 'error'>('idle')
  async function copy() {
    try {
      await navigator.clipboard.writeText(text)
      setStatus('copied')
    } catch {
      setStatus('error')
    }
  }
  return (
    <span className="inline-flex items-center gap-2">
      <button
        type="button"
        onClick={copy}
        aria-label={label}
        title={label}
        className="inline-flex items-center gap-1 rounded border border-gray-200 bg-white px-2 py-1 text-xs text-gray-600 hover:bg-gray-50 focus-visible:outline focus-visible:outline-2 focus-visible:outline-blue-600"
      >
        {status === 'copied' ? <Check className="h-3.5 w-3.5" /> : <Copy className="h-3.5 w-3.5" />}
        {status === 'copied' ? 'Copied' : 'Copy'}
      </button>
      <span role="status" className={status === 'error' ? 'text-xs text-red-600' : 'sr-only'}>
        {status === 'copied'
          ? `${label}: copied to clipboard`
          : status === 'error'
            ? 'Clipboard unavailable. Select and copy the text manually.'
            : ''}
      </span>
    </span>
  )
}
