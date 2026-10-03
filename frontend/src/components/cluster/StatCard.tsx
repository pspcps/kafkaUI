import { LucideIcon } from 'lucide-react'
import { cn } from '@/lib/utils'

interface StatCardProps {
  label: string
  value: string | number
  icon: LucideIcon
  iconColor?: string
  iconBg?: string
}

export function StatCard({ label, value, icon: Icon, iconColor = 'text-blue-600', iconBg = 'bg-blue-50' }: StatCardProps) {
  return (
    <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-6 flex items-center gap-4">
      <div className={cn('p-3 rounded-lg flex-shrink-0', iconBg)}>
        <Icon className={cn('w-6 h-6', iconColor)} />
      </div>
      <div>
        <p className="text-sm text-gray-500">{label}</p>
        <p className="text-2xl font-bold text-gray-900 mt-0.5">{value}</p>
      </div>
    </div>
  )
}
