import { BrokerInfo } from '@/types'
import { Badge } from '@/components/ui/Badge'
import { Table, TableHead, TableBody, Th, Td } from '@/components/ui/Table'
import { Crown } from 'lucide-react'

export function BrokerTable({ brokers }: { brokers: BrokerInfo[] }) {
  return (
    <Table>
      <TableHead>
        <tr>
          <Th>Broker ID</Th>
          <Th>Host</Th>
          <Th>Role</Th>
        </tr>
      </TableHead>
      <TableBody>
        {brokers.map((b) => (
          <tr key={b.id} className="hover:bg-gray-50">
            <Td>
              <span className="font-mono text-gray-900">#{b.id}</span>
            </Td>
            <Td>
              <span className="font-mono text-sm">{b.host}</span>
            </Td>
            <Td>
              {b.isLeader ? (
                <Badge variant="info" className="gap-1">
                  <Crown className="w-3 h-3" /> Controller
                </Badge>
              ) : (
                <Badge variant="default">Broker</Badge>
              )}
            </Td>
          </tr>
        ))}
      </TableBody>
    </Table>
  )
}
