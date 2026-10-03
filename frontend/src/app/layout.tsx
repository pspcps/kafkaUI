import type { Metadata } from 'next'
import { Inter } from 'next/font/google'
import './globals.css'
import { AuthGate } from '@/components/AuthGate'
import { Providers } from './providers'

const inter = Inter({ subsets: ['latin'] })

export const metadata: Metadata = {
  title: 'KafkaUI',
  description: 'Kafka Cluster Management UI',
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className={inter.className}>
        <Providers>
          <AuthGate>{children}</AuthGate>
        </Providers>
      </body>
    </html>
  )
}
