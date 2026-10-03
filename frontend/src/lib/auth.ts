const TOKEN_KEY = 'kafbat_token'
const USER_KEY = 'kafbat_user'

export type AuthUser = {
  id: string
  username: string
  role: 'admin' | 'viewer'
}

export const getToken = (): string | null => {
  if (typeof window === 'undefined') return null
  return window.localStorage.getItem(TOKEN_KEY)
}

export const setToken = (token: string) => {
  if (typeof window === 'undefined') return
  window.localStorage.setItem(TOKEN_KEY, token)
}

export const getUser = (): AuthUser | null => {
  if (typeof window === 'undefined') return null
  const raw = window.localStorage.getItem(USER_KEY)
  if (!raw) return null
  try {
    return JSON.parse(raw) as AuthUser
  } catch {
    return null
  }
}

export const setUser = (user: AuthUser) => {
  if (typeof window === 'undefined') return
  window.localStorage.setItem(USER_KEY, JSON.stringify(user))
}

export const clearAuth = () => {
  if (typeof window === 'undefined') return
  window.localStorage.removeItem(TOKEN_KEY)
  window.localStorage.removeItem(USER_KEY)
}

export const isAuthenticated = (): boolean => !!getToken()

export const logout = () => {
  clearAuth()
  if (typeof window !== 'undefined') {
    window.location.href = '/login'
  }
}
