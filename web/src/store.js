import { writable, derived } from 'svelte/store'

// ---- reactive state ----
export const clusterState = writable('creating') // 'creating' | 'ready' | 'destroying'
export const pods = writable({})         // name → PodEvent
export const endpoints = writable([])    // string[]
export const stats = writable(null)      // StatsEvent | null
export const signals = writable([])      // SignalEvent[] (last 50)
export const deployment = writable(null) // DeploymentEvent | null

// ---- derived ----
export const podList = derived(pods, $pods => Object.values($pods))

// ---- per-pod request rate (rolling 5s window) ----
let podSamples = [] // { ts: number, podName: string }
export const podRps = writable({}) // podName → req/s (float)

setInterval(() => {
  const cutoff = Date.now() - 5000
  podSamples = podSamples.filter(s => s.ts > cutoff)
  const counts = {}
  for (const { podName } of podSamples) counts[podName] = (counts[podName] || 0) + 1
  const result = {}
  for (const [name, count] of Object.entries(counts)) result[name] = count / 5
  podRps.set(result)
}, 500)

// ---- SSE connection ----
let es = null

export function connectSSE() {
  if (es) return
  es = new EventSource('/api/stream')
  es.onmessage = (e) => {
    let ev
    try { ev = JSON.parse(e.data) } catch { return }
    switch (ev.type) {
      case 'cluster':
        clusterState.set(ev.state)
        break
      case 'pod':
        if (ev.deleted) {
          pods.update(p => { const n = { ...p }; delete n[ev.name]; return n })
        } else {
          pods.update(p => ({ ...p, [ev.name]: ev }))
        }
        break
      case 'endpoints':
        endpoints.set(ev.pods ?? [])
        break
      case 'stats':
        stats.set(ev)
        break
      case 'signal':
        signals.update(s => [ev, ...s].slice(0, 50))
        window.dispatchEvent(new CustomEvent('demo:signal', { detail: ev }))
        break
      case 'deployment':
        deployment.set(ev)
        break
      case 'request':
        if (ev.podName) podSamples.push({ ts: Date.now(), podName: ev.podName })
        window.dispatchEvent(new CustomEvent('demo:request', { detail: ev }))
        break
    }
  }
  es.onerror = () => {
    es.close()
    es = null
    setTimeout(connectSSE, 2000) // reconnect
  }
}

// ---- API helpers ----
async function apiPost(path, body) {
  const r = await fetch(path, {
    method: 'POST',
    headers: body ? { 'Content-Type': 'application/json' } : {},
    body: body ? JSON.stringify(body) : undefined,
  })
  if (!r.ok) throw new Error(`${path}: HTTP ${r.status}`)
  return r
}
async function apiPut(path, body) {
  const r = await fetch(path, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!r.ok) throw new Error(`${path}: HTTP ${r.status}`)
  return r
}

export const api = {
  setReplicas:    (n)   => apiPut('/api/deployment/replicas', { replicas: n }),
  setStrategy:    (s)   => apiPut('/api/deployment/strategy', { strategy: s }),
  setGrace:       (sec) => apiPut('/api/deployment/grace',    { seconds: sec }),
  setPreStop:     (sec) => apiPut('/api/deployment/prestop',  { seconds: sec }),
  triggerRollout: ()    => apiPost('/api/deployment/rollout'),
  setPodReady:       (name, ready) => apiPut(`/api/pod/${name}/ready`, { ready }),
  setPodDelay:       (name, ms) => apiPut(`/api/pod/${name}/delay`, { ms }),
  startTraffic:      () => apiPost('/api/traffic/start'),
  stopTraffic:       () => apiPost('/api/traffic/stop'),
  setRPS:            (rps) => apiPut('/api/traffic/rps', { rps }),
  setDuration:       (ms) => apiPut('/api/traffic/duration', { ms }),
  destroyCluster:    () => apiPost('/api/cluster/destroy'),
}
