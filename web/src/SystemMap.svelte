<script>
  import { onMount, onDestroy } from 'svelte'
  import { get } from 'svelte/store'
  import { pods, endpoints, podRps } from './store.js'
  import PodNode from './PodNode.svelte'

  // ---- fixed layout ----
  const W = 1000, H = 400
  const GEN  = { x: 80,  y: 200, w: 100, h: 50, label: 'load gen',  sub: 'in-cluster' }
  const ING  = { x: 280, y: 200, w: 130, h: 50, label: 'ingress',   sub: 'nginx' }
  const SVC  = { x: 490, y: 200, w: 130, h: 50, label: 'service',   sub: 'demo-svc' }
  const PODS_X = 730
  const POD_H  = 56
  const POD_W  = 130

  // Dynamic pod layout
  let podLayout = $derived.by(() => {
    const list = Object.values($pods)
    if (list.length === 0) return []
    const totalH = list.length * POD_H + (list.length - 1) * 8
    const startY = H / 2 - totalH / 2 + POD_H / 2
    return list.map((pod, i) => ({
      pod,
      x: PODS_X,
      y: startY + i * (POD_H + 8),
    }))
  })

  // SVG ref for getPointAtLength
  let svgEl = $state(null)

  // ---- dot animation ----
  const SPEED = 0.38 // viewBox units per ms
  let dots = []
  let raf = 0
  let dotPool = []

  // ---- signal pulse animation ----
  let pulses = []
  const SIGNAL_CLS = {
    SIGTERM:         'del',
    PROBE_FAIL:      'warn',
    ENDPOINT_REMOVE: 'warn',
    PROBE_OK:        'add',
    ENDPOINT_ADD:    'add',
    DRAINED:         'add',
  }

  function edgePath(a, b) {
    const x1 = a.x + a.w / 2, x2 = b.x - b.w / 2
    const y1 = a.y, y2 = b.y
    const mx = (x1 + x2) / 2
    return `M${x1} ${y1} C${mx} ${y1} ${mx} ${y2} ${x2} ${y2}`
  }

  // Edges between fixed nodes
  const fixedEdges = $derived([
    { id: 'gen-ing', d: edgePath(GEN, ING) },
    { id: 'ing-svc', d: edgePath(ING, SVC) },
  ])

  // Edges from SVC to each pod
  let podEdges = $derived(
    podLayout.map(({ pod, x, y }) => ({
      id: `svc-${pod.name}`,
      podName: pod.name,
      d: edgePath(SVC, { x, y, w: POD_W, h: POD_H }),
    }))
  )

  // All path elements indexed by edge id (populated by bind:this on <path>)
  let pathEls = {}

  export function spawnDot(podName, cls) {
    if (!svgEl) return
    const podEdge = podEdges.find(e => e.podName === podName)
    if (!podEdge) return

    const segIds = ['gen-ing', 'ing-svc', `svc-${podName}`]
    const segs = segIds.map(id => pathEls[id]).filter(Boolean).map(el => ({
      el,
      len: el.getTotalLength(),
    }))
    if (segs.some(s => s.len === 0)) return

    const c = dotPool.pop() || document.createElementNS('http://www.w3.org/2000/svg', 'circle')
    c.setAttribute('r', '4.5')
    c.setAttribute('class', `dot ${cls}`)
    c.setAttribute('cx', '-20')
    svgEl.querySelector('.dot-layer').appendChild(c)

    dots.push({
      c,
      segs,
      total: segs.reduce((a, s) => a + s.len, 0),
      start: performance.now(),
    })
    if (!raf) raf = requestAnimationFrame(tick)
  }

  function tick(now) {
    raf = 0
    for (let i = dots.length - 1; i >= 0; i--) {
      const d = dots[i]
      let dist = (now - d.start) * SPEED
      if (dist < 0) continue
      if (dist >= d.total) {
        d.c.remove()
        dotPool.push(d.c)
        dots.splice(i, 1)
        continue
      }
      let s = 0
      while (s < d.segs.length - 1 && dist > d.segs[s].len) {
        dist -= d.segs[s].len
        s++
      }
      const localDist = Math.min(dist, d.segs[s].len)
      const pathPos = d.segs[s].reversed ? d.segs[s].len - localDist : localDist
      const pt = d.segs[s].el.getPointAtLength(Math.max(0, pathPos))
      d.c.setAttribute('cx', pt.x.toFixed(1))
      d.c.setAttribute('cy', pt.y.toFixed(1))
    }
    for (let i = pulses.length - 1; i >= 0; i--) {
      const p = pulses[i]
      const t = Math.min((now - p.start) / p.duration, 1)
      if (t >= 1) { p.c.remove(); pulses.splice(i, 1); continue }
      p.c.setAttribute('r', (2 + 40 * t).toFixed(1))
      p.c.setAttribute('opacity', (1 - t).toFixed(3))
    }
    if (dots.length || pulses.length) raf = requestAnimationFrame(tick)
  }

  export function spawnReturnDot(podName, cls) {
    if (!svgEl) return
    const podEdge = podEdges.find(e => e.podName === podName)
    if (!podEdge) return

    // Travel in reverse: pod → SVC → ingress → load gen
    const segIds = [`svc-${podName}`, 'ing-svc', 'gen-ing']
    const segs = segIds.map(id => pathEls[id]).filter(Boolean).map(el => ({
      el,
      len: el.getTotalLength(),
      reversed: true,
    }))
    if (segs.length < segIds.length || segs.some(s => s.len === 0)) return

    const c = dotPool.pop() || document.createElementNS('http://www.w3.org/2000/svg', 'circle')
    c.setAttribute('r', '3')
    c.setAttribute('class', `dot ret ${cls}`)
    c.setAttribute('cx', '-20')
    svgEl.querySelector('.dot-layer').appendChild(c)

    dots.push({
      c,
      segs,
      total: segs.reduce((a, s) => a + s.len, 0),
      start: performance.now(),
    })
    if (!raf) raf = requestAnimationFrame(tick)
  }

  // Listen to request events from the SSE stream via a custom event
  function handleRequest(ev) {
    const { podName, status, slow } = ev.detail
    const cls = slow ? 'warn' : status >= 200 && status < 300 ? 'add' : 'del'
    // Forward dot only if pod is currently in the endpoint slice
    if (get(endpoints).includes(podName)) spawnDot(podName, cls)
    // Return dot always — pod still responds even after endpoint removal (graceful drain)
    spawnReturnDot(podName, cls)
  }

  function spawnPulse(x, y, cls, duration = 700) {
    if (!svgEl) return
    const c = document.createElementNS('http://www.w3.org/2000/svg', 'circle')
    c.setAttribute('cx', x.toFixed(1))
    c.setAttribute('cy', y.toFixed(1))
    c.setAttribute('r', '2')
    c.setAttribute('class', `pulse ${cls}`)
    svgEl.querySelector('.pulse-layer').appendChild(c)
    pulses.push({ c, start: performance.now(), duration })
    if (!raf) raf = requestAnimationFrame(tick)
  }

  function handleSignal(ev) {
    const cls = SIGNAL_CLS[ev.detail.kind] ?? 'muted'
    const pl = podLayout.find(e => e.pod.name === ev.detail.podName)
    if (!pl) return
    spawnPulse(pl.x, pl.y, cls)
    // Double ring for SIGTERM to make it more visible
    if (ev.detail.kind === 'SIGTERM') spawnPulse(pl.x, pl.y, cls, 1100)
  }

  onMount(() => {
    window.addEventListener('demo:request', handleRequest)
    window.addEventListener('demo:signal', handleSignal)
  })
  onDestroy(() => {
    window.removeEventListener('demo:request', handleRequest)
    window.removeEventListener('demo:signal', handleSignal)
    if (raf) cancelAnimationFrame(raf)
  })
</script>

<svg bind:this={svgEl} viewBox="0 0 {W} {H}" class="sysmap"
  role="img" aria-label="Traffic flow diagram">
  <!-- column labels -->
  <text x={GEN.x}   y="22" text-anchor="middle" class="col-label">LOAD GEN</text>
  <text x={ING.x}   y="22" text-anchor="middle" class="col-label">INGRESS</text>
  <text x={SVC.x}   y="22" text-anchor="middle" class="col-label">SERVICE</text>
  <text x={PODS_X}  y="22" text-anchor="middle" class="col-label">PODS</text>

  <!-- fixed edges -->
  {#each fixedEdges as edge (edge.id)}
    <path bind:this={pathEls[edge.id]} d={edge.d} class="edge" />
  {/each}

  <!-- pod edges -->
  {#each podEdges as edge (edge.id)}
    <path bind:this={pathEls[edge.id]} d={edge.d}
      class="edge {$endpoints.includes(edge.podName) ? 'active' : 'inactive'}" />
  {/each}

  <!-- fixed nodes -->
  {#each [GEN, ING, SVC] as node}
    <g>
      <rect x={node.x - node.w/2} y={node.y - node.h/2}
        width={node.w} height={node.h} rx="6" class="node-rect" />
      <text x={node.x} y={node.y - 5} text-anchor="middle" class="t1">{node.label}</text>
      <text x={node.x} y={node.y + 11} text-anchor="middle" class="t2">{node.sub}</text>
      {#if node === SVC && $endpoints.length > 0}
        <text x={node.x} y={node.y + 24} text-anchor="middle" class="t3">
          {$endpoints.length} endpoint{$endpoints.length !== 1 ? 's' : ''}
        </text>
      {/if}
    </g>
  {/each}

  <!-- dynamic pod nodes -->
  {#each podLayout as { pod, x, y } (pod.name)}
    <PodNode {pod} {x} {y} w={POD_W} h={POD_H} rps={$podRps[pod.name] ?? 0} />
  {/each}

  <!-- empty state -->
  {#if podLayout.length === 0}
    <text x={PODS_X} y={H/2} text-anchor="middle" class="empty-hint">
      Spawn a pod →
    </text>
  {/if}

  <!-- dot layer (dots appended here programmatically) -->
  <g class="pulse-layer" aria-hidden="true"></g>
  <g class="dot-layer" aria-hidden="true"></g>
</svg>

<style>
  .sysmap { width: 100%; height: 100%; display: block; color: var(--text); }
  .col-label { font: 400 11px var(--font-head); fill: var(--faint); letter-spacing: .1em; }
  .edge { fill: none; stroke: var(--border-strong); stroke-width: 1.6; }
  .edge.active { stroke: color-mix(in srgb, var(--add) 55%, var(--border-strong)); stroke-width: 2; }
  .edge.inactive { stroke: var(--border-strong); stroke-dasharray: 5 4; opacity: .6; }
  .node-rect { fill: var(--surface-2); stroke: var(--border-strong); stroke-width: 1.2; }
  .t1 { font: 700 13px var(--font-body); fill: var(--text); }
  .t2 { font: 500 10.5px var(--font-mono); fill: var(--muted); }
  .t3 { font: 500 9.5px var(--font-mono); fill: var(--faint); }
  .empty-hint { font: 400 13px var(--font-body); fill: var(--muted); }
  :global(.dot) { pointer-events: none; }
  :global(.dot.add)  { fill: var(--add); }
  :global(.dot.del)  { fill: var(--del); }
  :global(.dot.warn) { fill: var(--warn); }
  :global(.dot.ret)  { opacity: 0.55; }
  :global(.pulse) { fill: none; stroke-width: 2.5; pointer-events: none; }
  :global(.pulse.add)  { stroke: var(--add); }
  :global(.pulse.del)  { stroke: var(--del); }
  :global(.pulse.warn) { stroke: var(--warn); }
  :global(.pulse.muted) { stroke: var(--muted); }
</style>
