<script>
  let { pod, x, y, w = 130, h = 50, rps = 0 } = $props()

  const borderClass = $derived(
    pod.terminating ? 'term' : pod.ready ? 'ready' : 'notready'
  )
  const label = $derived(
    pod.terminating
      ? (pod.inFlight > 0 ? `TERM · ${pod.inFlight} in-flight` : `TERM ${pod.graceRemaining}s`)
      : pod.ready ? 'ready' : 'NOT READY'
  )
</script>

<g transform="translate({x - w/2} {y - h/2})">
  <rect width={w} height={h} rx="6" class="pod-rect {borderClass}" />
  <text x={w/2} y={h/2 - 5} text-anchor="middle" class="t1">{pod.name}</text>
  <text x={w/2} y={h/2 + 11} text-anchor="middle" class="t2 {borderClass}">{label}</text>
  {#if rps >= 0.1}
    <text x={w + 10} y={h/2 + 4} text-anchor="start" class="rps">{rps.toFixed(1)}/s</text>
  {/if}
</g>

<style>
  .pod-rect { fill: var(--surface-2); stroke-width: 1.8; }
  .pod-rect.ready { stroke: var(--add); }
  .pod-rect.notready { stroke: var(--warn); stroke-dasharray: 4 3; }
  .pod-rect.term { stroke: var(--del); stroke-dasharray: 4 3; }
  .t1 { font: 700 11px var(--font-mono); fill: var(--text); }
  .t2 { font: 500 10px var(--font-body); }
  .t2.ready { fill: var(--add); }
  .t2.notready { fill: var(--warn); }
  .t2.term { fill: var(--del); }
  .rps { font: 600 9.5px var(--font-mono); fill: var(--muted); }
</style>
