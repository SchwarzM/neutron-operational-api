<script>
  import { pods, signals, deployment, stats, api } from './store.js'

  // Deployment controls — synced from backend once it arrives
  let strategy = $state('surge')
  let grace    = $state(30)
  let preStop  = $state(0)

  $effect(() => {
    if ($deployment) {
      strategy = $deployment.strategy
      grace    = $deployment.gracePeriod
      preStop  = $deployment.preStopSleep
    }
  })

  async function scaleUp() {
    await api.setReplicas(($deployment?.replicas ?? 2) + 1)
  }
  async function scaleDown() {
    const cur = $deployment?.replicas ?? 2
    if (cur > 1) await api.setReplicas(cur - 1)
  }
  async function applyStrategy() {
    await api.setStrategy(strategy)
  }
  async function applyGrace() {
    await api.setGrace(Number(grace))
  }
  async function applyPreStop() {
    await api.setPreStop(Number(preStop))
  }
  async function rollout() {
    await api.triggerRollout()
  }

  // Per-pod
  let podPending = $state({}) // podName → true while async call in flight
  async function toggleReady(pod) {
    podPending = { ...podPending, [pod.name]: true }
    try { await api.setPodReady(pod.name, !pod.ready) }
    finally { podPending = { ...podPending, [pod.name]: false } }
  }
  async function setDelay(name, ms) {
    await api.setPodDelay(name, ms)
  }

  // Traffic — trafficRunning is authoritative from the server via stats SSE
  let trafficRunning = $derived($stats?.running ?? false)
  let rps            = $state(5)
  let durationMs     = $state(0)

  async function toggleTraffic() {
    if (trafficRunning) {
      await api.stopTraffic()
    } else {
      await api.startTraffic()
    }
  }
  async function applyRPS() {
    await api.setRPS(Number(rps))
  }
  async function applyDuration() {
    await api.setDuration(Number(durationMs))
  }

  // Session destroy + reset
  let destroying     = $state(false)
  let destroyConfirm = $state(false)
  async function destroy() {
    destroying = true
    destroyConfirm = false
    await api.destroyCluster()
    window.location.href = '/'
  }

  const signalColors = {
    PROBE_OK: 'add', PROBE_FAIL: 'warn',
    ENDPOINT_ADD: 'frame', ENDPOINT_REMOVE: 'warn',
    SIGTERM: 'del', DRAINED: 'frame',
  }
</script>

<div class="sidebar">
  <!-- Deployment -->
  <section class="grp">
    <h3>Deployment</h3>
    <div class="row">
      <button class="btn small" onclick={scaleDown}
        disabled={!$deployment || $deployment.replicas <= 1}>−</button>
      <span class="replica-count mono">{$deployment?.replicas ?? '…'}</span>
      <button class="btn small" onclick={scaleUp}
        disabled={!$deployment}>+</button>
      <button class="btn primary" onclick={rollout}
        disabled={!$deployment || $deployment.rolloutInProgress}>
        {$deployment?.rolloutInProgress ? 'rolling…' : 'Trigger rollout'}
      </button>
    </div>
    <div class="row">
      <label>Grace</label>
      <select class="sel" bind:value={grace} onchange={applyGrace} disabled={!$deployment}>
        <option value={0}>0s</option>
        <option value={5}>5s</option>
        <option value={15}>15s</option>
        <option value={30}>30s</option>
        <option value={60}>60s</option>
      </select>
    </div>
    <div class="row">
      <label>preStop</label>
      <select class="sel" bind:value={preStop} onchange={applyPreStop} disabled={!$deployment}>
        <option value={0}>none</option>
        <option value={3}>3s</option>
        <option value={5}>5s</option>
        <option value={10}>10s</option>
        <option value={15}>15s</option>
        <option value={20}>20s</option>
      </select>
    </div>
    <div class="row">
      <label>Strategy</label>
      <select class="sel" bind:value={strategy} onchange={applyStrategy} disabled={!$deployment}>
        <option value="surge">surge — zero downtime</option>
        <option value="rolling">rolling — one at a time</option>
        <option value="recreate">recreate — full restart</option>
      </select>
    </div>
    {#if $deployment}
      <p class="dep-status">
        {#if $deployment.rolloutInProgress}
          rolling out: {$deployment.updatedReplicas}/{$deployment.replicas} updated
        {:else}
          {$deployment.readyReplicas}/{$deployment.replicas} ready
        {/if}
      </p>
    {/if}
  </section>

  <!-- Pods -->
  <section class="grp">
    <h3>Pods</h3>
    {#each Object.values($pods) as pod (pod.name)}
      {@const elapsed = pod.gracePeriod - pod.graceRemaining}
      {@const inPreStop = pod.terminating && pod.preStopSleep > 0 && elapsed < pod.preStopSleep}
      {@const preStopRemaining = pod.preStopSleep - elapsed}
      <div class="pod-row">
        <span class="pod-name mono">{pod.name}</span>
        <span class="badge {pod.terminating ? 'err' : pod.ready ? 'ok' : 'warn'}">
          {#if inPreStop}
            pre-stop {preStopRemaining}s
          {:else if pod.terminating}
            term {pod.graceRemaining}s
          {:else if pod.ready}
            ready
          {:else}
            not ready
          {/if}
        </span>
        {#if !pod.terminating && pod.preStopSleep > 0}
          <span class="badge muted">pre:{pod.preStopSleep}s</span>
        {/if}
        <div class="pod-actions">
          <button class="btn small" onclick={() => toggleReady(pod)}
            disabled={pod.terminating || podPending[pod.name]}>
            {podPending[pod.name] ? '…' : pod.ready ? 'set not-ready' : 'set ready'}
          </button>
          <select class="sel" onchange={e => setDelay(pod.name, Number(e.target.value))}
            disabled={pod.terminating}>
            <option value="0">fast</option>
            <option value="500">500ms</option>
            <option value="2000">2s</option>
          </select>
        </div>
      </div>
    {/each}
    {#if Object.keys($pods).length === 0}
      <p class="empty">No pods yet.</p>
    {/if}
  </section>

  <!-- Traffic -->
  <section class="grp">
    <h3>Traffic</h3>
    <div class="row">
      <button class="btn {trafficRunning ? 'danger' : 'primary'}" onclick={toggleTraffic}>
        {trafficRunning ? 'Stop traffic' : 'Start traffic'}
      </button>
    </div>
    <div class="row">
      <label>req/s</label>
      <input type="number" min="1" max="50" bind:value={rps} onchange={applyRPS} class="input-num">
    </div>
    <div class="row">
      <label>duration</label>
      <select class="sel" bind:value={durationMs} onchange={applyDuration}>
        <option value={0}>fast</option>
        <option value={500}>500ms</option>
        <option value={2000}>2s</option>
      </select>
    </div>
  </section>

  <!-- Session -->
  <section class="grp">
    <h3>Session</h3>
    {#if !destroyConfirm}
      <button class="btn danger" onclick={() => destroyConfirm = true}
        disabled={destroying}>
        {destroying ? 'Resetting…' : 'New session…'}
      </button>
    {:else}
      <p class="confirm-q">Delete this session's namespace and start fresh?</p>
      <div class="row">
        <button class="btn danger" onclick={destroy}>Confirm</button>
        <button class="btn" onclick={() => destroyConfirm = false}>Cancel</button>
      </div>
    {/if}
  </section>

  <!-- K8s events -->
  <section class="grp events">
    <h3>K8s events</h3>
    <ol class="event-log">
      {#each $signals as sig, i (i)}
        <li class="ev {signalColors[sig.kind] ?? 'muted'}">
          <span class="ev-kind mono">{sig.kind}</span>
          <span class="ev-msg">{sig.message}</span>
        </li>
      {/each}
    </ol>
  </section>
</div>

<style>
  .sidebar { padding: var(--s3); display: grid; gap: var(--s4); align-content: start; }
  .grp { display: grid; gap: var(--s2); }
  h3 { font: 400 .7rem var(--font-head); text-transform: uppercase; letter-spacing: .08em;
    color: var(--muted); }
  .btn {
    display: inline-flex; align-items: center; justify-content: center; gap: 6px;
    min-height: 30px; padding: 4px 10px; border-radius: var(--radius);
    border: 1px solid var(--border-strong); background: var(--surface-2);
    font-size: .82rem; font-weight: 600; white-space: nowrap;
    transition: background var(--dur);
  }
  .btn:hover:not([disabled]) { background: var(--surface-3); border-color: var(--accent); }
  .btn[disabled] { opacity: .5; cursor: not-allowed; }
  .btn.primary { background: var(--accent); color: var(--accent-ink); border-color: var(--accent); }
  .btn.danger { color: var(--del); border-color: var(--del); }
  .btn.danger:hover:not([disabled]) { background: var(--del-bg); }
  .btn.small { font-size: .75rem; min-height: 24px; padding: 2px 7px; }
  .sel { background: var(--surface-2); border: 1px solid var(--border-strong);
    border-radius: var(--radius); padding: 3px 5px; font-size: .78rem; color: inherit; }
  .input-num { background: var(--surface-2); border: 1px solid var(--border-strong);
    border-radius: var(--radius); padding: 3px 5px; width: 60px; font: inherit; color: inherit; }
  .row { display: flex; gap: var(--s2); align-items: center; flex-wrap: wrap; }
  .row label { font-size: .75rem; color: var(--muted); width: 4.5rem; flex: none; }
  .pod-row { display: grid; gap: 4px; padding: var(--s2); background: var(--surface-2);
    border: 1px solid var(--border); border-radius: var(--radius); }
  .pod-name { font-size: .78rem; font-family: var(--font-mono); }
  .pod-actions { display: flex; gap: 4px; flex-wrap: wrap; }
  .badge { font: 700 .65rem var(--font-body); letter-spacing: .05em; text-transform: uppercase;
    padding: 2px 6px; border-radius: 3px; border: 1px solid currentColor; }
  .badge.ok    { color: var(--add); }
  .badge.warn  { color: var(--warn); }
  .badge.err   { color: var(--del); }
  .badge.muted { color: var(--muted); }
  .empty { font-size: .78rem; color: var(--muted); }
  .confirm-q { font-size: .82rem; }
  .mono { font-family: var(--font-mono); }
  .events { border-top: 1px solid var(--border); padding-top: var(--s3); }
  .event-log { list-style: none; display: grid; gap: 3px; max-height: 300px; overflow-y: auto; }
  .ev { font-size: .75rem; padding: 3px 6px 3px 8px; border-left: 3px solid;
    background: var(--surface-2); border-radius: 0 3px 3px 0; display: grid;
    grid-template-columns: auto 1fr; gap: 0 6px; }
  .ev.add { border-left-color: var(--add); }
  .ev.warn { border-left-color: var(--warn); }
  .ev.del { border-left-color: var(--del); }
  .ev.frame { border-left-color: var(--frame); }
  .ev.muted { border-left-color: var(--muted); }
  .ev-kind { font-weight: 700; color: var(--muted); font-family: var(--font-mono); font-size: .68rem; }
  .ev-msg { color: var(--text); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .replica-count { font: 700 1rem var(--font-mono); min-width: 2rem; text-align: center; }
  .dep-status { font-size: .75rem; color: var(--muted); margin: 0; }
</style>
