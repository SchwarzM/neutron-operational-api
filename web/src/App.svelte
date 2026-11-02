<script>
  import { onMount } from 'svelte'
  import { connectSSE, clusterState, stats } from './store.js'
  import SystemMap from './SystemMap.svelte'
  import Sidebar from './Sidebar.svelte'

  let theme = $state(localStorage.getItem('demo-theme') || 'dark')
  let showHelp = $state(false)

  function toggleTheme() {
    theme = theme === 'dark' ? 'light' : 'dark'
    localStorage.setItem('demo-theme', theme)
    document.documentElement.dataset.theme = theme
  }

  onMount(() => {
    document.documentElement.dataset.theme = theme
    connectSSE()
  })

  const stateColor = { creating: 'warn', ready: 'ok', destroying: 'err' }

  function successClass(rate) {
    if (rate == null) return ''
    if (rate >= 0.99) return 'ok'
    if (rate >= 0.95) return 'warn'
    return 'err'
  }
</script>

<svelte:window onkeydown={e => { if (e.key === 'Escape') showHelp = false }} />

<div class="shell">
  <header class="topbar">
    <h1>K8s Readiness <span>Demo</span></h1>
    <span class="pill {stateColor[$clusterState] ?? 'warn'}">{$clusterState}</span>
    <div class="topbar-end">
      <button class="btn help-btn" onclick={() => showHelp = true} aria-label="Help">?</button>
      <button class="btn" onclick={toggleTheme} aria-label="Toggle theme">
        {theme === 'dark' ? '☀' : '☾'}
      </button>
    </div>
  </header>

  <div class="stats-strip">
    <div class="stat-primary {successClass($stats?.successRate)}">
      <span class="stat-value">{$stats?.successRate != null ? ($stats.successRate * 100).toFixed(0) : '–'}%</span>
      <span class="stat-lbl">success rate</span>
    </div>
    <div class="stat-div"></div>
    <div class="stat-item">
      <span class="stat-num">{$stats?.rps?.toFixed(1) ?? '–'}</span>
      <span class="stat-lbl">req/s</span>
    </div>
    <div class="stat-item">
      <span class="stat-num">{$stats?.p50Ms != null ? $stats.p50Ms + 'ms' : '–'}</span>
      <span class="stat-lbl">p50</span>
    </div>
    <div class="stat-item">
      <span class="stat-num">{$stats?.p95Ms != null ? $stats.p95Ms + 'ms' : '–'}</span>
      <span class="stat-lbl">p95</span>
    </div>
    <div class="stat-item">
      <span class="stat-num">{$stats?.inFlight ?? '–'}</span>
      <span class="stat-lbl">in-flight</span>
    </div>
  </div>

  <main class="body">
    <div class="map-area">
      <SystemMap />
    </div>
    <aside class="sidebar-area">
      <Sidebar />
    </aside>
  </main>

  {#if showHelp}
    <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
    <div class="help-backdrop" onclick={() => showHelp = false}>
      <!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
      <div class="help-panel" onclick={e => e.stopPropagation()}>
        <div class="help-header">
          <h2>How this demo works</h2>
          <button class="btn" onclick={() => showHelp = false} aria-label="Close">✕</button>
        </div>
        <div class="help-body">
          <p class="help-intro">
            This demo shows how Kubernetes handles rolling updates under live traffic.
            Requests flow from the load generator → nginx-ingress → Service → Pods.
            Trigger a rollout while traffic is running and watch whether any requests fail.
            The goal is to understand which settings produce zero failures — and why.
          </p>
          <div class="help-grid">
            <section>
              <h3>preStop hook</h3>
              <p>A command that runs <em>before</em> SIGTERM is sent to your container.
              During this window the pod is still alive and can serve in-flight requests,
              but nginx-ingress is removing it from its upstream list. Without a preStop
              delay, nginx keeps routing to the pod for several seconds after termination
              starts — any new request that lands there gets a 503.</p>
              <p class="rec">Real world: set to the time your ingress/proxy needs to
              propagate endpoint removal. 5–15 s is typical for nginx-ingress.
              Always less than the grace period.</p>
            </section>
            <section>
              <h3>Grace period</h3>
              <p>Total time Kubernetes waits for a pod to finish shutting down before
              sending SIGKILL. This window covers both the preStop hook <em>and</em>
              the time for the process to drain in-flight requests. If SIGKILL arrives
              while requests are still in progress, those requests are dropped hard.</p>
              <p class="rec">Real world: preStop + max request duration + a safety buffer.
              Example — preStop=10 s, max request=5 s → set grace to at least 20 s.</p>
            </section>
            <section>
              <h3>Rollout strategy</h3>
              <p><strong>Surge</strong> — new pods start before old ones stop. Briefly
              uses extra capacity but causes zero downtime. Use this in production.</p>
              <p><strong>Rolling</strong> — old pod terminates before new one starts.
              Avoids extra resources but is slower and reduces capacity during rollout.</p>
              <p><strong>Recreate</strong> — all pods killed at once, then new ones start.
              Causes a hard downtime gap. Use only when two versions cannot run side-by-side.</p>
            </section>
            <section>
              <h3>Replicas</h3>
              <p>Number of pod instances. More replicas mean a smaller fraction of
              capacity is affected per rollout step. With 2 replicas and maxSurge=1,
              50% of capacity is cycling at once; with 4 replicas only 25%.
              More replicas also spread traffic more evenly, visible in the req/s
              labels next to each pod.</p>
              <p class="rec">Real world: 3+ for high availability. Spread across
              availability zones for resilience against node failures.</p>
            </section>
            <section>
              <h3>Readiness probe</h3>
              <p>Kubernetes calls <code>/readyz</code> every 2 s. A pod only enters the
              Service endpoint slice — and starts receiving traffic — once the probe
              passes. During a rollout the new pod won't get traffic until it's ready,
              and the old pod stays in the slice until the new one is confirmed ready.</p>
              <p class="rec">Use <em>set not-ready</em> on a pod in the sidebar to see
              it removed from the endpoint slice in real time.</p>
            </section>
            <section>
              <h3>Request duration</h3>
              <p>The demo server waits this long before responding. Fast (0 ms) requests
              complete before a pod can be terminated so no overlap is possible — success
              rate stays 100% regardless of settings. Longer durations (500 ms, 2 s) are
              in-flight when SIGTERM arrives, making graceful drain observable and failures
              reproducible.</p>
              <p class="rec">Use 2 s duration at 10 req/s to reliably reproduce failures
              and verify the fix with preStop + grace settings.</p>
            </section>
          </div>
        </div>
      </div>
    </div>
  {/if}
</div>

<style>
  :global(*) { box-sizing: border-box; margin: 0; padding: 0; }
  :global(:root) {
    --font-head: "Arial Black", system-ui, sans-serif;
    --font-body: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
    --font-mono: ui-monospace, "SF Mono", Menlo, Consolas, monospace;
    --s1:4px; --s2:8px; --s3:12px; --s4:16px; --s5:24px;
    --radius:6px; --dur:180ms;
    --bg:#eef1f5; --surface:#fff; --surface-2:#f4f6f9; --surface-3:#e7ebf0;
    --border:#d3d9e1; --border-strong:#aab4c0;
    --text:#17202b; --muted:#4f5b69; --faint:#6a7583;
    --accent:#0a5fc2; --accent-ink:#fff;
    --add:#116329; --add-bg:#dcf3e3;
    --del:#b4141f; --del-bg:#fde0e2;
    --warn:#8a5300; --warn-bg:#fbeccb;
    --frame:#0f7a6c;
    --shadow:0 8px 28px rgba(18,30,45,.18);
  }
  :global([data-theme="dark"]) {
    --bg:#0a0e13; --surface:#111821; --surface-2:#161f2a; --surface-3:#1d2834;
    --border:#253242; --border-strong:#3a4a5d;
    --text:#e6edf4; --muted:#a3b0be; --faint:#8592a1;
    --accent:#5aa9ff; --accent-ink:#06121f;
    --add:#5fd47f; --add-bg:#12301d;
    --del:#ff7b80; --del-bg:#3a1519;
    --warn:#f0b54a; --warn-bg:#33270f;
    --frame:#3fd6c1;
    --shadow:0 10px 34px rgba(0,0,0,.55);
  }
  :global(body) {
    background: var(--bg); color: var(--text);
    font: 400 1rem/1.4 var(--font-body);
    font-variant-numeric: tabular-nums;
    height: 100vh; overflow: hidden; display: flex; flex-direction: column;
  }
  :global(button) { font: inherit; color: inherit; cursor: pointer; }
  .shell { display: flex; flex-direction: column; height: 100vh; }

  /* topbar */
  .topbar {
    display: flex; align-items: center; gap: var(--s3);
    padding: var(--s2) var(--s4); border-bottom: 1px solid var(--border);
    background: var(--surface); flex-shrink: 0;
  }
  h1 { font: 400 1.4rem var(--font-head); white-space: nowrap; }
  h1 span { color: var(--accent); }
  .pill { font: 700 .7rem var(--font-body); letter-spacing:.06em; text-transform:uppercase;
    padding: 3px 8px; border-radius: 999px; border: 1px solid currentColor; white-space: nowrap; }
  .pill.ok { color: var(--add); }
  .pill.warn { color: var(--warn); }
  .pill.err { color: var(--del); }
  .topbar-end { margin-left: auto; display: flex; gap: var(--s2); align-items: center; }
  .btn { background: var(--surface-2); border: 1px solid var(--border-strong);
    border-radius: var(--radius); padding: 4px 10px; font-size: .9rem; }
  .help-btn { font-weight: 700; width: 30px; height: 30px; padding: 0;
    border-radius: 50%; display: flex; align-items: center; justify-content: center; }

  /* stats strip */
  .stats-strip {
    display: flex; align-items: center; justify-content: center; gap: var(--s5);
    padding: var(--s2) var(--s5); background: var(--surface);
    border-bottom: 2px solid var(--border); flex-shrink: 0;
  }
  .stat-primary { display: flex; flex-direction: column; align-items: flex-start; min-width: 5.5rem; }
  .stat-value { font: 700 2.4rem/1 var(--font-mono); transition: color var(--dur); color: var(--muted); }
  .stat-primary.ok .stat-value  { color: var(--add); }
  .stat-primary.warn .stat-value { color: var(--warn); }
  .stat-primary.err .stat-value  { color: var(--del); }
  .stat-div { width: 1px; height: 2.2rem; background: var(--border); flex-shrink: 0; }
  .stat-item { display: flex; flex-direction: column; align-items: flex-start; gap: 1px; }
  .stat-num { font: 700 1.25rem/1 var(--font-mono); }
  .stat-lbl { font-size: .62rem; text-transform: uppercase; letter-spacing: .08em; color: var(--muted); margin-top: 2px; }

  /* main layout */
  .body { display: grid; grid-template-columns: 1fr 300px; flex: 1; min-height: 0; }
  .map-area { min-height: 0; overflow: hidden; }
  .sidebar-area { border-left: 1px solid var(--border); overflow-y: auto; }

  /* help modal */
  .help-backdrop {
    position: fixed; inset: 0; background: rgba(0,0,0,.55);
    display: flex; align-items: center; justify-content: center; z-index: 100;
  }
  .help-panel {
    background: var(--surface); border: 1px solid var(--border-strong);
    border-radius: var(--radius); box-shadow: var(--shadow);
    width: min(880px, 92vw); max-height: 85vh;
    display: flex; flex-direction: column;
  }
  .help-header {
    display: flex; align-items: center; justify-content: space-between;
    padding: var(--s3) var(--s4); border-bottom: 1px solid var(--border); flex-shrink: 0;
  }
  .help-header h2 { font: 600 1.05rem var(--font-body); }
  .help-body { overflow-y: auto; padding: var(--s4); display: flex; flex-direction: column; gap: var(--s4); }
  .help-intro { font-size: .88rem; color: var(--muted); line-height: 1.6; }
  .help-grid { display: grid; grid-template-columns: 1fr 1fr; gap: var(--s4); }
  .help-grid section { display: flex; flex-direction: column; gap: var(--s2); }
  .help-grid h3 {
    font: 700 .72rem var(--font-head); text-transform: uppercase; letter-spacing: .08em;
    color: var(--accent);
  }
  .help-grid p { font-size: .82rem; line-height: 1.6; }
  .help-grid .rec {
    font-size: .78rem; color: var(--muted);
    border-left: 2px solid var(--border-strong); padding-left: var(--s2);
  }
  :global(.help-grid code) {
    font: 500 .82em var(--font-mono); background: var(--surface-2);
    padding: 1px 4px; border-radius: 3px;
  }
</style>
