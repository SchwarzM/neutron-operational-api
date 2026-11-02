package main

// EventType identifies the SSE event shape.
type EventType string

const (
	EventCluster    EventType = "cluster"
	EventPod        EventType = "pod"
	EventEndpoints  EventType = "endpoints"
	EventRequest    EventType = "request"
	EventSignal     EventType = "signal"
	EventStats      EventType = "stats"
	EventDeployment EventType = "deployment"
)

// ClusterEvent reports cluster bootstrap state.
type ClusterEvent struct {
	Type  EventType `json:"type"`
	State string    `json:"state"` // "creating" | "ready" | "destroying"
}

// PodEvent is emitted whenever a pod's observed state changes.
type PodEvent struct {
	Type           EventType `json:"type"`
	Name           string    `json:"name"`
	Phase          string    `json:"phase"`
	Ready          bool      `json:"ready"`
	Terminating    bool      `json:"terminating"`
	GraceRemaining int       `json:"graceRemaining"`    // seconds remaining in grace period
	GracePeriod    int32     `json:"gracePeriod"`       // total terminationGracePeriodSeconds
	PreStopSleep   int32     `json:"preStopSleep"`      // configured preStop sleep (0 = none)
	InFlight       int       `json:"inFlight"`          // requests currently in-flight to this pod
	Deleted        bool      `json:"deleted,omitempty"` // true when pod has been fully removed
}

// EndpointsEvent lists which pods are currently in the endpoint slice.
type EndpointsEvent struct {
	Type EventType `json:"type"`
	Pods []string  `json:"pods"`
}

// RequestEvent is emitted for every completed HTTP request by the load generator.
type RequestEvent struct {
	Type       EventType `json:"type"`
	ID         string    `json:"id"`
	PodName    string    `json:"podName"`
	Status     int       `json:"status"`
	DurationMs int64     `json:"durationMs"`
	Slow       bool      `json:"slow"`
}

// SignalKind identifies the k8s signal type for SignalEvent.
type SignalKind string

const (
	SignalProbeOK        SignalKind = "PROBE_OK"
	SignalProbeFail      SignalKind = "PROBE_FAIL"
	SignalEndpointAdd    SignalKind = "ENDPOINT_ADD"
	SignalEndpointRemove SignalKind = "ENDPOINT_REMOVE"
	SignalSIGTERM        SignalKind = "SIGTERM"
	SignalDrained        SignalKind = "DRAINED"
)

// SignalEvent reports a notable k8s lifecycle event for a pod.
type SignalEvent struct {
	Type    EventType  `json:"type"`
	Kind    SignalKind `json:"kind"`
	PodName string     `json:"podName"`
	Message string     `json:"message"`
}

// StatsEvent is emitted by the load generator every second.
type StatsEvent struct {
	Type        EventType `json:"type"`
	RPS         float64   `json:"rps"`
	SuccessRate float64   `json:"successRate"`
	P50Ms       int64     `json:"p50Ms"`
	P95Ms       int64     `json:"p95Ms"`
	InFlight    int       `json:"inFlight"`
}

// DeploymentEvent is emitted whenever the demo Deployment's status changes.
type DeploymentEvent struct {
	Type              EventType `json:"type"`
	Replicas          int32     `json:"replicas"`
	ReadyReplicas     int32     `json:"readyReplicas"`
	UpdatedReplicas   int32     `json:"updatedReplicas"`
	AvailableReplicas int32     `json:"availableReplicas"`
	RolloutInProgress bool      `json:"rolloutInProgress"`
	Strategy          string    `json:"strategy"`     // "rolling" | "surge" | "recreate"
	GracePeriod       int32     `json:"gracePeriod"`  // terminationGracePeriodSeconds
	PreStopSleep      int32     `json:"preStopSleep"` // seconds of preStop sleep hook (0 = none)
}

// Publisher is implemented by the SSE Hub. All backend components publish
// events through this interface so they are decoupled from the hub.
type Publisher interface {
	Publish(v any)
}
