package main

// capPublisher captures published events for test assertions.
type capPublisher struct {
	events []any
}

func (c *capPublisher) Publish(v any) { c.events = append(c.events, v) }
