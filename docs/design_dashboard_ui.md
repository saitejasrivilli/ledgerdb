# Design: Metrics Dashboard UI (v0.17)

## Scope, honestly bounded
A small, single-page web UI reading the Prometheus metrics v0.11 already
exports and the cluster status the REST API (v0.16) can already answer —
this version adds a `/status` JSON endpoint and a static HTML/JS page
that polls it, rendering leader status per node, write throughput, and
consumer lag as a simple table and a canvas sparkline. This is
explicitly the lowest-value piece of the three requested additions for
interview purposes — it's a demo artifact, not new correctness surface —
and is scoped accordingly: no build tooling, no framework, no design
system, one static HTML file and one small Go handler.

## Why a dedicated `/status` JSON endpoint instead of scraping `/metrics` client-side
Prometheus's text exposition format is meant for Prometheus to scrape,
not for browser JS to parse — parsing it client-side would mean
re-implementing a chunk of the Prometheus text format for no real
benefit. `/status` (added to `api/`, v0.16) returns a small, purpose-built
JSON shape the dashboard actually needs, built from the same underlying
`metrics` package values.

## What proves this correct
`TestDashboard_StatusEndpointServesRealClusterState`: drives the
`/status` endpoint via a real HTTP request against a real 3-node
cluster, confirms the returned JSON's leader flags match `GetState()` on
each node directly — the endpoint reports real state, not placeholder
zeros. The static HTML/JS file itself isn't unit-tested (no headless
browser in this test suite) — stated as a boundary, not hidden.

## Manual verification (the part no automated test covers)
`cmd/dashboard_demo` starts a real 3-node cluster and serves the
dashboard at `:8080`. Run and checked directly: `curl /status` returned
`{"nodes":[{"index":0,...},{"index":1,...},{"index":2,...}]}` with
exactly one `is_leader:true` among the three, and `curl /` returned the
real HTML page — confirming both the JSON endpoint and the static page
serve correctly against a live cluster, not just in the automated test's
narrower JSON-shape check.

## What v0.17 deliberately does NOT do
- No headless-browser/UI test — verified manually by loading the page
  against a running cluster, not by an automated test (see note above)
- No auth, no multi-cluster support, no historical charting (Grafana,
  v0.11, already covers real dashboarding — this is a lightweight status
  view, not a Grafana replacement)
