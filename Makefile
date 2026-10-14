.PHONY: demo demo-quick up down test baseline

DURATION ?= 10m

# Cluster up, 10 minutes of transfers under faults, report in results/latest.html.
demo: up
	go run ./cmd/ybcheck -duration $(DURATION)

demo-quick:
	$(MAKE) demo DURATION=3m

# Same workload with no faults, for comparison.
baseline: up
	go run ./cmd/ybcheck -duration 2m -no-faults

up:
	docker build -q -t faultbox:local faultbox
	docker compose up -d --wait

down:
	docker compose down -v

test:
	go vet ./... && go test ./...
