# IndexTables4Spark Makefile
# Convenience wrapper around Maven and scripts/run-tests.sh

.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
export JAVA_HOME ?= /opt/homebrew/opt/openjdk@11

JOBS           ?= 4
SUITE          ?=
MVN            := mvn

# ---------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------
.PHONY: help setup compile build test-compile test test-dry-run test-single clean package verify

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

setup: ## Install/verify development dependencies (Java 11, Maven)
	./scripts/setup.sh

compile: ## Compile sources (mvn clean compile)
	$(MVN) clean compile

build: compile ## Alias for compile

test-compile: ## Compile sources and tests (mvn test-compile)
	$(MVN) test-compile

test: ## Run all tests via scripts/run-tests.sh (JOBS=N to set parallelism)
	./scripts/run-tests.sh -j $(JOBS)

test-dry-run: ## List test classes without running them
	./scripts/run-tests.sh --dry-run

test-single: ## Run a single test class (SUITE=fully.qualified.ClassName)
	@if [ -z "$(SUITE)" ]; then \
		echo "Usage: make test-single SUITE=io.indextables.spark.core.SomeTest"; \
		exit 1; \
	fi
	$(MVN) test-compile scalatest:test -DwildcardSuites='$(SUITE)'

clean: ## Remove build artifacts (mvn clean)
	$(MVN) clean

package: ## Build JAR, skip tests (mvn clean package -DskipTests)
	$(MVN) clean package -DskipTests

verify: ## Run full verification (mvn clean verify)
	$(MVN) clean verify
