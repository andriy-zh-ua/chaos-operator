# Chaos Operator

A Kubernetes operator for chaos engineering that intentionally breaks things to test system resilience.

## Overview

The Chaos Operator provides controlled chaos experiments on Kubernetes clusters, allowing you to test system resilience and failure scenarios in a safe, configurable manner.

## Features

- **Pod Kill Chaos**: Terminate pods to test application resilience
- *To be continued...*

## API Types

### Disruption

The main CRD for defining chaos experiments:

```yaml
apiVersion: chaos.a2solutions.ca/v1
kind: Disruption
metadata:
  name: example-disruption
  namespace: default
spec:
  podKill:
    selector:
      matchLabels:
        app: my-app
    duration: 30s
    killMode: "fixed-count"
    count: 2
    gracePeriodSeconds: 30
  safety:
    maxDurationSeconds: 300
    maxPodsAffected: 5
    maxPercentageAffected: 20
```

### Safety Configuration

Safety limits prevent excessive chaos:

- **MaxDurationSeconds**: Maximum experiment duration per disruption cycle (default: 300s)
- **MaxPodsAffected**: Maximum number of pods affected per reconciliation cycle (default: 1)
- **MaxPercentageAffected**: Maximum percentage of pods affected per reconciliation cycle (default: 10%)

## Environment Variables

Configure the operator behavior:

```bash
# Safety Configuration
CHAOS_DEFAULT_DURATION_SECONDS=300
CHAOS_DEFAULT_MAX_PODS=1
CHAOS_DEFAULT_MAX_PERCENTAGE=10

# Limits
CHAOS_MAX_COUNT_LIMIT=100
CHAOS_MAX_GRACE_PERIOD_SECONDS=300

# Monitoring
CHAOS_MONITORING_REQUEUE_INTERVAL=30
```

## Development

### Prerequisites

- **Go** - For building and running the operator
- **Docker** - For building container images and running integration tests
- **Kubernetes cluster** - For deploying and testing the operator (choose one):
  - **minikube** - Local single-node cluster
  - **kind** - Local multi-node cluster
  - **EKS/GKE/AKS** - Production cloud clusters
- **kubectl** - Kubernetes command-line tool for cluster management
- **controller-runtime CLI tools** - For scaffolding and managing operator projects
  - **kubebuilder** - Main CLI tool for operator development
  - **controller-gen** - Code generation for CRDs and clients
- **Make** - Build automation tool (included with most development environments)
- **Git** - Version control (for cloning and managing the repository)

### Optional Development Tools
- **Docker Desktop** - GUI for Docker management (macOS/Windows)
- **Lens** - Kubernetes IDE for cluster visualization
- **k9s** - Terminal-based Kubernetes UI
- **helm** - Package manager for Kubernetes (optional deployment method)

### Setting Up Multi-Node Kind Cluster

For more realistic testing, you can create a multi-node kind cluster:

```bash
# Create multi-node cluster
kind create cluster --name kind --config - <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
- role: worker
EOF
```

### Verify Cluster
```bash
# List clusters (shows kind cluster names)
kind get clusters

# Check current context (shows kubeconfig context name)
kubectl config current-context

# Shows full kubeconfig
# Complete kubeconfig Contents:
#   Cluster info - API server endpoint (https://127.0.0.1:56065)
#   Certificates - CA cert, client cert, client key (base64 encoded)
#   Context - Current context (kind-kind)
#   User - Authentication credentials
kind get kubeconfig

# Check nodes
kubectl get nodes
# Expected Output:
# NAME                 STATUS   ROLES           AGE     VERSION
# kind-control-plane   Ready    control-plane   6m3s    v1.35.0
# kind-worker          Ready    <none>          5m49s    v1.35.0 
```

### Building

```bash
# Build the operator
make build

# Build Docker image with registry
make docker-build IMG=<registry>/chaos-operator:tag

# Build Docker image for local testing
make docker-build IMG=chaos-operator:latest

# Push Docker image to registry
make docker-push IMG=<registry>/chaos-operator:tag
```

### Testing

The project includes comprehensive tests with **75.5% code coverage** for the controller package:

```bash
# Run all tests (unit + integration) with coverage
make test
# Note: 
# - Generates coverage profile
# - Runs all tests (excluding e2e)  
# - Sets up test environment
```

#### Unit Test Coverage

The unit test suite covers:

- ✅ **Validation Logic** (`TestValidatePodKill`) - PodKill specification validation
- ✅ **Safety Configuration** (`TestGetSafetyConfig`) - Safety config defaults and overrides
- ✅ **Status Management** (`TestUpdateDisruptionStatus`) - Status updates and error handling
- ✅ **Status Marking** (`TestMarkDisruptionRunning`) - Mark disruption as running
- ✅ **Status Marking** (`TestMarkDisruptionFailed`) - Mark disruption as failed
- ✅ **Status Marking** (`TestMarkDisruptionCompleted`) - Mark disruption as completed
- ✅ **Environment Variables** (`TestGetInt32Env`) - Environment variable parsing (int32)
- ✅ **Environment Variables** (`TestGetInt64Env`) - Environment variable parsing (int64)
- ✅ **Controller Setup** (`TestSetupWithManager`) - Controller manager integration
- ✅ **Constructor Function** (`TestNewDisruptionReconciler`) - Basic initialization verification

#### Unit Test Examples

```bash
# Run all unit tests with verbose output
go test -v ./internal/controller

# Run unit tests with coverage profile
go test -coverprofile=coverage.out ./internal/controller

# Generate HTML coverage report
go tool cover -html=coverage.out

# Run unit tests with coverage summary
go test -cover ./internal/controller

# Run specific unit test only (replace TestValidatePodKill with desired test)
go test -v ./internal/controller -run TestValidatePodKill
```

#### Integration Test Coverage

*To be continued...*

#### Integration Test Examples

*To be continued...*

### Deployment

#### Local Deployment

```bash
# Build Docker image for local testing
make docker-build IMG=chaos-operator:latest

# Load image into kind cluster
kind load docker-image chaos-operator:latest --name kind

# Install the Custom Resource Definitions (CRDs) into the cluster. 
# This defines the schema for custom resources (e.g., Disruption). 
# Without this, the Kubernetes API server doesn't know about resource types.
make install

# Deploy controller (operator) with local image
make deploy IMG=chaos-operator:latest
```

#### Using YAML (Production)

*To be continued...*

#### Using Helm

*To be continued...*

## Usage

*To be continued...*

### Monitor Status

*To be continued...*

## Safety Considerations

- *To be continued...*

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass (`make test`)
5. Maintain code coverage
6. Submit a pull request

### Adding New Tests

When adding new functionality, ensure comprehensive test coverage:

```go
func TestNewFeature(t *testing.T) {
    tests := []struct {
        name        string
        input       InputType
        expected    ExpectedType
        expectError bool
    }{
        // Test cases here
    }
    
    for _, test := range tests {
        t.Run(test.name, func(t *testing.T) {
            // Test implementation
        })
    }
}
```

## License

Copyright 2026 A2Solutions.ca.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Support

For support, please open an issue on the [GitHub repository](https://github.com/andriy-zh-ua/chaos-operator).
