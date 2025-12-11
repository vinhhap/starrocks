# Build StarRocks

## Build artifact image
```bash
cd /workspaces/codespaces-blank/starrocks
DOCKER_BUILDKIT=1 docker build \
  --build-arg builder=starrocks/dev-env-ubuntu:3.5-latest \
  -f docker/dockerfiles/artifacts/artifact.Dockerfile \
  -t artifact-ubuntu:local \
  .
```

## Build FE image
```bash
cd /workspaces/codespaces-blank/starrocks
DOCKER_BUILDKIT=1 docker build \
  --build-arg ARTIFACT_SOURCE=image \
  --build-arg ARTIFACTIMAGE=artifact-ubuntu:local \
  -f docker/dockerfiles/fe/fe-ubuntu.Dockerfile \
  -t fe-ubuntu:local \
  .
```