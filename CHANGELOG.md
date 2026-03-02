# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [v0.0.3] - 2025-03-02

### Fixed

- **Mapper**: Resolve concurrent map panics via eager caching and proper synchronization when processing DCP events (#5)

Upgrade with: `go get github.com/Trendyol/go-dcp-redis@v0.0.3`

## [v0.0.2]

### Added

- **Redis Cluster** support for horizontal scaling and high availability
- Connect to Redis Cluster using a list of node addresses (host:port)
- Optional cluster-specific username and password
- Configuration options: routeByLatency, routeRandomly, readOnly
- See README for Redis Cluster configuration and usage examples

Upgrade with: `go get github.com/Trendyol/go-dcp-redis@v0.0.2`

## [v0.0.1]

### Fixed

- Integration test: remove redundant `.Close` statements

[v0.0.3]: https://github.com/Trendyol/go-dcp-redis/compare/v0.0.2...v0.0.3
[v0.0.2]: https://github.com/Trendyol/go-dcp-redis/compare/v0.0.1...v0.0.2
[v0.0.1]: https://github.com/Trendyol/go-dcp-redis/releases/tag/v0.0.1
