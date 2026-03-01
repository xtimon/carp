# Contributing to CARP

Thank you for your interest in contributing to CARP. This document provides guidelines and instructions.

## Getting Started

1. **Fork and clone** the repository
2. **Build and test**:
   ```bash
   make build
   make test
   ```

3. Read the [Development Guide](docs/DEVELOPMENT.md) for project structure and testing details.

## Development Workflow

1. Create a branch for your change
2. Make your changes; run `make test` and `make test-cover`
3. For significant changes, run `make test-integration`
4. Ensure code is formatted: `go fmt ./...`
5. Submit a pull request with a clear description

## Areas for Contribution

- **New Redis commands**: See [DEVELOPMENT.md](docs/DEVELOPMENT.md#adding-a-new-command)
- **Bug fixes**: Include a test that reproduces the issue
- **Documentation**: Improve clarity in `docs/` or README
- **Performance**: Add benchmarks and measure impact

## Code Style

- Follow standard Go conventions
- Use `gofmt` for formatting
- Keep packages focused; avoid circular imports
- Add tests for new functionality

## Reporting Issues

When reporting bugs, please include:

- CARP version (or commit)
- Go version
- Configuration (redact secrets)
- Steps to reproduce
- Expected vs actual behavior

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
