.PHONY: test test-unit test-integration install-dev clean

# Install development dependencies
install-dev:
	pip install -e .
	pip install pytest pytest-cov

# Run all tests
test:
	pytest tests/ -v

# Run unit tests specifically  
test-unit:
	pytest tests/unittests/ -v

# Run integration tests
test-integration:
	pytest tests/test_integration.py -v

# Run with coverage
test-coverage:
	pytest tests/ --cov=tap_youtube_analytics --cov-report=term-missing

# Clean up
clean:
	rm -rf .pytest_cache/
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
