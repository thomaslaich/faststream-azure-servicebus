# type check the python codebase (ty)
[group('check')]
@typecheck:
    ty check

# format the entire repository
[group('check')]
@fmt:
    treefmt

# lint the python code (ruff)
[group('check')]
@lint:
    ruff check

# lint and fix the python code
[group('check')]
@lint-fix:
    ruff check --fix

# perform static code analysis (format, lint, typecheck)
[group('check')]
@check: fmt lint typecheck
    @echo "Code analysis complete"

[group('ci')]
@typecheck-ci:
    ty check

[group('ci')]
@fmt-ci:
    treefmt --ci

[group('ci')]
@lint-ci:
    ruff check --output-format=github

[group('ci')]
@ci: fmt-ci lint-ci typecheck-ci
    @echo "Code analysis complete"

@test-ci *args:
    pytest -n 2 --cov --cov-report= {{ args }}

@coverage-ci:
    coverage combine coverage
    coverage report

# start the Service Bus emulator and its SQL Edge backing store
[group('infra')]
@up:
    docker compose up -d --wait

# stop the emulator
[group('infra')]
@down:
    docker compose down -v

# tail the emulator logs
[group('infra')]
@logs:
    docker compose logs -f servicebus

# regenerate the emulator's entity pool (tests/infra/servicebus-config.json)
[group('infra')]
@gen-config:
    python tests/infra/generate_config.py

# run the test suite
# `-n 2`, not `-n auto`: the emulator caps a namespace at 10 concurrent
# connections, and each worker holds several.
[group('test')]
@test *args:
    pytest -n 2 {{ args }}

# run the tests with a coverage report
[group('test')]
@test-cov:
    pytest -n 2 --cov --cov-report=term --cov-report=html

# Smoke test the built wheel in an isolated environment.
[group('build & publish')]
@smoke-wheel:
    uv run --isolated --no-project --with dist/*.whl python -c "from faststream_azure_servicebus import ServiceBusBroker; assert ServiceBusBroker"

# Smoke test the built source distribution in an isolated environment.
[group('build & publish')]
@smoke-sdist:
    uv run --isolated --no-project --with dist/*.tar.gz python -c "from faststream_azure_servicebus import ServiceBusBroker; assert ServiceBusBroker"

[group('build & publish')]
@smoke-dist: smoke-wheel smoke-sdist

# Remove built distributions so smoke tests only see fresh artifacts.
[group('build & publish')]
@clean-dist:
    mkdir -p dist
    find dist -type f \( -name "*.whl" -o -name "*.tar.gz" \) -delete

# build the distribution
[group('build & publish')]
@build: clean-dist
    uv build

# What the release workflow runs before publishing.
[group('build & publish')]
@release-ci: ci build smoke-dist
