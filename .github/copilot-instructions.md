# GitHub Copilot Instructions

## General Guidelines

- Prefer to return early from functions to reduce nesting and improve code readability.
- Always follow good security practices
- Use f-strings for string formatting
- Use logging instead of print statements
- Follow the DRY principle (Don't Repeat Yourself)
- Use pathlib for all file operations
- Follow defensive programming practices
  - Validate all function inputs
  - Handle exceptions gracefully
- Use retry logic for any network calls
- Add a new line before return statements in functions
- Use list/dict/set comprehensions where appropriate
- Avoid using wildcard imports (e.g., from module import *)
- Use context managers for file operations (e.g., with open(...) as f:)
- Prefer using built-in functions and libraries over custom implementations when possible
- Prefer smaller, focused functions that do one thing well rather than large, monolithic functions

## Commit Message Format

All commits must follow the <a href="https://www.conventionalcommits.org/">Conventional Commits</a> format using the Angular preset.

For detailed guidelines on commit types, scopes, and formatting rules, see the <a href="https://github.com/agrc/release-composite-action/blob/main/README.md#commits">release-composite-action README</a>.

## Code Style and Conventions

### Python Style
- Line length: 120 characters (configured in ruff)
- Indentation: 4 spaces for Python files
- Use type hints for all new work
- Follow PEP 8 conventions
- Follow ruff style guide and linting rules
- Use pylint disable comments sparingly and only when necessary (e.g., `# pylint: disable=invalid-name`)

### Documentation
- Use docstrings for all classes and public methods
- Follow NumPy/SciPy docstring format with sections:
  - Brief description
  - `Attributes` for class attributes
  - `Parameters` for method parameters
  - `Returns` for return values
  - `Methods` for public methods in class docstrings

## Testing Guidelines

- Unit tests are required, and are required to pass before PR
- Mock external services
- Test both success and failure paths
- Verify warning messages for invalid configurations
- Code coverage should be maintained at a high level (tracked via codecov)
- Test names should be descriptive and follow the pattern `test_<method_name>_<expected_behavior>`

## Code Quality

- Run `ruff` for linting before committing
- Maintain test coverage (tracked via codecov)
- Follow existing patterns in the codebase
- Keep methods focused and single-purpose
- Use static methods when methods don't need instance state

## Program Structure

This is a skid, which means that it uses ugrc-palletjack to ETL data into ArcGIS Online.

This skid loads project data from the Utah Project Portal API (https://api.utahprojects.org/) into a geopandas geodataframe. It will then transform the data into the proper schema and format for an ArcGIS Online hosted feature service. Once the geodataframe is ready to be uploaded, the skid will use the ugrc-palletjack library to truncate and load the desired hosted feature service with the new data.

ugrc-supervisor is used for error handling and to create an email notification after each skid run. This email should include summary lines describing the results of the run (number of records updated, etc) and should include the log file as an attachment.

Logging is handled using the built-in logging library using the structure created in the _initialize() method of main.py.
