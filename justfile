set windows-shell := ["powershell.exe", "-NoLogo", "-Command"]
set working-directory := "raspberry"

# Point uv to the .venv at the project root (works on Windows & Linux)
export UV_PROJECT_ENVIRONMENT := join(justfile_directory(), ".venv")

# Run all tests in raspberry/tests
rp-tests:
    uv run pytest tests -v