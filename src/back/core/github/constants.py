"""GitHub REST API constants."""

GITHUB_API_URL = "https://api.github.com"
GITHUB_API_VERSION = "2022-11-28"
GITHUB_TIMEOUT_S = 20.0

# Attempts for the ref fast-forward when a concurrent push moved the branch.
GITHUB_COMMIT_RETRIES = 3

GITHUB_TOKEN_ENV = "ONTOBRICKS_GITHUB_TOKEN"
