set dotenv-load

# Lists all available backend commands.
_default:
  @just --list

# Runs the gastrolog backend with the specified configuration and sources.
run:
  cd backend
  go run cmd/gastrolog -config config.json -sources sources.json
