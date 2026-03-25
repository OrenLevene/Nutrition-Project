---
description: Start the NutriShop web application development server
---

# Start NutriShop Server

## Prerequisites
- Python environment with dependencies installed
- Working directory: `c:\Users\Oren Arie Levene\Nutrition Project`

## Start Command

// turbo
1. Start the FastAPI development server:
```bash
py -m uvicorn src.web_app.main:app --host 0.0.0.0 --port 8000 --reload
```

> **Note**: On Windows, use `py` instead of `python` (the Python launcher).

## Access
- Local URL: http://localhost:8000
- The `--reload` flag enables hot-reloading for development.

## Expected Startup Output
```
INFO:     Uvicorn running on http://0.0.0.0:8000
INFO:     Started server process
INFO:     Application startup complete.
```

## Stop Server
Press `Ctrl+C` in the terminal running the server.
