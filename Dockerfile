FROM python:3.11-slim
WORKDIR /app
RUN pip install uv
COPY pyproject.toml .python-version* ./
RUN uv sync --no-dev
COPY . .
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"
CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
