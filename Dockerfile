# ==========================================
# STAGE 1: The Builder (Secure & Explicit)
# ==========================================
FROM python:3.12-slim AS builder

# SECURITY: Clean apt cache immediately to reduce attack surface and layer size
RUN apt-get update && apt-get install -y --no-install-recommends gcc build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create a virtual environment to hold dependencies securely
RUN python -m venv /opt/venv
# Ensure all pip commands run inside the virtual environment
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ==========================================
# STAGE 2: The Runtime (Secure & Immutable)
# ==========================================
FROM python:3.12-slim

# Copy the completely built virtual environment from Stage 1 (Standardized on 3.12 ABI)
COPY --from=builder /opt/venv /opt/venv

# Force the container to use the virtual environment's Python and libraries
ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY kalshi_main.py .

# SECURITY: Create non-root user and grant explicit ownership of the working directory
RUN useradd -m botuser && chown -R botuser:botuser /app
USER botuser

# SECURITY: Healthcheck dynamically scans for secure randomized temp files [3]
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD python -c "import os, glob, time; ticks = glob.glob('/tmp/kalshi_heartbeat_*.tick'); exit(0 if ticks and any(time.time() - os.path.getmtime(t) < 120 for t in ticks) else 1)"

CMD ["python", "kalshi_main.py"]