# ==========================================
# STAGE 1: The Builder
# ==========================================
FROM python:3.12-slim AS builder

RUN apt-get update && apt-get install -y gcc build-essential

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

# Copy the completely built virtual environment from Stage 1
COPY --from=builder /opt/venv /opt/venv

# Force the container to use the virtual environment's Python and libraries
ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY kalshi_main.py .

# SECURITY: Create non-root user and grant explicit ownership of the working directory
RUN useradd -m botuser && chown -R botuser:botuser /app
USER botuser

# SECURITY: Healthcheck perfectly synchronized with Python's static heartbeat file
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD python -c "import os, time; f='/tmp/kalshi_heartbeat.tick'; exit(0 if os.path.exists(f) and time.time()-os.path.getmtime(f) < 120 else 1)"

CMD ["python", "kalshi_main.py"]