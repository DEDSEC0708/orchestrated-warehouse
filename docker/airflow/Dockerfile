# =============================================================================
# VoltHive Airflow image
#
# ONE image serves every Airflow role in the stack - init, scheduler and
# webserver - because they need identical code and dependencies and differ only
# in the command they run. One Dockerfile, one build, one dependency set to keep
# in step.
#
# The base tag pins BOTH the Airflow version and the Python version. Airflow's
# official constraints file is published per (airflow, python) pair, so pinning
# only one of them would let the other drift and silently invalidate
# constraints.txt. See docs/adr/012-airflow-version.md.
# =============================================================================
FROM apache/airflow:3.3.1-python3.11

# --- system packages -------------------------------------------------------
# postgresql-client : psql for the healthcheck fallback and manual debugging
# curl              : used by the webserver healthcheck
# Both are small and both have a named consumer; nothing else is installed.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        postgresql-client \
        curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# --- Python dependencies ---------------------------------------------------
# Back to the unprivileged airflow user BEFORE pip install, so packages land in
# the right place and nothing in the image is owned by root unnecessarily.
USER airflow

# Copied separately from the source tree so this layer is cached and only
# rebuilds when dependencies actually change - not on every code edit.
COPY --chown=airflow:root requirements.txt constraints.txt /tmp/

# ALWAYS with the constraints file. Installing Airflow without it is the single
# most common cause of a broken Airflow environment.
RUN pip install --no-cache-dir -r /tmp/requirements.txt -c /tmp/constraints.txt

# --- project code ----------------------------------------------------------
# Baked into the image so the image is self-contained and CI can run it without
# any bind mount. docker-compose.yml ALSO bind-mounts ./src over this path for
# local development, so edits take effect without a rebuild. The image copy is
# the fallback; the mount is the convenience.
COPY --chown=airflow:root src/ /opt/airflow/src/

# The one-shot bootstrap script. Copied (not mounted) and made executable here
# on purpose: a bind-mounted shell script inherits the host's permission bits,
# and files created on Windows arrive without the executable bit. Setting it in
# the image removes that entire class of "works on my machine" failure.
COPY --chown=airflow:root docker/airflow/init.sh /opt/airflow/init.sh
RUN chmod +x /opt/airflow/init.sh

# /opt/airflow/src makes `import volthive` work; /opt/airflow makes
# `import dags.common...` work - the DAGs import their shared helpers as a
# package rooted at the repository, and Airflow only puts the DAGs FOLDER on
# sys.path, not its parent. docker-compose.yml sets the same value; this keeps
# a bare `docker run` of the image behaving identically.
ENV PYTHONPATH=/opt/airflow/src:/opt/airflow \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /opt/airflow
