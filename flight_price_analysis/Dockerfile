FROM astrocrpublic.azurecr.io/runtime:3.0-2

# Install additional packages
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-mysql-client \
    postgresql-client && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
USER astro

