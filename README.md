# MODERATE Data Integrity and Validation Architecture Deployment

This repository contains deployment configurations and automation scripts for the MODERATE Data Integrity and Validation Architecture. The deployment includes Apache Kafka for data streaming, Keycloak for identity and access management, Apache NiFi for data processing workflows, and supporting infrastructure components. The deployment is orchestrated using Docker Compose and Ansible, with configuration templates for environment-specific parameters.

> [!NOTE]
> Upstream Git repositories that were previously cloned during deployment are now vendored into this repository (e.g., `ansible-configurator` and NiFi processors) to ensure reproducible deployments. No cloning occurs at deploy time; see "Updating Vendored Repositories" below for how to refresh them.

## Prerequisites

- Docker
- Docker Compose
- Ansible
- Python 3
- OpenSSL
- `keytool`
- `envsubst`
- [Taskfile](https://taskfile.dev)

## Deployment Guide

### Environment Configuration

The `validate-config` task performs essential configuration validation to ensure all required environment variables are properly set before proceeding with deployment. It checks for the presence of the `.env` file, validates SSH key paths, and verifies that mandatory variables like `MACHINE_URL` are configured correctly.

```bash
cp .env.default .env
# Edit .env - update MACHINE_URL, passwords, and environment values
task validate-config
```

### Initial Setup

The `setup` task prepares the deployment environment by verifying required tools (Docker, Ansible, Python, `keytool`, etc.) and logging into the Docker registry to access private images. All code required for deployment is already vendored in this repository; no repositories are cloned during setup.

```bash
task setup
```

### SSL Certificates

This section handles SSL certificate management for secure communication across services. Caddy automatically obtains and renews Let's Encrypt certificates for the root domain and subdomains defined in `caddy/Caddyfile` (e.g., `keycloak.$MACHINE_URL`, `grafana.$MACHINE_URL`, `reporter.$MACHINE_URL`). The certificates must then be copied and converted to Java keystore formats for Kafka, NiFi, and other Java-based services.

#### Initial Certificate Setup

For the initial deployment, run these tasks to set up certificates:

```bash
task copy-caddy-certificates
task setup-letsencrypt-truststore
task convert-letsencrypt-to-java-stores
```

This will:

1. Start Caddy server and obtain Let's Encrypt certificates
2. Copy certificates from Caddy's data directory to expected locations
3. Download Let's Encrypt root certificates and create a Java truststore
4. Convert PEM certificates to Java keystore format (JKS and PKCS12)

#### Certificate Renewal

While Caddy automatically renews Let's Encrypt certificates (typically every 60-90 days), the copied certificates and Java keystores must be updated when renewals occur. The `update-certificates` task runs both `copy-caddy-certificates` and `convert-letsencrypt-to-java-stores`, which are idempotent and will only update if Caddy's certificates are newer:

```bash
task update-certificates
```

### Keycloak Deployment (Run First)

Deploy and configure Keycloak before the rest of the stack so OAuth client credentials are available to other services.

```bash
task start-keycloak
```

Access Keycloak at `https://keycloak.<MACHINE_URL>` (Caddy terminates TLS and reverse-proxies to Keycloak). Log in with `KEYCLOAK_USER` / `KEYCLOAK_PASSWORD` from `.env`, create your realm, and configure OAuth clients for Kafka, NiFi, and Grafana.

Recommended settings for the NiFi client (Access settings):

- NiFi UI URL: `https://<MACHINE_URL>:8443/nifi`
- Home URL: `https://<MACHINE_URL>:8443/nifi`
- Valid Redirect URIs: `https://<MACHINE_URL>:8443/*`
- Valid post logout URIs: `+`
- Web Origins: `+`

Copy the client secrets to `.env`:

- `KAFKA_KEYCLOAK_SECRET`
- `NIFI_KEYCLOAK_SECRET`
- `GRAFANA_KEYCLOAK_SECRET`

You can stop Keycloak later with:

```bash
task stop-keycloak
```

### Deploy Infrastructure

The `diva` task orchestrates deployment of Kafka, NiFi, Quality Reporter, and supporting components using Ansible playbooks. It verifies SSL certificates, processes configuration templates, and runs the playbooks. Keycloak is deployed separately and should be configured beforehand (see above).

```bash
task diva
```

## Updating Vendored Repositories

This repository vendors several upstream projects so deployments remain self-contained. Use this workflow whenever you need to pull in upstream changes.

### Prerequisites

- SSH access to `gitlab.linksfoundation.com`
- `git` installed locally
- Write access to this repository

### Repository Layout

```
moderate-diva-deployment/
├── kafka/                                          # Kafka setup
├── nifi/                                           # NiFi setup
├── quality_reporter/                               # Quality Reporter setup
└── ansible-configurator/NiFi_Processors/vendored/  # NiFi processors
    ├── dqa-validator/
    ├── schema-validator/
    └── unified-data-model-encapsulator/
```

### Update Workflow

1. From the repository root (`/home/agmangas/moderate-diva-deployment`), create a temporary workspace: `mkdir -p .tmp-updates && cd .tmp-updates`.
2. Clone the desired upstream repository (Kafka, NiFi, Quality Reporter, or a NiFi processor) from GitLab.
3. Remove the upstream `.git` directory so the code becomes a vendored copy: `rm -rf <repo>/.git`.
4. Replace the existing vendored directory:
   - Kafka / NiFi / Quality Reporter: `rm -rf ../<repo>` then `mv <repo> ..`.
   - NiFi processors: `rm -rf ../ansible-configurator/NiFi_Processors/vendored/<processor>` then `mv <processor> ../ansible-configurator/NiFi_Processors/vendored/`.
5. Return to the repository root and remove the temporary workspace: `cd .. && rm -rf .tmp-updates`.

Tip: back up the previous vendored directory before replacing it if you expect to compare or restore changes.
