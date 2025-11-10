# MODERATE Data Integrity and Validation Architecture Deployment

This repository contains deployment configurations and automation scripts for the MODERATE Data Integrity and Validation Architecture. The deployment includes Apache Kafka for data streaming, Keycloak for identity and access management, Apache NiFi for data processing workflows, and supporting infrastructure components. The deployment is orchestrated using Docker Compose and Ansible, with configuration templates for environment-specific parameters.

## Prerequisites

* Docker
* Docker Compose
* Ansible
* Python 3
* OpenSSL
* `keytool`
* `envsubst`
* [Taskfile](https://taskfile.dev)

## Deployment Guide

### Environment Configuration

The `validate-config` task performs essential configuration validation to ensure all required environment variables are properly set before proceeding with deployment. It checks for the presence of the `.env` file, validates SSH key paths, and verifies that mandatory variables like `MACHINE_URL` are configured correctly.

```bash
cp .env.default .env
# Edit .env - update MACHINE_URL, passwords, and environment values
task validate-config
```

### Initial Setup

The `setup` task prepares the deployment environment by performing dependency checks and repository management. It verifies that all required tools (Docker, Ansible, Python, keytool, etc.) are installed and accessible, clones the necessary repositories (including the ansible-configurator), and authenticates with the Docker registry to access private images.

```bash
task setup
```

### SSL Certificates

This section handles SSL certificate management for secure communication across all services. Caddy automatically obtains and renews Let's Encrypt certificates, but the certificates must be copied and converted to Java keystore formats for use by Kafka, NiFi, and other Java-based services.

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

### Deploy Infrastructure

The `diva` task orchestrates the complete deployment. It verifies that SSL certificates are valid, processes configuration templates by substituting environment variables, and then deploys all services (Kafka, Keycloak, NiFi, and supporting components) using Ansible playbooks. This task represents the main deployment command that brings up the entire system.

```bash
task diva
```

#### Configuration of OAuth Clients

This section involves manual configuration of OAuth clients in Keycloak for secure authentication across services. After Keycloak is deployed, you must manually create OAuth clients for Kafka, NiFi, and Grafana through the Keycloak web interface. The `process-configuration-templates` task then takes the client secrets you've configured and substitutes them into the service configuration templates, ensuring all components can authenticate properly with Keycloak.

1. Wait for Keycloak prompt: `After Keycloak deployment, you have to manually create a client for NiFi [...]`
2. Access Keycloak at `KEYCLOAK_URL` and create OAuth clients for *Kafka*, *NiFi*, and *Grafana*
3. Ensure that the OAuth client for *NiFi* is properly configured in the *Access settings* (e.g., valid redirect URIs).
4. Copy client secrets to `.env` file (`KAFKA_KEYCLOAK_SECRET`, `NIFI_KEYCLOAK_SECRET`, `GRAFANA_KEYCLOAK_SECRET`)
5. Run `task process-configuration-templates` in separate terminal
6. Confirm Ansible prompt to continue

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

### Test the Update

After refreshing any vendored component, run:

```bash
task process-configuration-templates
task deploy
```

Verify the deployment and service logs before committing.

### Commit the Changes

Stage only the updated vendored paths, then create a concise commit describing which components were refreshed:

```bash
git add kafka/ nifi/ quality_reporter/ ansible-configurator/NiFi_Processors/vendored/
git commit -m "<updated components>"
```

### Track Upstream Versions

Before removing the upstream `.git` directory, capture the source commit hash for reference:

```bash
git log -1 --format="%H %ai %s" > ../../vendored-<component>-version.txt
```

Aggregate these references in `VENDORED_VERSIONS.md` (create the file if it does not exist) so you always know which upstream revisions are deployed.

### Custom Patches

Apply any local modifications directly to the vendored files, describe them in a `PATCHES.md`, and commit the updates together with the refreshed sources.

### Troubleshooting

- `task update-repos` now points to this manual flow; seeing that message is expected.
- If deployment fails, verify that all vendored directories were replaced correctly and that no `.git` folders remain.
- Re-run `task process-configuration-templates` to ensure Ansible receives up-to-date parameters.
- Inspect Ansible logs for detailed failure messages.

### Optional Update Script

Automate the workflow with a helper script such as:

```bash
#!/bin/bash
# update-vendored-repo.sh
REPO_NAME=$1
GITLAB_REPO_PATH=$2
LOCAL_PATH=$3

if [ -z "$REPO_NAME" ] || [ -z "$GITLAB_REPO_PATH" ] || [ -z "$LOCAL_PATH" ]; then
  echo "Usage: $0 <repo-name> <gitlab-path> <local-path>"
  exit 1
fi

mkdir -p .tmp-updates
cd .tmp-updates

git clone "$GITLAB_REPO_PATH" "$REPO_NAME"
rm -rf "$REPO_NAME/.git"

cd ..
[ -d "$LOCAL_PATH" ] && mv "$LOCAL_PATH" "${LOCAL_PATH}.backup"
mv ".tmp-updates/$REPO_NAME" "$LOCAL_PATH"
rm -rf .tmp-updates

echo "$REPO_NAME updated. Previous version backed up to ${LOCAL_PATH}.backup"
```

Make the script executable (`chmod +x update-vendored-repo.sh`) and run it from the repository root when you need a repeatable update process.