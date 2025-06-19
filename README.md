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

This section handles SSL certificate management for secure communication across all services. The `copy-caddy-certificates` task starts a Caddy server to automatically obtain Let's Encrypt certificates and copies them to the expected locations. The `setup-letsencrypt-truststore` task downloads Let's Encrypt root certificates and creates a Java truststore for services that require it. Finally, `convert-letsencrypt-to-java-stores` converts the PEM certificates to Java keystore format (JKS and PKCS12) for compatibility with Java-based services like Kafka and NiFi.

```bash
task copy-caddy-certificates
task setup-letsencrypt-truststore
task convert-letsencrypt-to-java-stores
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
3. Copy client secrets to `.env` file (`KAFKA_KEYCLOAK_SECRET`, `NIFI_KEYCLOAK_SECRET`, `GRAFANA_KEYCLOAK_SECRET`)
4. Run `task process-configuration-templates` in separate terminal
5. Confirm Ansible prompt to continue