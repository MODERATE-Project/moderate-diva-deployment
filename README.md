# MODERATE Data Integrity and Validation Architecture Deployment

This repository contains deployment configurations and automation scripts for the MODERATE Data Integrity and Validation Architecture. The deployment includes Apache Kafka for data streaming, Keycloak for identity and access management, Apache NiFi for data processing workflows, and supporting infrastructure components. The deployment is orchestrated using Docker Compose and Ansible, with configuration templates for environment-specific parameters.

## Deployment guide

> [!IMPORTANT]
> There is a **manual step** required while Ansible is executing the playbook.
> 
> When it's time to create the OAuth clients in Keycloak, after you have created them and updated the `.env` file, you must run the task `task process-configuration-templates` in a separate session.
> 
> Do this before confirming the prompt related to the OAuth clients: `After Keycloak deployment, you have to manually create a client for NiFi [...]`.