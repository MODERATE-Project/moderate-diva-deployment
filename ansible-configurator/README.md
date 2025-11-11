# Ansible Configurator

Collection of Ansible Playbooks to deploy Digital Twin infrastructure.

## Preparation step

- In every file `params.yml` (the general one and those in every subfolder), replace `PLCHLD_*` with the actual value to be used in the play.

### Configuration Templates

This configurator uses Jinja2 templates to generate configuration files for each component. The templates are located in their respective app-specific directories:

- `kafka/templates/` - Kafka configuration templates
- `nifi/templates/` - NiFi configuration templates  
- `quality_reporter/templates/` - Quality Reporter configuration templates

When you run the Ansible playbooks, they will:
1. Read the template files (`.j2` extension)
2. Replace variables with values from `params.yml` files
3. Generate final configuration files in the component directories (kafka/, nifi/, quality_reporter/)

**Important**: The generated configuration files are gitignored. Only the templates are version-controlled. This keeps the Git repository clean and prevents sensitive data from being accidentally committed.

## Deployment

### Complete Infrastructure

Run the ansible playbook with: `ansible-playbook -K ansible-plb.yml`. It will ask for the `sudo` password and it will subsequently import the playbook for every component and deploy it.

### Single Component

First open the general `ansible-plb.yml` and comment all the `import_playbook` command except the one regarding the component to be deployed. Then run `ansible-playbook -K ansible-plb.yml`.

If it is sure that the general script was already executed, enter the folder of the selected component and run: `ansible-playbook ansible.plb.yml`

### Updates or Retries after Errors

Run the ansible playbook with the parameter: `--tags never`. It will `docker compose down` every docker compose launched by the playbooks.

### Maintaining Configuration Templates

If you need to update component configurations:

1. **Edit the template files** in the component's `templates/` directory (NOT the generated files in component directories)
2. The template files use Jinja2 syntax: `{{ variable_name }}`
3. Variables are defined in the respective `params.yml` files
4. After editing templates, re-run the Ansible playbook to regenerate the configuration files

**Example**: To change Kafka configuration, edit `kafka/templates/docker-compose.yml.j2`, then run the Kafka playbook.

### Keycloak

> **⚠️ IMPORTANT**: Keycloak is NOT deployed by this configurator. You must deploy and configure Keycloak separately before running these playbooks.

#### Prerequisites

1. **Deploy Keycloak externally** (this configurator only configures components to connect to an existing Keycloak instance)
2. Set up a PostgreSQL database for Keycloak
3. Ensure Keycloak is accessible via a URL (e.g., `https://keycloak.yourdomain.com`)

#### Configuration Steps

**Step 1: Configure Keycloak URL**

In the main `params.yml`, set the Keycloak URL:

```yaml
general_vars:
  keycloak_url: https://keycloak.yourdomain.com # Replace PLCHLD_KCK
```

**Step 2: Create Realm and OAuth Clients in Keycloak**

Manually configure Keycloak via its admin UI:

1. **Create a Realm** with the same name as your `project_name`
2. **Create 3 OAuth/OIDC Clients**:
   - **Kafka Client** - for Kafka authentication
   - **NiFi Client** - for NiFi authentication
   - **Grafana Client** - for Grafana authentication

For each client, note down:

- Client ID
- Client Secret

**Step 3: Update Component Configuration Files**

Add the client credentials to their respective `params.yml` files:

- **Kafka** (`Kafka/params.yml`):

  ```yaml
  kafka_cred:
    kafka_kck_id: your-kafka-client-id # Replace PLCHLD_KK_ID
    kafka_kck_scr: your-kafka-client-secret # Replace PLCHLD_KK_SCR
  ```

- **NiFi** (`NiFi/params.yml`):

  ```yaml
  nifi_cred:
    nifi_kck_id: your-nifi-client-id # Replace PLCHLD_NK_ID
    nifi_kck_scr: your-nifi-client-secret # Replace PLCHLD_NK_SCR
  ```

- **Grafana** (`Quality_Reporter/params.yml`):
  ```yaml
  grafana_conf:
    graf_kck_id: your-grafana-client-id # Replace PLCHLD_KG_ID
    graf_kck_scr: your-grafana-client-secret # Replace PLCHLD_KG_PSW
  ```

**Step 4: Run Deployment**

After completing the above steps, run the playbooks. The configurator will inject these values into the component configuration files.

## Parameters Summary

### General

- `digital_twin_folder`: the folder on the machine where the digital twin's files will be stored
- `generic_psw`: the password used for keystore, truststore and every other place where it is not specified otherwise
- `machine_url`: the tailscale address of the machine
- `project_name`: the name of the project, used to differentiate container belonging to different projects
- `keycloak_url`: the address of the keycloak's instance used for authentication
- `project_repo`: the git path relative to where all the repositories are hosted (e.g. gitlab.linksfoundation:csc/csc-projects/moderate)

### Kafka

- `files_to_fill`: dotted list of the files where the variables need to be replaced
- `kafka_user`: the user for Kafka
- `kafka_psw`: the password for Kafka user
- `kafka_kck_id`: client ID of the Keycloak client used for Kafka authentication (must be configured in Keycloak first - see [Keycloak Configuration](#keycloak))
- `kafka_kck_scr`: client secret of the Keycloak client for Kafka (must be configured in Keycloak first - see [Keycloak Configuration](#keycloak))

### Keycloak

> **Note**: Keycloak deployment is NOT handled by this configurator. The parameters below are for reference only if you're deploying Keycloak separately.

**For Keycloak deployment (external):**

- `keyck_user`: the admin user for Keycloak
- `keyck_psw`: the password for Keycloak admin user
- `keyck_db_user`: the user of Keycloak's PostgreSQL database
- `keyck_db_psw`: the password for Keycloak's PostgreSQL database

**For component integration (configured by this tool):**

- `keycloak_url`: URL of the Keycloak instance (set in main `params.yml`)
- Component-specific client IDs and secrets (see [Keycloak Configuration](#keycloak) section)

### NiFi

- `files_to_fill`: dotted list of the files where the variables need to be replaced
- `nifi_user`: the user for NiFi
- `nifi_psw`: the password for NiFi user (must be longer than 12 characters)
- `nifi_kck_id`: client ID of the Keycloak client used for NiFi authentication (must be configured in Keycloak first - see [Keycloak Configuration](#keycloak))
- `nifi_kck_scr`: client secret of the Keycloak client for NiFi (must be configured in Keycloak first - see [Keycloak Configuration](#keycloak))

### NiFi Processors

- `files_to_fill`: dotted list of the files where the variables need to be replaced
- `nifi_vers`: the deployed version of NiFi
- `proc_vers_vars`: the version of the AMQP processor

### Quality Reporter

- `files_to_fill`: dotted list of the files where the variables need to be replaced
- `kafka_bootstrap_srv`: machine_url:PORT for Kafka
- `kafka_sasl_usr`: the user for Kafka
- `kafka_sasl_psw`: the password for Kafka user
- `kafka_endpnt_id`: "" (fill with something different only if you know what you are doing)
- `graf_usr`: the user for Grafana
- `graf_psw`: the password for Grafana user
- `graf_kck_id`: client ID of the Keycloak client used for Grafana authentication (must be configured in Keycloak first - see [Keycloak Configuration](#keycloak))
- `graf_kck_scr`: client secret of the Keycloak client for Grafana (must be configured in Keycloak first - see [Keycloak Configuration](#keycloak))
