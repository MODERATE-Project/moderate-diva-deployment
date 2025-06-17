# NiFi Variables
---

files_to_fill: 
  - docker-compose.yml
  - nifi.properties

nifi_cred:
  nifi_user: "${NIFI_USER}"
  # PASSWORD LONGER THAN 12 CHARS
  nifi_psw: "${NIFI_PASSWORD}"
  nifi_kck_id: "${NIFI_KEYCLOAK_ID}" # manually added after keycloak configuration
  nifi_kck_scr: "${NIFI_KEYCLOAK_SECRET}" # manually added after keycloak configuration 