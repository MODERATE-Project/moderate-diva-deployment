# Quality Reporter Variables
---

files_to_fill: 
  - docker-compose.yml

kafka_conf:
  kafka_bootstrap_srv: "${KAFKA_BOOTSTRAP_SRV}"
  kafka_sasl_usr: "${KAFKA_USER}"
  kafka_sasl_psw: "${KAFKA_PASSWORD}"
  kafka_endpnt_id: "${KAFKA_ENDPOINT_ID}"

grafana_conf:
  graf_usr: "${GRAFANA_USER}"
  graf_psw: "${GRAFANA_PASSWORD}"
  graf_kck_id: "${GRAFANA_KEYCLOAK_ID}"
  graf_kck_scr: "${GRAFANA_KEYCLOAK_SECRET}" 