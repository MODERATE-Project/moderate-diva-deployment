# Kafka Variables
---

files_to_fill: 
  - docker-compose.yml
  - client.config
  - kafka-ui-config.yml

kafka_cred:
  kafka_user: "${KAFKA_USER}"
  kafka_psw: "${KAFKA_PASSWORD}"
  kafka_kck_id: "${KAFKA_KEYCLOAK_ID}" # manually added after keycloak configuration
  kafka_kck_scr: "${KAFKA_KEYCLOAK_SECRET}" # manually added after keycloak configuration 