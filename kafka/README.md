# Kafka Docker Compose Setup

This repository contains a Docker Compose setup for running a Kafka ecosystem including Kafka broker, Schema Registry, ksqlDB Server, Kafka REST Proxy, and Kafka UI.

# Troubleshooting

## Keycloak Self-Signed Certificates

In case of appearance of an error like: `PKIX path building failed` or `unable to find valid certification path to requested target`, the problem statistically is this: JVM is not able to authenticate the self signed certificate used. To make it recognize the CA, you have to import the ca.der in the `cacerts` of the JVM. The command to do so is: `keytool -importcert -noprompt -cacerts -alias alias_name -storepass changeit -file /path/to/ca.der`.  
**NB** The password: `changeit` is the cacerts default password
