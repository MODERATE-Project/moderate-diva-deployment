# Use the base nifi image
FROM docker.io/apache/nifi:2.0.0-M4

# Copy the certificate file to /opt/certs/ca.der
COPY ca.der /opt/certs/ca.der

USER root
# Run keytool to import the certificate
RUN keytool -importcert -noprompt -cacerts -alias root -storepass changeit -file /opt/certs/ca.der

USER nifi
# set entrypoint as the original image
CMD [ "/bin/bash", "-c", "/opt/nifi/scripts/start.sh" ]
