# NiFi

Repository containind NiFi configuration and deployment scripts for the CSC
Digital Twin

## Deployment Instructions

### Using ansible

_Warning, untested_

1. Copy `nifi.properties` to the deployment folder
2. Clone [certificate generator](git@git.mycloud-links.com:csc/csc-assets/generate-truststore-and-keystore.git) repo in `certificates` folder
3. Generate certificates
4. Customize `nifi.properties`
5. Run `ansible-playbook -i localhost, --connection=local ansible-deploy-dt.yml --ask-become-pass` to deploy

### Custom Processors

To install a custom processor run the appropriate script in the `scripts`
directory before bringing NiFi up.
