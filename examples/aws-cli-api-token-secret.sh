#!/bin/bash
aws secretsmanager create-secret \
  --name "clumio/automation/api-token" \
  --description "Secret containing the Clumio API token" \
  --secret-string '{"token":"<put-your-api-token-here>"}' \
  --tags Key=clumio,Value=automation
