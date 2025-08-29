#!/bin/bash
# Create certs directory if it doesn't exist
mkdir -p certs

# Download the MongoDB Atlas CA certificate
curl -o certs/ca-certificate.crt https://www.digicert.com/CACerts/BaltimoreCyberTrustRoot.crt

# Install Python dependencies
pip install -r requirements.txt