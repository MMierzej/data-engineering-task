#!/bin/bash


docker-compose -f buildconfig/docker-compose.yml up --build -d

echo; echo
echo "The service has been started."
echo "To shut down the service, execute the script:"
echo "    shutdown.sh"
echo
cat buildconfig/caution.txt
