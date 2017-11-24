# Metaverse MySQL Sync
Docker image to sync from Metaverse mvsd to MySQL.

# Setup
To configure the sync service you need to set the environment variables:
- DB_NAME
- DB_PORT
- DB_HOST
- DB_PASS
- DB_USER
- MVSD_HOST
- MVSD_PORT
- MAX_RETRY

# Example
You can find an example configuration in the examples folder. To start it you will need to have docker-compose installed.
``` bash
docker-compose up
```
