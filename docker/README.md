### Docker Usage Instructions

1. Build docker image

   ```
   docker build -t e-mission-server:latest .
   ```

2. Create docker network

   ```
   docker network create e-mission --driver=bridge
   ```
   
3. Run the server

   Assuming `config` files are located at `docker/config` folder

   ```
   docker run -d \
     -p 8080:8080 \
     --name=e-mission-server-1 \
     --net="e-mission" \
     --mount type=bind,source="$(pwd)"/docker/config/db.conf,target=/usr/src/app/conf/storage/db.conf,readonly \
     --mount type=bind,source="$(pwd)"/docker/config/webserver.conf,target=/usr/src/app/conf/net/api/webserver.conf,readonly \
     --mount type=bind,source="$(pwd)"/docker/config/google_auth.json,target=/usr/src/app/conf/net/auth/google_auth.json,readonly \
     e-mission-server:latest
   ```
