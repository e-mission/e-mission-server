### Docker Usage Instructions

1. Build docker image

   ```
   docker build -t e-mission-server:latest .
   ```

2. Create docker network

   We will be creating network name `e-mission` which will allows any docker container in the network refer to each other by its `name` instead of IP Address which can be changed over time.
   
   ```
   docker network create e-mission --driver=bridge
   ```
   
3. Run MongoDB

   We will be using Official MongoDB Docker image, so no need to build one.

   ```
   docker run -d \
     --name=e-mission-mongo-1 \
     --net="e-mission" \
     --restart=always \
     --mount source=e-mission-mongo-1_data,target=/data/db \
     --mount source=e-mission-mongo-1_config,target=/data/configdb \
     mongo:latest \
       --bind_ip_all
   ```
   
   FOR ADVANCED USER: If you would like to access to MongoDB directly for debugging purpose, you can add the line
   
   ```
     -p 27017:27017 \
   ```
   
   and access it like MongoDB is running on your host machine.
   
4. Configure `db.conf`

   ```
   {
     "timeseries": {
       "url": "e-mission-mongo-1"
     }
   }
   ```
   
5. Run the server

   Assuming `config` files are located at `docker/config` folder

   ```
   docker run -d \
     -p 8080:8080 \
     --name=e-mission-server-1 \
     --net="e-mission" \
     --restart=always \
     --mount type=bind,source="$(pwd)"/conf/storage/db.conf.docker.sample,target=/usr/src/app/conf/storage/db.conf,readonly \
     --mount type=bind,source="$(pwd)"/conf/net/api/webserver.conf.docker.sample,target=/usr/src/app/conf/net/api/webserver.conf,readonly \
     --mount type=bind,source="$(pwd)"/docker/config/google_auth.json,target=/usr/src/app/conf/net/auth/google_auth.json,readonly \
     e-mission-server:latest
   ```
