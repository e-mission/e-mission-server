# Docker Instructions

## Docker usage instructions
This project is now published on dockerhub!
https://hub.docker.com/r/emission/e-mission-server/

Instructions on re-building the image are at [in the build instructions](#Docker_Build_Instructions)

1. Create docker network

   We will be creating network name `e-mission` which will allows any docker container in the network refer to each other by its `name` instead of IP Address which can be changed over time.
   
   ```
   docker network create e-mission --driver=bridge
   ```
   
2. Run MongoDB

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
   
3. Run the server

   ```
   docker run -d \
     -p 8080:8080 \
     --name=e-mission-server-1 \
     --net="e-mission" \
     --restart=always \
     --mount type=bind,source="$(pwd)"/conf/storage/db.conf.docker.sample,target=/usr/src/app/conf/storage/db.conf,readonly \
     --mount type=bind,source="$(pwd)"/conf/net/api/webserver.conf.docker.sample,target=/usr/src/app/conf/net/api/webserver.conf,readonly \
     emission/e-mission-server:latest
   ```

1. Test your connection to the server
  * Using a web browser, go to [http://localhost:8080](http://localhost:8080)
  * Using safari in the iOS emulator, go to [http://localhost:8080](http://localhost:8080)
  * Using chrome in the android emulator, go to [http://10.0.2.2:8080](http://10.0.2.2:8080) 
    This is the [special IP for the current host in the android emulator](https://developer.android.com/tools/devices/emulator.html#networkaddresses)

### Docker Build Instructions

1. Build local docker image

   ```
   docker build -f docker/Dockerfile -t emission/e-mission-server:latest .
   ```

1. Tag the release (make sure you are in the owners group for emission, or
    replace emission by your own namespace)

   ```
   docker tag emission/e-mission-server:latest emission/e-mission-server:<version>
   ```
   
1. Push the release 

   ```
   docker login
   docker push emission/e-mission-server:<version>
   docker push emission/e-mission-server:latest
   ```
