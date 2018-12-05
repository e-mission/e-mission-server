# Docker usage instructions
This project is now published on dockerhub!
https://hub.docker.com/r/emission/e-mission-server/
and https://hub.docker.com/r/emission/e-mission-server-base/

Instructions on re-building the image are [in the build instructions](#Docker_Build_Instructions)

1. Initialize Swarm
    ```
   docker swarm init 
   ``` 
   For more details on how to do manage a swarm please see the official documentation: https://docs.docker.com/get-started/part4/ 


2. Configure the compose file. 
    * Update the port mappings and environment variables if necessary. 
    For more details on how to configure compose files please see the official documentation: https://docs.docker.com/compose/compose-file/#service-configuration-reference 

3. Deploy to swarm
    ```
    docker stack deploy -c docker-compose.yml emission
   ```
   There are many ways you can manage your deployment. Again, please see the official documentation for more details: https://docs.docker.com/get-started/part4/

4. Test your connection to the server
  * Using a web browser, go to [http://localhost:8080](http://localhost:8080)
  * Using safari in the iOS emulator, go to [http://localhost:8080](http://localhost:8080)
  * Using chrome in the android emulator, go to [http://10.0.2.2:8080](http://10.0.2.2:8080) 
    This is the [special IP for the current host in the android emulator](https://developer.android.com/tools/devices/emulator.html#networkaddresses)

### Docker Build Instructions
#### emission-server image

1. Build local docker image

   ```
   docker build -f docker/Dockerfile -t emission/e-mission-server:latest ./docker
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
