## Step 1 : Build Dockerfile-Base 
Note: make sure your working directory is /e-mission-server
```bash
docker build -f Dockerfile-base -t emission-server-base . 
```


## Step 2: Build Dockerfile
```bash
docker build -t emission-server .
```

## Step 3: Initialize swarm and deploy using compose file. 
Follow the steps outlined here: https://docs.docker.com/get-started/part3/#run-your-new-load-balanced-app

The compose file is  located in the /docker directory.