# Locust Load Testing Setup (partially auto-generated using Copilot)

This directory contains the configuration and scripts required to perform load testing of the webapp. For greater versimilitude, it runs the webapp behind an nginx server.

## Files and Directories

### `docker-compose.loadtest.yml`
This file defines the Docker Compose setup for running load tests. It includes the following services:

- **locust**: Runs the Locust load testing tool.
- **nginxrp**: Runs an Nginx reverse proxy for routing and load balancing.
- **webapp**: Hosts the e-mission web application.
- **db**: Runs a MongoDB instance for the backend database.

### `nginx.conf`
The Nginx configuration file used by the `nginxrp` service. It defines routing rules, rate limiting, and proxy settings.

### `webapp.py`
A Python script defining Locust tasks for simulating user behavior and generating load on the web application.

## Usage

### Prerequisites
- Docker and Docker Compose must be installed on your system.

### Steps to Run
1. Start the containers using Docker Compose:
   ```bash
   docker-compose -f docker-compose.loadtest.yml up
   ```

2. The webapp and locust containers do not automatically start their servers, allowing easier development.

3. Activate webapp in the container (once)
   ```bash
   cd /usr/src/devapp && source setup/activate.sh
   ```

3. Run the webapp in the container manually (restart after code changes as needed)
   ```bash
   ./e-mission-py.bash emission/net/api/cfc_webapp.py
   ```

3. Setup locust in the container (once)
   ```bash
   source setup/activate.sh
   pip install locust
   ```

3. Run locust in the container manually (restart after code changes as needed)
   ```bash
   locust -f ../devapp/webapp.py
   ```

2. Access the Locust web interface at `http://localhost:8089`.

3. Configure the number of users and spawn rate in the Locust UI, then start the test.

4. Monitor the Nginx reverse proxy at `http://localhost:8081`.

5. Note that changes to the reverse proxy configuration currently require restarting the nginx container since it is not run in "dev mode" like the webapp and locust.

## Notes
- Ensure that the `nginx.conf` file is correctly configured for your testing needs. For example:
  - To simulate rate limiting, add or adjust the `limit_req` directives in the `http` block.
  - To test specific routing rules, modify the `location` blocks to point to the desired endpoints.

- Modify `webapp.py` to define custom user behavior for load testing. For example:
  - Use Locust's `HttpUser` class to define user tasks, such as login or data submission.
  - Adjust the `wait_time` attribute to simulate realistic user interaction delays.
  - Add custom headers or authentication tokens to mimic real-world API requests.
- Ensure that the required ports (8080, 8081, 8089) are not in use by other applications. You can check port usage with the following commands:
  ```bash
  netstat -tuln | grep <port>
  ```
  or
  ```bash
  lsof -i:<port>
  ```
## Troubleshooting
- If services fail to start, check the logs using:
  ```bash
  docker-compose -f docker-compose.loadtest.yml logs
  ```
- Ensure that the required ports (8080, 8081, 8089) are not in use by other applications.