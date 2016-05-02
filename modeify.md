##### Modeify install

Below is a link to the given modeify install instructions which are stunningly vague
https://github.com/conveyal/modeify/tree/master/docs

I will walk you through my adventure of getting this thing to work.



### S3 Section

You need to set up an Amazon s3 Bucket to use for the project. 

1. Place the bucket name in deployment/config.yaml on bucket name like so 's3://bucket-name'

2. Run 'aws config' and fill out the access keys and s3 region 

3. Create an IAM user for whoever will run the app

4. Place that user's ID under the permissions for the s3 bucket.

### OpsWorks Section

You need an opsworks setup in order to deploy modeify

1. go to this website https://aws.amazon.com/opsworks/

2. Follow the instructions here https://aws.amazon.com/opsworks/getting-started/

3. Set up a Node.js application

4. Make sure to take note of the app_id, the layer_id, and the stack_id; you will need to place these in the file deployment/config.yaml

	I think that we can use the Open Trip Planner server as our EC2 server for the layer section.

5. Fill out the the otp_app_id, otp_layer_id, and stack_id
