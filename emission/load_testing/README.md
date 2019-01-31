# Load testing instructions
Load testing is done using the locust package. To learn more about how to use locust
see the official documentation: 

This directory includes a sample load test 'user1.py'. To run the test use the following command:
```bash
./e-mission-locust.bash -f emission/load_testing/user1.py --host=http://localhost:8080
```
Note: make sure the webserver is running before you execute the command. 

Instructions on how to create a test file. 

1. Define user behavior 
   see emission/load_testing/conf/user1.json for an 
   
   Here is an example of how you can define a fake user. 
```json
{
            "email" : "fake_user_129",

            "locations" :
            [
               {
                    "label": "home",
                    "coordinate": [37.77264255,-122.399714854263]
                },

                {
                    "label": "work",
                    "coordinate": [37.42870635,-122.140926605802]
                },
                {
                    "label": "family",
                    "coordinate": [37.87119, -122.27388]
                }
            ],
            "transition_probabilities":
                    
            [
                [0.32833882, 0.06245342, 0.60920776],
                [0.57634164, 0.20089474, 0.22276363],
                [0.85068322, 0.07665405, 0.07266273]
            ],

            "modes" :
            {
                "CAR" : [["home", "family"]],
                "TRANSIT" : [["home", "work"], ["work", "home"]]
            },

            "default_mode": "CAR",
            "initial_state" : "home",
            "radius" : ".1"
}
```

* email: string used to identify user.  
* locations
    * label - this can be any string
    * coordinate: must be a coordinate supported by your OTP instance
    
* transition probabilities.User behavior is modeled using a markov model. Must be a NxN matrix where N is the number of locations. Also note that the rows in the matrix must add up to one. 

* modes - Here you specify the mode of transportation between locations. The mode must be supported by your OTP instance.
* default mode -
* initial_state - must correspond to a label in your locations array. 
* radius - specifies how