### Executing IoT core thing registration script from local machine which calls IoT core thing APIs

-----------------------------------------------------------------------------------------------------------------------
    1. Create IAM role `capstoneagritechfarmrole1` to allow AWS IoT Core to call other AWS services. It is necessary to use `start_thing_registration_task()` function. Created IAM role must have minimum the following permission policies attached:
        a. AWSIoTThingsRegistration (AWS managed policy)
        b. AmazonS3ReadOnlyAccess (AWS managed policy)
    2. Open the file gl_capstone_project_agri_tech_farm/scripts/iot_s3_config.py, and change the variable-values ACCOUNT_ID, ENDPOINT, IOT_CORE_REGION, S3_REGION, MQTT_PORT & TOPIC as per requirement, rest of the variable-values can be kept the same.
    3. Install python libraries, 
            cd gl_capstone_project_agri_tech_farm/
            pip3 install requirement.txt
    4. Run the script, 
            cd scripts/
            python3 things_registration_main.py
            This script typically takes around 5 mins to run all the steps of registration.

-----------------------------------------------------------------------------------------------------------------------
### Executing the soil_sensor simulator from the EC2 instance

-----------------------------------------------------------------------------------------------------------------------
    1. Launch a EC2 Ubuntu instance from the AWS console
    2. SSH to the EC2 instance
    3. From the home path execute "git clone https://github.com/srijibrockz95/gl_capstone_project_agri_tech_farm.git" to copy the project to the EC2 instance.
    4. Enter the following command to provide execute permissions to the soil simulator setup script
		chmod +x gl_capstone_project_agri_tech_farm/scripts/EC2_Simulator_setup.sh
    5. Execute the soil simulator setup script by entering the following commands:
		cd gl_capstone_project_agri_tech_farm/scripts
		./EC2_Simulator_setup.sh
		The script is interactive. Please enter “Y” for all package installation requests. You will also be requested for AWS access id and access key.

-----------------------------------------------------------------------------------------------------------------------
### Setting up Kinesis Data Stream,Kinesis Delivery Stream and Kinesis Analytics App:

-----------------------------------------------------------------------------------------------------------------------
    1. Create following IAM Roles before starting with Kinesis Flow:
        1. IOT_TO_Kinesis
        Permissions policies: AWSIoTThingsRegistration,AWSIoTLogging,AWSIoTRuleActions,AmazonKinesisFullAccess
        2. FirhoseToS3
        Permissions policies: AmazonS3FullAccess,AmazonKinesisFullAccess
        3. kinesis-analytics-App01-us-east-1
        Permissions policies: AmazonKinesisFullAccess,AWSLambda_FullAccess

    2. Create Kinesis Data Stream:
        Kinesis->Data Stream-> Create Data Stream:
            Name:raw_data_stream
            Data Stream Capacity:
                Capacity mode:Provisioned
                Provisioned shards:1

    3. Create  IOT Rule to transfer Data from IOT Core to Kinesis Data Stream:
        AWS IOT Core->Act->Rules->Create Rule:
            Rule Name:IOTTTOKinesis
            Configure SQL Statement:
                Specify SQL Version:2016-03-23
                SQL Statement:SELECT * FROM 'iot/agritech'
                Rule Actions:Kinesis Stream
                    Stream Name:raw_data_stream
                    Partition key:sprinkler_id
                    IAM Role:IOT_TO_Kinesis
                Create.

    4. Create Delivery Stream(Data Firehose):
        Kinesis->Delivery Streams->Create Delivery Stream:
            Source: Amazon Kinesis Data Streams
            Destination: Amazon S3
            Source Settings:
                Kinesis Data Stream: raw_data_stream
            Delivery stream name: raw_delivery_stream
            Destination Settings:
                S3 bucket:capstoneagritechfarm20
            Advanced Settings:
                Permissions:
                    Choose existing IAM role:FirehoseToS3

    5. Create Kinesis SQL Legacy App:
        Kinesis->Analytics Applications->SQL applications (legacy)->Create SQL Application(Legacy)->
            Application Name: App-01
            Create
        
            1. Source->Configure:
            Source:Kinesis Data Stream
            Kinesis Data Stream: raw_data_stream
            IAM role for reading source stream: kinesis-analytics-App01-us-east-1
            Discover Schema: Check for the values populating and Save Changes
            Save Changes
        
            2. Real-time analytics->Configure:
                -Add the SQL Code(Refer to gl_capstone_project_agri_tech_farm/Kinesis/Kinesis_Analytics_SQL_Code for sql code) in code editor.
                -Save and run application.
                -After running,2 inline streams gets created:AGGREGATE_SQL_STREAM,ANOMALY_SQL_STREAM
                -Connect streams to destination:
                (Note:For Connecting to destination, first create the Lambda functions defined in next step. At last connect these destinations)
                    Connect Destination for AGGREGATE_SQL_STREAM:Aggregatefn
                    Connect Destination for ANOMALY_SQL_STREAM:Anomalyfn

------------------------------------------------------------------------------------------------------------------
### Setting up Dynamodb, SNS, Lambdas, MQTT publish and python flask dashboard

------------------------------------------------------------------------------------------------------------------
    Dynamodb and SNS:
    1. Update Endpoint in dbsetup_utils.py file inside the "create_sns" method with your email address
    2. Run the file dbsetup_main.py file to create DynamoDB tables and sns
    3. Open your email inbox and confirm sns subscription.

    Lambdas:
        Go to AWS lambda console.

        Create a layer in Lambda:
            1. Create a layer in Lambda by clicking on the Layers menu on the left side of AWS Lambda console
            2. Give name as “pyowm”.
            3. Upload python.zip file which can be found inside Lambda folder of solution.
            4. Select python 3.9 as runtime and x86_64 as architecture.
            5. Click on Create button.

        Create Lambdas:
            1. Create a lambda function named Aggregatefn and select “Create a new role with basic lambda permissions”, Runtime – Python 3.9, Architecture – x86_64
            2. Copy the contents of Aggregate_Lambda_Handler.py file under Lambda folder of solution to the lambda code. 
            3. Change the lambda timeout to 3 mins under configuration tab.
            4. Attach policies for accessing Cloud watch, DynamoDB, IoT and SNS to the newly created role.
            5. Create another lambda function named Anomalyfn and copy the contents of Anomaly_Lambda_Handler.py file to the lambda code.
            6. Change the lambda timeout to 3 mins under configuration tab.
            7. Add a layer for Anomalyfn lambda and add pyown layer as custom layer
            8. Use the same role that was created for the Aggregatefn.
        
    MQTT publish:
        1. Go to AWS console and open IoT Core -> Test -> MQTT test client
        2. Subscribe to the topic weather_data

    Python Flask Dashboard:
	    Run the file display.py under the webapp folder and go to: http://127.0.0.1:5000/ sensor and sprinkler dashboard can be seen.

-----------------------------------------------------------------------------------------------------------------------
### Nodered dashboard

-----------------------------------------------------------------------------------------------------------------------
    1. Open a new SSH connection to the EC2 instance.
        Enter “node-red”
        http://<public ip>:1880/

        OR

        Open node-red on your local computer by entering the command “node-red”
        http://127.0.0.1:1880

    2. Import the json file containing the flow into node-red. The file is attached with the project - “Final_Dashboard_Nodered”
    3. Open the “DynamoDB Scan” node and update the AWS Region, access id and secret key
    4. Login to the AWS console and create a new thing called “Nodered_Client”. Download the certificate and private key and Amazon CA certificate as well.
    5. Open the MQTT IN node and update the topic as “iot/agritech”
            Update the server details by providing the thing end point address in the server field and setting port as 8883
            Click on “Use TLS” and update the TLS configuration by uploading the certificate, private key and root CA.
    
    6. Open the dashboard with the link “http://127.0.0.1:1880/ui”
