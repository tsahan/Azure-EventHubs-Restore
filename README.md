# Purpose:
The function of this script is to iterate through the data recovered in response hub using the send.py script and replay it to the respective eventhub to be processed by data hub. 

# Pre-Req: 
* Python (3.11 or newer)
* The recovered files to be processed
	
# Execution Steps:
* Install the required packages using command 
	`pip install -r requirements.txt`
* Update following variables in send.py
	```
	EVENT_HUB_CONNECTION_STR = Connection string to tenant's datahub eventhub to which data is to be sent (eg. solarhub)
	EVENT_HUB_NAME = Tenant's datahub event hub to which data is to be sent
	root_folder = Root directory for tenant data files which contain messages to be replayed  
	log_file = The location for the log file to be created. Create a blank file called output.txt for log data to be stored in.
	```
* Execute the send.py file using following command 
	`python send.py`