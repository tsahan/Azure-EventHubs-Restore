# Purpose:
The purpose of this project is to iterate through the data recovered in the response hub using the send.py script and replay it to the respective EventHub.

# Pre-Req: 
* Python (3.11 or newer)
* EventHub connection string to the files to be processed
	
# Execution Steps:
* Install the required packages using the command 
	`pip install -r requirements.txt`
* Update the following variables in send.py
	```
	EVENT_HUB_CONNECTION_STR = Connection string to tenant's datahub EventHub to which data is to be sent (eg. solarhub)
	EVENT_HUB_NAME = EventHub to which data is to be sent
	root_folder = Root directory for tenant data files which contain messages to be replayed  
	log_file = The location for the log file to be created.
	```
* Execute the send.py file using the following command 
	`python send.py`
