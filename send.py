import asyncio
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
import asyncio
import json
import uuid
import datetime,dateutil
import time
import os
from azure.eventhub import EventHubConsumerClient, EventData
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.aio import EventHubProducerClient
from dateutil import parser
import dateutil.parser
from datetime import date, datetime,timedelta
from azure.identity import DefaultAzureCredential
import logging 
import ast

# Notes: Remove unneccessary packages
EVENT_HUB_CONNECTION_STR = ""
EVENT_HUB_NAME = ""
BATCH_SIZE = 1000
   
async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
    )

    event_count = 0
    file_count = 0
    total_lines = 0
    async with producer:
        # Root folder
        root_folder = ""
        log_file = ""
        
        for subdir, dirs, files in os.walk(root_folder):
            for file in files:
                file_count += 1
                mfilename = os.path.join(subdir, file)
                msg_filename = mfilename
                # Create a batch.
                event_data_batch = await producer.create_batch()
                # can we add more events to the existing batch
                # Add events to the batch.
                # batch = 0
                num_lines = 0
                y = 0
                # msg_filename ="files/0.txt"
                with open(msg_filename,'r') as f:
                    print('\033[33mFile name: \033[0m', msg_filename)
                    for line in f:
                        num_lines += 1
                        try:
                            msg_str = line.replace ("b'", "'")
                            msg_str_body = msg_str[msg_str.find ('{ body:')+7 : msg_str.find (", properties:")]
                            msg_str_body = msg_str_body.replace ("'",'')
                            msg_str_properties = msg_str[msg_str.find (", properties:")+ len(', properties:') : msg_str.find(", offset:") ]
                            msg_str_properties = msg_str_properties.replace(r"\x",r"/x")
                            if msg_str_properties.find ("'DSP-Id': UUID") >0:
                                msg_str_properties = msg_str_properties.replace ('UUID(','"UUID(') 
                                #msg_str_properties = msg_str_properties[:msg_str_properties.find ('"UUID(')] + '"' +  msg_str_properties[msg_str_properties.find (', DSP-Response-Code:')+45:] 
                                msg_str_properties = msg_str_properties[:msg_str_properties.find (", 'DSP-Request-Type':")] + '"' + msg_str_properties[msg_str_properties.find (", 'DSP-Request-Type':"):]
                            # batch +=1
                            if (len(event_data_batch) < BATCH_SIZE):
                                event = EventData(body= msg_str_body)
                                # print(event)
                                event.properties = ast.literal_eval(msg_str_properties)
                                event_data_batch.add(event)
                                event_count += 1
                            else:
                                print('\033[32mBatch size: \033[0m', len(event_data_batch))
                                await producer.send_batch(event_data_batch)    
                                event_data_batch = await producer.create_batch()
                                event = EventData(body= msg_str_body)
                                event.properties = ast.literal_eval(msg_str_properties)
                                event_data_batch.add(event)
                                event_count += 1
                                # batch = 0
                        except Exception as err:
                            with open(log_file, 'a') as log_f:
                                print("We're in the exception")
                                print(str(err))
                                log_f.write ('Error encountered while processing:' + msg_str + 'in the file' + msg_filename + '\n')
                                log_f.write (str(err))
                                log_f.write (msg_str_body + '\n')
                                log_f.write(msg_str_properties + '\n')
                                log_f.close()
                            pass 
                        # continue
                    if len(event_data_batch) > 0:
                        print('\033[32mBatch size: \033[0m', len(event_data_batch))
                        await producer.send_batch(event_data_batch)    
                        event_data_batch = await producer.create_batch()                               
                        # batch = 0
                    f.close()
                with open(log_file, 'a') as log_f:
                    output_time = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    log_f.write (output_time + ' Completed processing: ' + msg_filename + '\n')
                    log_f.close()
                # quit()
                print('\033[32mNumber of lines in the file:\033[0m', num_lines)
                total_lines += num_lines
    print('\033[35mTotal num of events: \033[0m', event_count)
    print('\033[35mTotal num of lines: \033[0m', total_lines)
    print('\033[35mTotal num of files: \033[0m', file_count)

startTime = datetime.now()
print('The script has started running at ', startTime)
loop = asyncio.get_event_loop()
loop.run_until_complete(run())
print('\033[35mTotal processing time in seconds: \033[0m', datetime.now() - startTime)
print('The script has finished running at ', datetime.now())




