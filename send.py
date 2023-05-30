import asyncio
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
import time
import os
from datetime import datetime
import ast

# Connection details
EVENT_HUB_CONNECTION_STR = "<connection_string>"
EVENT_HUB_NAME = "<event_hub_name>"

# File locations
root_folder = r"<root_file_path>"
log_file = r"<log_file_path\output.txt>"

async def run(BATCH_SIZE):
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
        for subdir, dirs, files in os.walk(root_folder):
            for file in files:
                file_count += 1
                mfilename = os.path.join(subdir, file)
                msg_filename = mfilename
                # Create a batch.
                event_data_batch = await producer.create_batch()
                # batch = 0
                num_lines = 0
                # msg_filename ="files/0.txt"
                with open(msg_filename, 'r') as f:
                    print('File name: ', msg_filename)
                    msg_str = ''
                    for line in f:
                        num_lines += 1
                        if line.startswith('{ body: ') and ('timezone.utc) }' in line):
                            msg_str = line
                        else:
                            #line.strip('\n')
                            msg_str += line
                            if msg_str.startswith('{ body: ') and ('timezone.utc) }' in line):
                                pass
                            else:
                                continue
                        try:
                            msg_str = msg_str.replace(" b'", " '").replace(" {b'", " {'")
                            msg_str_body = msg_str[msg_str.find('{ body:') + 7: msg_str.find(", properties:")]
                            msg_str_body = msg_str_body.replace("'", '')
                            msg_str_properties = msg_str[
                                                 msg_str.find(", properties:") + len(', properties:'): msg_str.find(
                                                     ", offset:")]
                            msg_str_properties = msg_str_properties.replace(r"\x", r"/x")
                            if msg_str_properties.find(", 'DSP-Response-Code'") > 0:
                                msg_str_properties = msg_str_properties[:msg_str_properties.find(", 'DSP-Response-Code'")] + '}'
                            if msg_str_properties.find("'DSP-Id': UUID") > 0:
                                msg_str_properties = msg_str_properties.replace('UUID(', '').replace("')","'")
                                # msg_str_properties = msg_str_properties[:msg_str_properties.find ('"UUID(')] + '"' +  msg_str_properties[msg_str_properties.find (', DSP-Response-Code:')+45:]
                                # msg_str_properties = msg_str_properties[:msg_str_properties.find(
                                #     ", 'DSP-Request-Type':")] + '"' + msg_str_properties[msg_str_properties.find(
                                #     ", 'DSP-Request-Type':"):]
                                # print(msg_str_properties)
                            # batch +=1
                            if (len(event_data_batch) < BATCH_SIZE):
                                event = EventData(body=msg_str_body)
                                event.properties = ast.literal_eval(msg_str_properties)
                                # Add events to the batch.
                                event_data_batch.add(event)
                                event_count += 1
                                msg_str=''
                            else:
                                print('Batch size: ', len(event_data_batch))
                                if (event_data_batch.size_in_bytes > 1000000):
                                    print("The max batch size is exceeded.")
                                else:
                                    await producer.send_batch(event_data_batch)
                                    time.sleep(1)
                                event_data_batch = await producer.create_batch()
                                event = EventData(body=msg_str_body)
                                event.properties = ast.literal_eval(msg_str_properties)
                                # Add events to the batch.
                                event_data_batch.add(event)
                                event_count += 1
                                # batch = 0
                        except Exception as err:
                            with open(log_file, 'a') as log_f:
                                print(str(err))
                                log_f.write(
                                    'Error encountered while processing: ' + msg_str + ' in the file ' + msg_filename + '\n')
                                log_f.write(str(err))
                                log_f.write(msg_str_body + '\n')
                                log_f.write(msg_str_properties + '\n')
                            pass
                            # continue
                    if len(event_data_batch) > 0:
                        print('Batch size: ', len(event_data_batch))
                        if (event_data_batch.size_in_bytes > 1000000):
                            print("The max batch size is exceeded.")
                        else:
                            await producer.send_batch(event_data_batch)
                            #time.sleep(1)
                        event_data_batch = await producer.create_batch()
                        # batch = 0
                with open(log_file, 'a') as log_f:
                    output_time = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    log_f.write(output_time + ' Completed processing: ' + msg_filename + '\n')
                # quit()
                print('Number of lines in the file: ', num_lines)
                print('------------------------------------------------------------------------------------------------------')
                total_lines += num_lines
    with open(log_file, 'a') as log_f:
        log_f.write('Total num of events: ' + str(event_count) + '\n')
        log_f.write('Total num of lines: ' + str(total_lines) + '\n')
        log_f.write('Total num of files: ' + str(file_count) + '\n')

startTime = datetime.now()
with open(log_file, 'a') as log_f:
    log_f.write(str(startTime.strftime("%Y-%m-%d %H:%M:%S")) + ' ----------SCRIPT START----------' + '\n') 
loop = asyncio.get_event_loop()
loop.run_until_complete(run(2500))
with open(log_file, 'a') as log_f:
    log_f.write('Total processing time in seconds: ' + str((datetime.now() - startTime)) + '\n')
    log_f.write(str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + ' ----------SCRIPT END----------' + '\n')





