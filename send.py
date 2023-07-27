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


async def run(batch_size):
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
                with open(msg_filename, "r") as f:
                    print("File name: ", msg_filename)
                    msg_str = ""
                    # Check if it is a multi-line message
                    for line in f:
                        num_lines += 1
                        if line.startswith("{ body: ") and ("timezone.utc) }" in line):
                            msg_str = line
                        else:
                            msg_str += line
                            if not (msg_str.startswith("{ body: ") and ("timezone.utc) }" in line)):
                                continue

                        # Process the message            
                        try:
                            # String manipulations on the messages
                            msg_str_body = msg_str[msg_str.find("{ body:") + 7: msg_str.find(", properties:")]
                            msg_str_body = msg_str_body.replace("'", "")
                            msg_str_properties = msg_str[
                                                 msg_str.find(", properties:") + len(", properties:"): msg_str.find(
                                                     ", offset:")]
                            # msg_str_properties = msg_str_properties.replace(r"\x", r"/x")
                            if msg_str_properties.find(", 'DSP-Response-Code'") > 0:
                                msg_str_properties = msg_str_properties[
                                                     :msg_str_properties.find(", 'DSP-Response-Code'")] + '}'
                            if msg_str_properties.find("'DSP-Id': UUID") > 0:
                                msg_str_properties = msg_str_properties.replace('UUID(', '').replace("')", "'")
                            if msg_str_properties.find("'DSP-Request-Type': 'PUT'") > 0:
                                if msg_str_properties.find(
                                        "'DSP-Type': 'NumericTimeSeries'") > 0 or msg_str_properties.find(
                                        "'DSP-Type': 'StringTimeSeries'") > 0:
                                    msg_str_properties = msg_str_properties.replace("'DSP-Request-Type': 'PUT'",
                                                                                    "'DSP-Request-Type': 'POST'")
                            # Convert the text to a Python dictionary
                            raw_properties = ast.literal_eval(msg_str_properties)
                            
                            # Convert byte strings to regular strings
                            decoded_properties = {}
                            for key, value in raw_properties.items():
                                decoded_key = key.decode("utf-8") if isinstance(key, bytes) else key                            
                                decoded_value = value.decode("utf-8") if isinstance(value, bytes) else value
                                decoded_properties[decoded_key] = decoded_value
                            
                            # Create an event
                            event = EventData(body=msg_str_body)
                            event.properties = decoded_properties
                            
                            # Add events to the batch while the batch size is not exceeded
                            if len(event_data_batch) < batch_size:                               
                                event_data_batch.add(event)

                            # Send batch of events
                            else:
                                print("Batch size: ", len(event_data_batch))
                                if event_data_batch.size_in_bytes > 1000000:
                                    print("The max batch size is exceeded.")
                                else:
                                    await producer.send_batch(event_data_batch)
                                    time.sleep(1)
                                # Add current message to a new batch
                                event_data_batch = await producer.create_batch()
                                event_data_batch.add(event)

                        except Exception as err:
                            print(str(err))
                            with open(log_file, "a+") as log_f:
                                log_f.write(
                                    "ERROR: " + str(err) + ", File path: " + msg_filename + ", Event: " + msg_str)
                            pass
                        # Reset the message
                        msg_str = ""
                        event_count += 1
                        
                    # Send remaining events that are not sent from the last batch
                    if len(event_data_batch) > 0:
                        print("Batch size: ", len(event_data_batch))
                        if event_data_batch.size_in_bytes > 1000000:
                            print("The max batch size is exceeded.")
                        else:
                            await producer.send_batch(event_data_batch)
                            # time.sleep(1)

                # Log for each file
                with open(log_file, "a+") as log_f:
                    output_time = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    log_f.write(output_time + " Completed processing: " + msg_filename + "\n")
                print("Number of lines in the file: ", num_lines)
                print(
                    "------------------------------------------------------------------------------------------------------")
                total_lines += num_lines

    # Log for the total process
    with open(log_file, "a+") as log_f:
        log_f.write("Total num of files: " + str(file_count) + "\n")
        log_f.write("Total num of lines: " + str(total_lines) + "\n")
        log_f.write("Total num of events: " + str(event_count) + "\n")

def main():
    startTime = datetime.now()
    with open(log_file, "a+") as log_f:
        log_f.write(str(startTime.strftime("%Y-%m-%d %H:%M:%S")) + " ----------SCRIPT EXECUTION STARTED----------" + "\n")
    # Start processing the files
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(2500))
    # Finish processing the files
    with open(log_file, "a+") as log_f:
        log_f.write("Total processing time in seconds: " + str((datetime.now() - startTime)) + "\n")
        log_f.write(
            str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " ----------SCRIPT EXECUTION ENDED ----------" + "\n")
        
if __name__ == "__main__":
    # This code won't run if this file is imported.
    main()
