import asyncio
import os
from datetime import datetime

# File locations
root_folder = r"<root_file_path>"
log_file = r"<log_file_path\output.txt>"


async def run():
    # Initialize the variables
    file_count = 0
    total_lines = 0
    special_count = 0
    event_count = 0
    
    for subdir, dirs, files in os.walk(root_folder):
        for file in files:
            file_count += 1
            msg_filename = os.path.join(subdir, file)
            num_lines = 0
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
                        has_special = 0
                        if msg_str.find("\\x") > 0:
                            has_special = 1                      
                        special_count += has_special
                                           
                    except Exception as err:
                        print(str(err))
                        with open(log_file, "a+") as log_f:
                            log_f.write(
                                "ERROR: " + str(err) + ", File path: " + msg_filename + ", Event: " + msg_str)
                        pass

                    # Reset the message
                    msg_str = ""
                    event_count += 1
                    
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
        log_f.write("Total num of events with special characters: " + str(special_count) + "\n")

def main():
    startTime = datetime.now()
    with open(log_file, "a+") as log_f:
        log_f.write(str(startTime.strftime("%Y-%m-%d %H:%M:%S")) + " ----------SCRIPT EXECUTION STARTED----------" + "\n")
    # Start processing the files
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
    # Finish processing the files
    with open(log_file, "a+") as log_f:
        log_f.write("Total processing time in seconds: " + str((datetime.now() - startTime)) + "\n")
        log_f.write(
            str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " ----------SCRIPT EXECUTION ENDED ----------" + "\n")
        
if __name__ == "__main__":
    # This code won't run if this file is imported.
    main()
