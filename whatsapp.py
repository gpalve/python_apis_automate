import pywhatkit 
from datetime import datetime
currentDateAndTime = datetime.now()

# Read the text file
with open('data_to_send.txt', 'r') as f:
    text = f.read()
    # print(text)

# # using Exception Handling to avoid unexpected errors
try:
    pywhatkit.sendwhatmsg_to_group("Bdc68uu5RXV5wkthMYJxnz", text, currentDateAndTime.hour, currentDateAndTime.minute+1)
 
    print("Message Sent!") #Prints success message in console
 
     # error message
except: 
    print("Error in sending the message")
open('data_to_send.txt', 'w').close()
