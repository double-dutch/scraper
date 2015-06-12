### Importing libraries
import smtplib
from email.mime.text import MIMEText

# Function for emailing updates
def email_update(subject,from_add,passwd):

    # Message
    msg = MIMEText('')
    msg['Subject'] = subject
    msg['From'] = from_add
    msg['To'] = 'aph2126@columbia.edu'

    # Send the message via our own SMTP server, but don't include the
    # envelope header.
    server = smtplib.SMTP('smtp.gmail.com',587) #port 465 or 587
    server.ehlo()
    server.starttls()
    server.ehlo()
    server.login(from_add,passwd)
    server.sendmail(from_add,'aph2126@columbia.edu',msg.as_string())
    server.close()
