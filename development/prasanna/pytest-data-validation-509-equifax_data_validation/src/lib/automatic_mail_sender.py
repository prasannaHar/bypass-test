from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import smtplib
import os

def automatic_mail_using_gmail_fun(arg_req_email_address, arg_subject, arg_mail_content, arg_required_attachments=None, arg_smtp_access_code=None, arg_sender=None):

    target_emails = arg_req_email_address.split(":")
    target_emails_new = [x + "@harness.io" for x in target_emails]

    subject = arg_subject
    content_txt = arg_mail_content
    attachments = arg_required_attachments

    ### Define email ###
    message = MIMEMultipart()
    message['From'] = Header(arg_sender+'@harness.io')
    message['To'] = ", ".join(target_emails_new)     
    message['Subject'] = Header(subject)
    # add content text

    message.attach(MIMEText(content_txt, 'html'))

    for attachment in attachments:
        # add attachment
        att_name = os.path.basename(attachment)
        att1 = MIMEText(open(attachment, 'rb').read(), 'base64', 'utf-8')
        att1['Content-Type'] = 'application/octet-stream'
        att1['Content-Disposition'] = 'attachment; filename=' + att_name
        message.attach(att1)
        
    ### Send email ###
    server = smtplib.SMTP_SSL('smtp.gmail.com', 465) 
    server.login(arg_sender+'@harness.io', arg_smtp_access_code)
    server.sendmail(arg_sender+'@harness.io', target_emails_new, message.as_string()) 
    server.quit() 
    print('Sent email successfully')       


    return True
