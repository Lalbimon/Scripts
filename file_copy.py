#!/usr/bin/python
# -*- coding: utf-8 -*-
import smtplib
import shutil
import os
import datetime
import ConfigParser
import fnmatch
import logging
import subprocess

curr_date = datetime.date.today().strftime('%d%m%Y')

logging.basicConfig(filename='/home/okdk/DWH_REPORT_Replicator/log/debug.log',format='%(asctime)s %(levelname)s:%(message)s',level=logging.DEBUG)

Config = ConfigParser.ConfigParser()
Config.read('/home/okdk/DWH_REPORT_Replicator/conf/configuration.txt')

SOURCE = Config.get('config', 'source_path')
DESTINATION = Config.get('config', 'destination_path')
BACK_FOLDER = Config.get('config', 'backFolder_path')

REMOTE_SERVER = Config.get('config', 'username') + '@'  + Config.get('config', 'destination_ip') + ':' + Config.get('config', 'location')

SLEEP_INTERVAL = Config.get('config', 'SLEEP_INTERVAL')

EMAIL_SUBJECT = Config.get('config', 'email_subject')
EMAIL_SUBJECT = EMAIL_SUBJECT + 'Date:' + curr_date
RECEIVERS = Config.get('config', 'receivers')
SENDER = Config.get('config', 'sender')
PASSWORD = Config.get('config', 'password')
FAILURE_MAIL_RECEIVER = Config.get('config', 'failureMailReceiver')

receiver_list = RECEIVERS.split()

s = smtplib.SMTP('10.88.7.37')

#########################################################################################
################################## File copy ############################################
#########################################################################################


def copy_file():
    copied_files = ''
    file_count = 0
    file_occurrence = 0
    src_files = os.listdir(SOURCE)
    for files in src_files:
        if fnmatch.fnmatch(files, '*' + curr_date + '*.csv.gz'):
            file_occurrence = file_occurrence + 1
            try:
                res = subprocess.Popen(['sshpass', '-p', PASSWORD, 'scp', os.path.join(SOURCE, files), REMOTE_SERVER],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                success, failure = res.communicate()
                if failure != "" and res.returncode != 0:
                    logging.debug("Error while copying the file " + files)
                    logging.debug(failure.strip())
                    send_failure_email(files)
                else:
                    logging.debug(files + " Copied to the remote server, path(" + SOURCE + ")")
                    shutil.move(os.path.join(SOURCE, files), os.path.join(BACK_FOLDER, files))
                    logging.debug(files + " Moved to " + BACK_FOLDER)
                    copied_files = copied_files + '\n' + files
                    file_count = file_count + 1
            except OSError as e:
                logging.debug(e)
                send_failure_email('Error')
    if file_count != 0:
        logging.debug("Total number of files copied " + `file_count`)
        send_success_email(file_count, copied_files)
    elif file_occurrence == 0:
        logging.debug("No files to copy")
        send_failure_email('No files')
		
		
##################################################################
###############Sending Success Email##############################
##################################################################


def send_success_email(no_of_files, content):
    TEXT = 'Hi Team,\n\n We have successfully Pushed ' + `no_of_files` + '\tFiles to DWH sftp\n\n\n File details :';
    TEXT=TEXT+'\n'+content+'\n\n\nRegards,\nFlytxt'

    message = 'Subject: {0}\n\n{1}'.format(EMAIL_SUBJECT, TEXT)
    for rec in receiver_list:
        try:
            s.sendmail(SENDER, rec, message)
            logging.debug("Mail send to " + rec)
        except smtplib.SMTPRecipientsRefused, e:
            logging.debug("Error while sending email ", e)
    s.quit()

##################################################################
###############Sending failure Email##############################
##################################################################


def send_failure_email(msg):
    if msg == 'No files':
        message = 'Subject: {0}\n\n{1}'.format("DWH Report: ERROR", "No files to copy" + '\n\n\nRegards,\nFlytxt')
    elif msg == 'Error':
        message = 'Subject: {0}\n\n{1}'.format("DWH Report: ERROR", "Unable to push the files" + "\n Please check the logs" + '\n\n\nRegards,\nFlytxt')
    else:
        message = 'Subject: {0}\n\n{1}'.format("DWH Report: ERROR", "Error while Copying the file '" + msg + "'\n Please check the logs" + '\n\n\nRegards,\nFlytxt')
    try:
        s.sendmail(SENDER,FAILURE_MAIL_RECEIVER,message)
        logging.debug("Failure mail send successfully..... ")
    except smtplib.SMTPRecipientsRefused, e:
        logging.debug("Error while sending mail")


if __name__ == '__main__':
    copy_file()


