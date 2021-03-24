import os
import logging
from datetime import date
import configparser
import win32com.client as win32


log_file = 'IPO Monitoring Logs.txt'
log_folder = os.path.join(os.getcwd(), 'Logs')
screenshot_folder = os.path.join(log_folder, 'Screenshots')
today_date = date.today().strftime('%Y-%m-%d')

for folder in [log_folder, screenshot_folder]:
    if not os.path.exists(folder):
        os.mkdir(folder)
handler = logging.FileHandler(os.path.join(log_folder, log_file), mode='a+', encoding='UTF-8')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.info('-' * 100)


def error_email(error_message: str = ''):
    """
    Used to send an email when an error is encountered.
    Email details like sender and recipients are provided in .ini file which is read by configparser.
    :param error_message: optional string that will be added to body of email
    :return: None
    """
    config = configparser.ConfigParser()
    config.read('email_settings.ini')
    outlook = win32.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)
    mail.To = config.get('Email', 'ErrorTo')
    mail.Sender = config.get('Email', 'Sender')
    mail.Subject = f"ERROR: {config.get('Email', 'Subject')} {today_date}"
    mail.HTMLBody = config.get('Email', 'ErrorBody') + error_message + config.get('Email', 'Signature')
    mail.Attachments.Add(log_file)
    mail.Send()


def consolidate_webscraping_results():
    # ToDo: review webscraping results to see how the webscraper is performing
    pass
