import os
import sys
from datetime import date
import configparser
import win32com.client as win32
from logging_ipo_dates import logger, error_email
import pandas as pd

config = configparser.ConfigParser()
config.read('email_settings.ini')
today_date = date.today().strftime('%Y-%m-%d')


def email_report(attach_file=None, addtl_message: str = ''):
    """
    Emails the report as an attachment.
    Email details like sender and recipients are provided in .ini file which is read by configparser.
    :param attach_file: path of a file or list of files which will be attached to the email
    :param addtl_message: optional string that can be added to body of email
    :return: None
    """
    outlook = win32.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)
    mail.To = config.get('Email', 'To')
    mail.Sender = config.get('Email', 'Sender')
    mail.Subject = f"{config.get('Email', 'Subject')} {today_date}"
    mail.HTMLBody = config.get('Email', 'Body') + addtl_message + config.get('Email', 'Signature')
    if isinstance(attach_file, str) and os.path.exists(attach_file):
        mail.Attachments.Add(attach_file)
    elif isinstance(attach_file, list):
        for f in attach_file:
            mail.Attachments.Add(f)
    mail.Send()
    logger.info('Email sent')


def main(file_attachment: str, addtl_message: str = ''):
    try:
        email_report(attach_file=file_attachment, addtl_message=addtl_message)
    except Exception as e:
        logger.error(e, exc_info=sys.exc_info())
        error_email(str(e))


if __name__ == '__main__':
    file = os.path.join(os.getcwd(), 'Results', 'IPO Monitoring.xlsx')
    df_summary = pd.read_excel(file, sheet_name='Summary')
    main(file_attachment=file, addtl_message=df_summary.to_html(na_rep="", index=False, justify="left"))
