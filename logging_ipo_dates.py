import os
import logging
from datetime import date, timedelta
import configparser
import pandas as pd
import win32com.client as win32
from pg_connection import pg_connection, convert_cols_db


log_file = 'IPO Monitoring Logs.txt'
log_folder = os.path.join(os.getcwd(), 'Logs')
screenshot_folder = os.path.join(log_folder, 'Screenshots')
prev_log_folder = os.path.join(log_folder, 'Previous Logs')
today_date = date.today().isoformat()

for folder in [log_folder, screenshot_folder]:
    if not os.path.exists(folder):
        os.mkdir(folder)

if date.today().day == 1:
    prev_log_file = os.path.join(prev_log_folder, f"IPO Monitoring Logs {(date.today() - timedelta(days=1)).isoformat()}.txt")
    if not os.path.exists(prev_log_file):
        os.rename(src=os.path.join(log_folder, log_file), dst=prev_log_file)

handler = logging.FileHandler(os.path.join(log_folder, log_file), mode='a+', encoding='UTF-8')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def error_email(error_message: str = ''):
    """
    Used to send an email when an error is encountered.
    Email details like sender and recipients are provided in .ini file which is read by configparser
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
    mail.Attachments.Add(os.path.join(log_folder, log_file))
    mail.Send()


def consolidate_webscraping_results(num_recent: int = 30):
    df = pd.read_csv(os.path.join(log_folder, 'Webscraping Results.csv'))
    df.sort_values(by=['time_checked'], ascending=False, inplace=True)

    failures = df.loc[df['result'] == 0].drop_duplicates(subset='source', ignore_index=True)
    failures.rename(columns={'time_checked': 'most_recent_failure'}, inplace=True)
    failures.drop(columns='result', inplace=True)

    successes = df.loc[df['result'] == 1].drop_duplicates(subset='source', ignore_index=True)
    successes.rename(columns={'time_checked': 'most_recent_success'}, inplace=True)
    successes.drop(columns='result', inplace=True)

    recent_checks = df['time_checked'].unique().tolist()[:num_recent]
    df_recent = df.loc[df['time_checked'].isin(recent_checks)]
    df_recent = df_recent.groupby('source')['result'].sum().reset_index()
    df_recent.sort_values(by=['result'], inplace=True)
    df_recent['Recent Success Rate'] = round((df_recent['result'] / num_recent) * 100, 2)

    df_recent = df_recent.merge(failures, how='left', on='source')
    df_recent = df_recent.merge(successes, how='left', on='source')
    df_recent.to_csv(os.path.join(log_folder, 'Recent Webscraping Performance.csv'), index=False, encoding='utf-8-sig')
    conn = pg_connection()
    try:
        df_all = pd.read_csv(os.path.join('Logs', 'Webscraping Results.csv'))
        df_all.columns = convert_cols_db(df_all.columns)
        df_all.to_sql('webscraping_results', conn, if_exists='replace', index=False)

        df = pd.read_csv(os.path.join('Logs', 'Recent Webscraping Performance.csv'))
        df.columns = convert_cols_db(df.columns)
        df.to_sql('webscraping_results_recent', conn, if_exists='replace', index=False)
    except Exception as e:
        logger.error(e)
    finally:
        conn.close()


if __name__ == '__main__':
    consolidate_webscraping_results()
