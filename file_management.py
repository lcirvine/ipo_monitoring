import os
import sys
from datetime import datetime, timedelta
from logging_ipo_dates import logger, error_email


def delete_old_files(folder: str, num_days: int = 30) -> list:
    """
    Deletes files older than the number of days given as a parameter. Defaults to delete files more than 30 days old.
    :param folder: folder location files will be deleted from
    :param num_days: int specifying the number of days before a file is deleted
    :return: list of files that were deleted
    """
    old_date = datetime.utcnow() - timedelta(days=num_days)
    files_deleted = []
    for root, dirs, files in os.walk(folder):
        for file in files:
            f_abs = os.path.join(root, file)
            f_modified = datetime.fromtimestamp(os.path.getmtime(f_abs))
            if f_modified <= old_date:
                os.unlink(f_abs)
                files_deleted.append(file)
    if len(files_deleted) > 0:
        logger.info(f"Deleted {files_deleted}")
    return files_deleted


def delete_old_files_test(folder: str, num_days: int = 30) -> list:
    """
    Used to test the delete_old_files function. This will only print the name of the files that would be deleted.
    :param folder: folder location files will be deleted from
    :param num_days: int specifying the number of days before a file is deleted
    :return: list of files that were deleted
    """
    old_date = datetime.utcnow() - timedelta(days=num_days)
    files_deleted = []
    for root, dirs, files in os.walk(folder):
        for file in files:
            f_abs = os.path.join(root, file)
            f_modified = datetime.fromtimestamp(os.path.getmtime(f_abs))
            if f_modified <= old_date:
                print(f_abs)
                files_deleted.append(file)
    if len(files_deleted) > 0:
        print(f"Deleted {files_deleted}")
    return files_deleted


def main():
    for folder in [os.path.join(os.getcwd(), f) for f in ['Results', 'Logs']]:
        try:
            delete_old_files(folder)
        except Exception as e:
            logger.error(e, exc_info=sys.exc_info())
            error_email(str(e))
            logger.info('-' * 100)


if __name__ == '__main__':
    main()
