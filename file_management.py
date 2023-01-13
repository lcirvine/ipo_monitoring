import os
import sys
from datetime import datetime, timedelta
from logging_ipo_dates import logger


def delete_old_files(folder: str, num_days: int = 30, test: bool = False) -> list:
    """
    Deletes files older than the number of days given as a parameter. Defaults to delete files more than 30 days old

    :param folder: folder location files will be deleted from
    :param num_days: int specifying the number of days before a file is deleted
    :param test: if function is being tested, it will only print the file names rather than deleting them
    :return: list of files that were deleted
    """
    old_date = datetime.utcnow() - timedelta(days=num_days)
    files_deleted = []
    for root, dirs, files in os.walk(folder):
        for file in files:
            f_abs = os.path.join(root, file)
            f_modified = datetime.fromtimestamp(os.path.getmtime(f_abs))
            if f_modified <= old_date:
                if test:
                    print(f_abs)
                else:
                    os.unlink(f_abs)
                files_deleted.append(file)
    if len(files_deleted) > 0:
        if test:
            print(f"Deleted {', '.join(files_deleted)}")
        else:
            logger.info(f"Deleted {', '.join(files_deleted)}")
    return files_deleted


def main():
    try:
        for folder in [os.path.join(os.getcwd(), 'Reference', 'Entity Mapping Requests'),
                       os.path.join(os.getcwd(), 'Logs', 'Screenshots'),
                       os.path.join(os.getcwd(), 'Logs', 'Concordance API Responses')]:
            delete_old_files(folder)
    except Exception as e:
        logger.error(e, exc_info=sys.exc_info())


if __name__ == '__main__':
    main()
