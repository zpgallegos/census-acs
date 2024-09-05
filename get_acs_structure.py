"""
get filesystem layout of the ACS FTP server 
used to inform what files are available for download and their locations
"""

import os
import time
import pickle
import logging

from ftplib import FTP

HOST = "ftp2.census.gov"
ROOT = "/programs-surveys/acs"
DATA_FILE = "acs_structure.pkl"
RETRIES = 10
DELAY = 5


def login() -> FTP:
    ftp = FTP(HOST)
    ftp.login()
    return ftp


def is_directory(ftp: FTP, path: str) -> bool:
    try:
        ftp.cwd(path)
        ftp.cwd("..")
        return True
    except Exception as e:
        return False


def walk(ftp: FTP, path: str, data: dict):
    """
    replicate behavior of os.walk for FTP connections
    allows checkpointing in case the connection expires

    :type ftp: ftplib.FTP connection
    :type path: str, path to walk
    :rtype: generator, (root, dirs, files)
    """

    if data and path in data:
        dirs, _ = data[path]
        for subdir in dirs:
            yield from walk(ftp, f"{path}/{subdir}", data)

    else:
        dirs, files = [], []

        ftp.cwd(path)
        items = ftp.nlst()

        for item in items:
            if is_directory(ftp, item):
                dirs.append(item)
            else:
                files.append(item)

        yield path, dirs, files

        for subdir in dirs:
            yield from walk(ftp, f"{path}/{subdir}", data)


def collect(ftp: FTP, data: dict) -> None:
    for root, dirs, files in walk(ftp, ROOT, data):
        data[root] = (dirs, files)


def write(data: dict) -> None:
    with open(DATA_FILE, "wb") as f:
        pickle.dump(data, f)


if __name__ == "__main__":

    logging.basicConfig(
        filename=__file__.replace(".py", ".log"),
        format="%(asctime)s - %(levelname)s: %(message)s",
        level=logging.INFO,
        filemode="a",
    )

    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "rb") as f:
            data = pickle.load(f)
    else:
        data = {}

    ftp = login()

    retries = 0
    while retries <= RETRIES:
        try:
            collect(ftp, data)
            break
        except Exception as e:
            logging.exception(e)
            time.sleep(DELAY * retries)
            ftp = login()
            retries += 1

    if retries > RETRIES:
        logging.error("max retries exceeded, data may be incomplete")
    else:
        logging.info("completed successfully")

    write(data)
