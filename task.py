import os
import shutil
import configparser
import random
import time
from concurrent.futures import (ThreadPoolExecutor,
                                as_completed)

import requests

config = configparser.ConfigParser()
config.read("Img2Load.ini")
animals = config["Load"]

directory = None

class Task:

    @staticmethod
    def check_dir(path: str = "Downloads") -> tuple[bool, str]:
        time.sleep(random.randint(10, 500)/1000)
        yield os.path.exists(path), path
    
    @staticmethod
    def create_dir(*args) -> str:
        for arg in args:
            for exists, path in arg:
                time.sleep(random.randint(10, 500)/1000)
                if not exists:
                    os.mkdir(path)
                global directory
                directory = path
                yield path

    @staticmethod
    def check_file(*args) -> tuple[str, bool]:
        time.sleep(random.randint(10, 500)/1000)
        for path in args[0]:
            for img in animals:
                time.sleep(random.randint(10, 500)/1000)
                yield img, os.path.isfile(os.path.join(path, img))

    @staticmethod
    def check_responce(*args) -> tuple[str, requests.Response]:
        def loader(url):
            res = requests.get(url, stream=True)
            return res

        args = args[0]
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(loader, animals[img[0]]) : img[0]
                       for img in args if img[1] == False}
            for f in as_completed(futures):
                img = futures[f]
                res = f.result()
                yield img, res.status_code

    @staticmethod
    def upload_image(*args):
        def loader(name, url):
            try:
                res = requests.get(url, stream=True)
                with open(os.path.join(directory, name),'wb') as f:
                    shutil.copyfileobj(res.raw, f)
                return res
            except requests.exceptions.RequestException:
                print(404) 
                exit("Connection failed.")

        args = args[0]
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(loader, img[0], animals[img[0]]) : img[0]
                       for img in args if img[1] == 200}

            for f in as_completed(futures):
                img = futures[f]
                res = f.result()
                yield img, res.status_code




