import os
import tarfile
from datetime import timedelta
from random import randint, choice
import pycountry
import docker


def add_random_time_delta(input_date_param, days_max_count):
    random_days = randint(0, days_max_count)
    random_minutes = randint(1, 60)
    random_seconds = randint(1, 60)

    time_delta = timedelta(days=random_days, minutes=random_minutes, seconds=random_seconds)

    result_date = input_date_param + time_delta

    return result_date


def generate_random_country_name():
    countries = list(pycountry.countries)
    country = choice(countries)
    return country.name


# Загрузка файла в контейнер
def copy_to2(src, dst):
    client = docker.from_env()
    name, dst = dst.split(':')
    container = client.containers.get(name)

    os.chdir(os.path.dirname(src))
    srcname = os.path.basename(src)
    tar = tarfile.open(src + '.tar', mode='w')
    try:
        tar.add(srcname)
    finally:
        tar.close()

    data = open(src + '.tar', 'rb').read()
    print(os.path.dirname(dst))
    container.put_archive(os.path.dirname(dst), data)


def upload_to_hdfs(file_path, hdfs_path, container_name):
    client = docker.from_env()

    container_name = container_name

    container = client.containers.get(container_name)

    container.exec_run(f'hdfs dfs -put datanode1:/{file_path} /{hdfs_path}')

    client.close()
