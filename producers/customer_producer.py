import json
import time
from datetime import datetime
from random import getrandbits

from faker import Faker

from connectors import customer_producer, client
from data_schemes.customer_data_scheme import CustomerData
from helpers import generate_random_country_name, add_random_time_delta, upload_to_hdfs, copy_to2

try:
    count = 0
    customer_id = 0
    input_date = datetime(2021, 12, 31, 0, 0, 0)
    new_date = add_random_time_delta(input_date, 5)
    while True:
        # Генерация данных
        fake = Faker()
        email = fake.email()
        password = fake.password()
        first_name = fake.first_name()
        last_name = fake.last_name()
        age = fake.random_int(18, 80)
        country = generate_random_country_name()
        subscribed_to_news = bool(getrandbits(1))
        time_created = str(new_date)

        # Отправка сообщений в топик
        message = CustomerData(customer_id=customer_id,
                               email=email,
                               password=password,
                               first_name=first_name,
                               last_name=last_name,
                               age=age,
                               country=country,
                               subscribed_to_news=subscribed_to_news,
                               time_created=time_created)
        customer_producer.send(message)

        # Сохранение сообщений в файл
        msg = {"customer_id": customer_id,
               "email": email,
               "password": password,
               "first_name": first_name,
               "last_name": last_name,
               "age": age,
               "country": country,
               "subscribed_to_news": subscribed_to_news,
               "time_created": time_created}
        str_msg = json.dumps(msg)
        if customer_id % 5000 == 0 and customer_id > 0:
            copy_to2(f"C:/Users/julma/PycharmProjects/pythonPulsar/producers/customers{count}.csv",
                    f"datanode1:/customers{count}.csv")
            upload_to_hdfs(f"customers{count}.csv", f"customers{count}.csv", 'datanode1')
            count = count + 1
        with open(f"customers{count}.csv", 'a') as csvfile:
            csvfile.write(str_msg)
        print(str_msg)
        customer_id += 1
        new_date = add_random_time_delta(new_date, 2)
        if customer_id > 1000:
            time.sleep(600)
        else:
            time.sleep(5)

except KeyboardInterrupt:
    print("Stop")

finally:
    customer_producer.close()
    client.close()
