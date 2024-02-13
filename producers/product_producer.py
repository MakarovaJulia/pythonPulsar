import json
import time
from datetime import datetime
from faker import Faker
from wonderwords import RandomWord
from connectors import product_producer, client
from data_schemes.product_data_scheme import ProductData
from helpers import add_random_time_delta, upload_to_hdfs, copy_to2

try:
    count = 0
    input_date = datetime(2021, 12, 31, 0, 0, 0)
    new_date = add_random_time_delta(input_date, 5)
    product_id = 0
    fake = Faker()
    rw = RandomWord()
    while True:
        name = rw.word(include_parts_of_speech=["nouns"])
        price = fake.random_int(min=10, max=10000)
        rating = fake.random_int(min=1, max=5)
        views_count = fake.random_int(min=10, max=10000)
        product_type = fake.random_element(elements=("electronics",
                                                     "entertainment",
                                                     "books",
                                                     "clothes",
                                                     "kids",
                                                     "accessories"))
        time_created = str(new_date)

        # Отправка сообщений в топик
        message = ProductData(product_id=product_id,
                              name=name,
                              price=price,
                              rating=rating,
                              views_count=views_count,
                              type=product_type,
                              time_created=time_created)
        product_producer.send(message)

        # Сохранение сообщений в файл
        msg_product = {"product_id": product_id,
                       "name": name,
                       "price": price,
                       "rating": rating,
                       "views_count": views_count,
                       "type": product_type,
                       "time_created": time_created}
        str_msg = json.dumps(msg_product)
        if product_id % 10 == 0 and product_id > 0:
            copy_to2(f"C:/Users/julma/PycharmProjects/pythonPulsar/producers/products{count}.csv",
                     f"datanode1:/products{count}.csv")
            upload_to_hdfs(f"products{count}.csv", f"products{count}.csv", 'datanode1')
            count = count + 1
        with open(f"products{count}.csv", 'a') as csvfile:
            csvfile.write(str_msg)
        print(msg_product)
        product_id += 1
        if product_id > 600:
            time.sleep(600)
        else:
            time.sleep(5)
except KeyboardInterrupt:
    print("Stop")

finally:
    product_producer.close()
    client.close()
