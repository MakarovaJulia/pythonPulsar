import json
import time
from datetime import datetime

from faker import Faker

from connectors import cart_producer, action_producer, client, order_producer
from data_schemes.action_data_scheme import ActionData
from data_schemes.cart_data_scheme import CartData
from data_schemes.order_data_scheme import OrderData
from helpers import add_random_time_delta, upload_to_hdfs, copy_to2

try:
    count = 0
    customer_id = 0
    cart_id = 0
    order_id = 0
    action_id = 0
    fake = Faker()
    input_date = datetime(2022, 1, 15, 0, 0, 0)
    new_date = add_random_time_delta(input_date, 5)
    time.sleep(1)
    while True:
        status = 0
        fake = Faker()
        step_num = fake.random_int(min=1, max=10)
        time_created = str(new_date)
        for i in range(step_num):
            print(i)
            print(step_num)
            if i == step_num - 1:
                status = 1
            product_id = fake.random_int(min=0, max=300)

            # Отправка сообщений в топик
            message = CartData(cart_id=cart_id,
                               customer_id=customer_id,
                               product_id=product_id,
                               status=bool(status),
                               time_created=time_created)
            cart_producer.send(message)

            # Сохранение сообщений в файл
            msg = {"cart_id": cart_id,
                   "customer_id": customer_id,
                   "product_id": product_id,
                   "status": bool(status),
                   "time_created": time_created}
            str_msg = json.dumps(msg)
            if cart_id % 5000 == 0 and cart_id > 0:
                copy_to2(f"C:/Users/julma/PycharmProjects/pythonPulsar/producers/carts{count}.csv",
                         f"datanode1:/carts{count}.csv")
                upload_to_hdfs(f"carts{count}.csv", f"carts{count}.csv", 'datanode1')
                count = count + 1
            with open(f"carts{count}.csv", 'a') as csvfile:
                csvfile.write(str_msg)
            print(str_msg)
            time.sleep(1)
        if status == 1:
            print("status")
            print(status)
            status_order = fake.random_element(elements=("cancelled", "delivered"))
            promo_used = fake.boolean()

            # Отправка сообщений в топик
            message = OrderData(order_id=order_id,
                                customer_id=customer_id,
                                order_date=time_created,
                                cart_id=cart_id,
                                promo_used=promo_used,
                                status=status_order)
            order_producer.send(message)

            # Сохранение сообщений в файл
            msg_order = {"customer_id": customer_id,
                         "order_date": time_created,
                         "cart_id": cart_id,
                         "promo_used": promo_used,
                         "status": status_order}
            str_msg = json.dumps(msg_order)
            if order_id % 5000 == 0 and order_id > 0:
                copy_to2(f"C:/Users/julma/PycharmProjects/pythonPulsar/producers/orders{count}.csv",
                         f"datanode1:/orders{count}.csv")
                upload_to_hdfs(f"orders{count}.csv", f"orders{count}.csv", 'datanode1')
                count = count + 1
            with open(f"orders{count}.csv", 'a') as csvfile:
                csvfile.write(str_msg)
            print(msg_order)
            if status_order == "cancelled":

                # Отправка сообщений в топик
                message_action = ActionData(action_id=action_id,
                                            customer_id=customer_id,
                                            action_type="Cancel order",
                                            action_time=time_created)
                action_producer.send(message_action)

                # Сохранение сообщений в файл
                msg_action = {"action_id": action_id,
                              "customer_id": customer_id,
                              "action_type": "Cancel order",
                              "action_time": time_created}
                action_id = action_id + 1
                str_msg = json.dumps(msg_action)
                if action_id % 5000 == 0 and action_id > 0:
                    copy_to2(f"C:/Users/julma/PycharmProjects/pythonPulsar/producers/actions{count}.csv",
                             f"datanode1:/actions{count}.csv")
                    upload_to_hdfs(f"actions{count}.csv", f"actions{count}.csv", 'datanode1')
                    count = count + 1
                with open(f"actions{count}.csv", 'a') as csvfile:
                    csvfile.write(str_msg)
                print(str_msg)
            if status_order == "delivered":

                # Отправка сообщений в топик
                message_action = ActionData(action_id=action_id,
                                            customer_id=customer_id,
                                            action_type="Make order",
                                            action_time=time_created)
                action_producer.send(message_action)

                # Сохранение сообщений в файл
                msg_action = {"action_id": action_id,
                              "customer_id": customer_id,
                              "action_type": "Make order",
                              "action_time": time_created}
                action_id = action_id + 1
                str_msg = json.dumps(msg_action)
                if action_id % 5000 == 0 and action_id > 0:
                    copy_to2(f"C:/Users/julma/PycharmProjects/pythonPulsar/producers/actions{count}.csv",
                             f"datanode1:/actions{count}.csv")
                    upload_to_hdfs(f"actions{count}.csv", f"actions{count}.csv", 'datanode1')
                    count = count + 1
                with open(f"actions{count}.csv", 'a') as csvfile:
                    csvfile.write(str_msg)
                print(str_msg)
        cart_id += 1
        chance = fake.random_int(min=1, max=2)
        if chance == 2:
            customer_id += 1
        order_id += 1
        new_date = add_random_time_delta(new_date, 5)
        time.sleep(fake.random_int(min=2, max=10))
except KeyboardInterrupt:
    print("Stop")

finally:
    cart_producer.close()
    client.close()
