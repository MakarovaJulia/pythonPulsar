from pulsar.schema import JsonSchema

from data_schemes.action_data_scheme import ActionData
from data_schemes.cart_data_scheme import CartData
from data_schemes.customer_data_scheme import CustomerData
from data_schemes.order_data_scheme import OrderData
from data_schemes.product_data_scheme import ProductData

json_customer_schema = JsonSchema(CustomerData)
json_cart_schema = JsonSchema(CartData)
json_action_schema = JsonSchema(ActionData)
json_order_schema = JsonSchema(OrderData)
json_product_schema = JsonSchema(ProductData)