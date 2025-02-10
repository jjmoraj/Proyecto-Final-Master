import psycopg2
import psycopg2.extras      
import random
from typing import Callable, Dict
import time
import logging
from datetime import datetime

#----------- LOGGER CONFIGURATION -----------
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler('log.log', encoding='utf-8')
file_handler.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

#----------- Connect to any database using the configuration dictionary -----------
def connect_to_database(db_config: Dict) -> psycopg2.extensions.connection:
    try:
        return psycopg2.connect(**db_config)
    except Exception as error:
        logger.error("Error connecting to %s database: %s", db_config['database'], error)
        raise


#---------------- ONLINE STORE FAKER DATA ----------------------
# ----------- Checks if a customer exists in the database -----------
def check_customer(customer_id: str, online_store_cursor: Callable) -> Dict:
    try:
        online_store_cursor.execute("SELECT * FROM customers WHERE customer_id = %s", (customer_id,))
        customer = online_store_cursor.fetchone()
        logger.info('A customer has been checked')
        return customer if customer else {}
    except Exception as error:
        logger.error("Error checking an existing customer: %s", error)
        return {}
    
# ----------- Updates the customer's number of previous purchases -----------
def update_customer_previous_purchases(customer_id: str, previous_purchases: int, online_store_cursor: Callable, online_store_connection: Callable):
    try:
        online_store_cursor.execute("""
            UPDATE customers
            SET previous_purchases = %s
            WHERE customer_id = %s
        """, (previous_purchases, customer_id))
        online_store_connection.commit()
        logger.info('A customer has been updated')
    except Exception as error:
        logger.logger("Error updating customer previous purchases: %s", error)


# ----------- Inserts a new customer in the database -----------
def insert_new_customer(customer_id: str, age: int, gender: str, previous_purchases: int, online_store_cursor: Callable, online_store_connection: Callable):
    try:
        online_store_cursor.execute("""
            INSERT INTO customers
            (customer_id, age, gender, previous_purchases)
            VALUES (%s, %s, %s, %s)
        """, (customer_id, age, gender, previous_purchases))
        online_store_connection.commit()
        logger.info('A new customer has been inserted')

    except Exception as error:
        logger.error("Error inserting new customer: %s", error)

# ----------- Generates the data of a new online purchase order -----------
def generate_online_order_data(customer_id: str, online_store_cursor: Callable) -> Dict:
    customer = check_customer(customer_id, online_store_cursor)
    previous_purchases = customer.get('previous_purchases', 0) + 1 if customer else 0

    customer_data = {
        'customer_id': customer_id,
        'age': random.randint(18,69),
        'gender': random.choice(['Male', 'Female']),
        'previous_purchases': previous_purchases,

    }
    order_data = {
        'customer_id': customer_id,
        'item_purchased': random.choice(['Blouse', 'Sweater', 'Jeans', 'Sandals', 'Sneakers', 'Shirt', 'Shorts', 'Coat',
                                               'Handbag', 'Shoes', 'Dress', 'Skirt', 'Sunglasses', 'Pants', 'Jacket', 'Hoodie',
                                               'Jewelry', 'T-shirt', 'Scarf', 'Hat', 'Socks', 'Backpack', 'Belt', 'Boots','Gloves']),
        'category': random.choice(['Clothing', 'Footwear', 'Outerwear', 'Accessories']),
        'purchase_amount_usd': random.randint(10,100),
        'location': random.choice(['Kentucky', 'Maine', 'Massachusetts', 'Rhode Island', 'Oregon', 'Wyoming',
                                          'Montana', 'Louisiana', 'West Virginia', 'Missouri', 'Arkansas', 'Hawaii',
                                          'Delaware', 'New Hampshire', 'New York', 'Alabama', 'Mississippi',
                                          'North Carolina', 'California', 'Oklahoma', 'Florida', 'Texas', 'Nevada',
                                          'Kansas', 'Colorado', 'North Dakota', 'Illinois', 'Indiana', 'Arizona',
                                          'Alaska', 'Tennessee', 'Ohio', 'New Jersey', 'Maryland', 'Vermont',
                                          'New Mexico', 'South Carolina', 'Idaho', 'Pennsylvania', 'Connecticut', 'Utah',
                                          'Virginia', 'Georgia', 'Nebraska', 'Iowa', 'South Dakota', 'Minnesota',
                                          'Washington', 'Wisconsin', 'Michigan']),
        'size': random.choice(['S', 'M', 'L', 'XL']),
        'color': random.choice(['Gray', 'Maroon', 'Turquoise', 'White', 'Charcoal', 'Silver', 'Pink', 'Purple',
                                      'Olive', 'Gold', 'Violet', 'Teal', 'Lavender', 'Black','Green', 'Peach', 'Red',
                                      'Cyan', 'Brown', 'Beige', 'Orange', 'Indigo', 'Yellow', 'Magenta', 'Blue']),
        'season': random.choice(['Spring', 'Summer', 'Fall', 'Winter']),
        'review_rating': round(random.uniform(1, 5), 1),
        'shipping_type': random.choice(['Express', 'Free Shipping', 'Next Day Air', 'Standard', '2-Day Shipping', 'Store Pickup']),
        'discount_applied': random.choice(['Yes', 'No']),
        'promo_code_used': random.choice(['Yes', 'No']),
        'payment_method': random.choice(['Venmo', 'Cash', 'Credit Card', 'PayPal', 'Bank Transfer', 'Debit Card']),
        'created_at':datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    }
    return order_data, customer_data, customer

# ----------- Inserts the online order data into the database -----------
def insert_online_order_data(order_data: Dict, online_store_cursor: Callable, online_store_connection: Callable):
    query = """
        INSERT INTO online_orders 
        (customer_id, item_purchased, category, purchase_amount_usd, location, size, color, season, review_rating, 
        shipping_type, discount_applied, promo_code_used, payment_method, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    online_store_cursor.execute(query, tuple(order_data.values()))
    online_store_connection.commit()
    logger.info('A online order has been inserted')


# ----------- Handles customer upgrade or insertion case -----------
def handle_customer(customer: Dict, order_data: Dict, customer_data: Dict, online_store_cursor: Callable, online_store_connection: Callable):
    if customer:
        insert_online_order_data(order_data, online_store_cursor, online_store_connection)
        update_customer_previous_purchases(customer['customer_id'], customer_data['previous_purchases'], online_store_cursor, online_store_connection)
    else:
        insert_new_customer(customer_data['customer_id'], customer_data['age'], customer_data['gender'], customer_data['previous_purchases'], online_store_cursor, online_store_connection)
        insert_online_order_data(order_data, online_store_cursor, online_store_connection)


# ----------- Generates and handles insertion of false data for online orders -----------
def insert_fake_data_for_online_orders(online_store_cursor: Callable, online_store_connection: Callable):
    customer_id = random.randint(1,3900) 
    order_data,customer_data,customer = generate_online_order_data(customer_id, online_store_cursor)
    handle_customer(customer, order_data, customer_data, online_store_cursor, online_store_connection)



#---------------- PHYSICAL STORE FAKER DATA ----------------------
# ----------- Generates the data of a new physical purchase order -----------
def generate_physical_order_data() -> Dict:

    order_data = {
        'item_purchased': random.choice(['Blouse', 'Sweater', 'Jeans', 'Sandals', 'Sneakers', 'Shirt', 'Shorts', 'Coat',
                                         'Handbag', 'Shoes', 'Dress', 'Skirt', 'Sunglasses', 'Pants', 'Jacket', 'Hoodie',
                                         'Jewelry', 'T-shirt', 'Scarf', 'Hat', 'Socks', 'Backpack', 'Belt', 'Boots','Gloves']),
        'category': random.choice(['Clothing', 'Footwear', 'Outerwear', 'Accessories']),
        'purchase_amount_usd': random.randint(10,100),
        'location': 'Tennessee',
        'size': random.choice(['S', 'M', 'L', 'XL']),
        'color': random.choice(['Gray', 'Maroon', 'Turquoise', 'White', 'Charcoal', 'Silver', 'Pink', 'Purple',
                                'Olive', 'Gold', 'Violet', 'Teal', 'Lavender', 'Black','Green', 'Peach', 'Red',
                                'Cyan', 'Brown', 'Beige', 'Orange', 'Indigo', 'Yellow', 'Magenta', 'Blue']),
        'season': random.choice(['Spring', 'Summer', 'Fall', 'Winter']),
        'discount_applied': random.choice(['Yes', 'No']),
        'promo_code_used': random.choice(['Yes', 'No']),
        'payment_method': random.choice(['Venmo', 'Cash', 'Credit Card', 'PayPal', 'Bank Transfer', 'Debit Card']),
        'created_at':datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    return order_data

# ----------- Inserts the physical order data into the database -----------
def insert_physical_order_data(order_data: Dict, physical_store_cursor: Callable, physical_store_connection: Callable):
    query = """
        INSERT INTO physical_orders 
        (item_purchased, category, purchase_amount_usd, location, size, color, season, discount_applied, promo_code_used, payment_method, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    physical_store_cursor.execute(query, tuple(order_data.values()))
    physical_store_connection.commit()
    logger.info('A new physical order has been inserted')


# ----------- Generates and handles insertion of false data for physical orders -----------
def insert_fake_data_for_physical_orders(physical_store_cursor: Callable, physical_store_connection: Callable):
    order_data = generate_physical_order_data()
    insert_physical_order_data(order_data, physical_store_cursor, physical_store_connection)

# ---------------- Main Application ---------------- 
if __name__ == "__main__":
    try:
        online_store_config = {
            'host': "db_online_store", 
            'port': "5432",
            'database': "online_store",
            'user': "admin",
            'password': "adminadmin"
        }
        online_store_connection = connect_to_database(online_store_config)
        online_store_cursor = online_store_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        logger.info('Connected to Online Store Database')

        physical_store_config = {
            'host': "db_physical_store", 
            'port': "5432",
            'database': "physical_store",
            'user': "admin",
            'password': "adminadmin"
        }
        physical_store_connection = connect_to_database(physical_store_config)
        physical_store_cursor = physical_store_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        logger.info('Connected to Physical Store Database')

        while True:
            insert_fake_data_for_online_orders(online_store_cursor, online_store_connection)
            time.sleep(2)

            insert_fake_data_for_physical_orders(physical_store_cursor, physical_store_connection)
            time.sleep(2)

    except Exception as error:
        logger.error("An error occurred: %s", error)
        online_store_cursor.close() 
        online_store_connection.close()       
        physical_store_cursor.close()
        physical_store_connection.close()
