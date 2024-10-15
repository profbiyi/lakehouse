import psycopg2
from psycopg2 import sql
from faker import Faker
import random
from tqdm import tqdm

# PostgreSQL connection details
DB_HOST = 'cbdhrtd93854d5.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com'
DB_PORT = '5432'
DB_NAME = 'dqocasc3m52oc'
DB_USER = 'u603tpuoo2a5l4'
DB_PASSWORD = 'p9d8f7be88fb7001d4ee7acf1a1fd569a316c9bef8ee77036a1e80a9fbd443aaf'
DB_SCHEMA = 'sample_schema'

# Connect to PostgreSQL
conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
cur = conn.cursor()


# Set search path to include the schema
cur.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(DB_SCHEMA)))

# Initialize Faker
fake = Faker()

# Number of records to generate
num_customers = 400
num_orders = 800
num_products = 200
num_order_items = 2000

# Track customer IDs, product IDs, and order IDs
customer_ids = []
product_ids = []
order_ids = []

# Insert customers
print("Inserting customers...")
emails = set()
for _ in tqdm(range(num_customers), desc="Customers"):
    name = fake.name()
    email = fake.email()
    while email in emails:
        email = fake.email()
    emails.add(email)
    cur.execute(
        sql.SQL("INSERT INTO customers (name, email, created_at, updated_at) VALUES (%s, %s, %s, %s) RETURNING customer_id"),
        [name, email, fake.date_time_this_decade(), fake.date_time_this_decade()]
    )
    customer_id = cur.fetchone()[0]
    customer_ids.append(customer_id)

# Insert products
print("Inserting products...")
for _ in tqdm(range(num_products), desc="Products"):
    name = fake.word().capitalize()
    description = fake.text(max_nb_chars=200)
    price = round(random.uniform(10.0, 100.0), 2)
    cur.execute(
        sql.SQL("INSERT INTO products (name, description, price, created_at, updated_at) VALUES (%s, %s, %s, %s, %s) RETURNING product_id"),
        [name, description, price, fake.date_time_this_decade(), fake.date_time_this_decade()]
    )
    product_id = cur.fetchone()[0]
    product_ids.append(product_id)

# Insert orders
print("Inserting orders...")
for _ in tqdm(range(num_orders), desc="Orders"):
    customer_id = random.choice(customer_ids)
    order_date = fake.date_time_this_decade()
    amount = round(random.uniform(20.0, 500.0), 2)
    cur.execute(
        sql.SQL("INSERT INTO orders (customer_id, order_date, amount, created_at, updated_at) VALUES (%s, %s, %s, %s, %s) RETURNING order_id"),
        [customer_id, order_date, amount, fake.date_time_this_decade(), fake.date_time_this_decade()]
    )
    order_id = cur.fetchone()[0]
    order_ids.append(order_id)

# Insert order items
print("Inserting order items...")
for _ in tqdm(range(num_order_items), desc="Order Items"):
    order_id = random.choice(order_ids)
    product_id = random.choice(product_ids)
    quantity = random.randint(1, 10)
    cur.execute(
        sql.SQL("INSERT INTO order_items (order_id, product_id, quantity, created_at, updated_at) VALUES (%s, %s, %s, %s, %s)"),
        [order_id, product_id, quantity, fake.date_time_this_decade(), fake.date_time_this_decade()]
    )

# Commit the transactions and close the connection
conn.commit()
cur.close()
conn.close()

print("Data insertion complete.")