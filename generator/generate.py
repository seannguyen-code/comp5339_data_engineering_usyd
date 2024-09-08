from random import randrange, choice
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values
import csv
import declxml as xml
import os
import random
import glob
from assets import PRODUCTS, ALL_DAYS, CHANNELS, get_channel_distribution, FIRST_NAMES, LAST_NAMES, CSV_RESELLERS, XML_RESELLERS, RESELLERS_TRANSACTIONS, random_date

CONNECTION = psycopg2.connect(user=os.environ["POSTGRES_USER"],
                                password=os.environ["POSTGRES_PASSWORD"],
                                host="oltp",
                                port="5432",
                                database="sales_oltp")

HOLIDAYS = [
    datetime(2023, 1, 1),  # New Year's Day
    datetime(2023, 12, 25),  # Christmas
    datetime(2023, 11, 23),  # Thanksgiving
    datetime(2023, 7, 4),  # Independence Day
    datetime(2024, 1, 1),  # New Year's Day
    datetime(2024, 5, 27)  # Memorial Day
]


def random_date(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return start + timedelta(seconds=random_second)


def generate_oltp(n=1000000):
    print('Generating transactions')

    trans = []

    for i in range(n):
        product = choice(PRODUCTS)

        # Increase the likelihood of transaction dates around holidays
        if random.random() < 0.3:  # 30% chance to select a holiday or nearby date
            holiday = choice(HOLIDAYS)
            # Add some randomness around the holiday dates within 3 days before or after
            bought = holiday + timedelta(days=random.randint(-3, 3))
        else:
            bought = random_date(datetime(2023, 1, 1), datetime(2024, 6, 1))

        boughtdate = str(bought)

        qty = randrange(1, 10)

        # Adjust channel selection to favor 'mobile app' channel
        if random.random() < 0.4:  # 50% chance to select 'mobile app' channel
            channel_id = 'CH3'
        else:
            channel_id = f'CH{choice([i["channel_id"] for i in CHANNELS if i["channel_id"] != 3])}'

        transaction = {
            'customer_id': f'A{randrange(1, 100000)}',
            'product_id': f'PA{product["product_id"]}',
            'amount': product['price'] * qty,
            'qty': qty,
            'channel_id': channel_id,
            'bought_date': boughtdate
        }

        trans.append(transaction)
    return trans


def insert_oltp_transactions():
    print('Inserting transactions')

    trans = generate_oltp()

    columns = trans[0].keys()

    with CONNECTION as conn:
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS transactions")
        cur.execute("CREATE TABLE transactions(transaction_id serial primary key, customer_id varchar, product_id varchar, amount money, qty int, channel_id varchar, bought_date date)")

        query = "INSERT INTO transactions({}) VALUES %s".format(','.join(columns))

        # convert projects values to sequence of sequences
        values = [[value for value in tran.values()] for tran in trans]

        execute_values(cur, query, values)

        conn.commit()


def insert_oltp_resellers():
    print('Inserting resellers')

    columns = RESELLERS_TRANSACTIONS[0].keys()

    with CONNECTION as conn:
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS resellers")
        cur.execute("CREATE TABLE resellers(reseller_id int, reseller_name VARCHAR(255), commission_pct decimal)")

        query = "INSERT INTO resellers({}) VALUES %s".format(','.join(columns))

        # convert projects values to sequence of sequences
        values = [[value for value in tran.values()] for tran in RESELLERS_TRANSACTIONS]

        execute_values(cur, query, values)

        conn.commit()


def insert_oltp_channels():
    print('Inserting channels')

    columns = CHANNELS[0].keys()

    with CONNECTION as conn:
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS channels")
        cur.execute("CREATE TABLE channels(channel_id varchar, channel_name VARCHAR(255))")

        query = "INSERT INTO channels({}) VALUES %s".format(','.join(columns))

        # convert projects values to sequence of sequences
        values = [[f'CH{value}' if key == 'channel_id' else value for key, value in tran.items()] for tran in CHANNELS]

        execute_values(cur, query, values)

        conn.commit()


def insert_oltp_customers():
    print('Inserting customers')

    trans = []
    for i in range(100000):
        first_name = choice(FIRST_NAMES)
        last_name = choice(LAST_NAMES)
        trans.append({'customer_id': f'A{i}', 'first_name': first_name, 'last_name': last_name, 'email': f'{first_name}.{last_name}@example.com'})

    columns = trans[0].keys()

    with CONNECTION as conn:
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS customers")
        cur.execute("CREATE TABLE customers(customer_id varchar, first_name VARCHAR(255), last_name VARCHAR(255), email VARCHAR(255))")

        query = "INSERT INTO customers({}) VALUES %s".format(','.join(columns))

        values = [[value for value in tran.values()] for tran in trans]

        execute_values(cur, query, values)

        conn.commit()


def insert_oltp_products():
    print('Inserting products')

    trans = PRODUCTS
    
    columns = trans[0].keys()

    with CONNECTION as conn:
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS products")
        cur.execute("CREATE TABLE products(product_id varchar, name VARCHAR(255), city VARCHAR(255), price money)")

        query = "INSERT INTO products({}) VALUES %s".format(','.join(columns))

        values = [[f'PA{value}' if key == 'product_id' else value for key, value in tran.items()] for tran in trans]

        execute_values(cur, query, values)

        conn.commit()


def generate_csv(n=50000):
    print('Generating CSV data')
    export = []

    for i in range(n):
        product = choice(PRODUCTS)
        qty = randrange(1, 7)
        boughtdate = str(random_date(datetime(2023, 1, 1), datetime(2024, 6, 1)))
        first_name = choice(FIRST_NAMES)
        last_name = choice(LAST_NAMES)

        transaction = {
            'Product name': product['name'],
            'Product ID': f'PA{product["product_id"]}',  # Ensure product_id starts with 'PA'
            'Quantity': qty,
            'Total amount': qty * product['price'],
            'Sales Channel': f'CH{choice(get_channel_distribution("reseller"))}',  # Ensure channel_id starts with 'CH'
            'Customer First Name': first_name,
            'Customer Last Name': last_name,
            'Customer Email': f'{first_name}.{last_name}@example.com',
            'Series City': product['city'],
            'Created Date': boughtdate
        }

        export.append(transaction)
    return export


def insert_csv():
    print('Inserting CSV data')

    for resellerid in CSV_RESELLERS:

        export = generate_csv()

        keys = ['Transaction ID'] + list(export[0].keys())

        tran_id = 0

        for day in ALL_DAYS:

            data = [tran for tran in export if tran['Created Date'] == day]

            for entry in data:
                entry['Transaction ID'] = f'TC{tran_id}'  # Ensure Transaction ID starts with 'A'
                tran_id += 1

            date_nameformat = day.split('-')
            new_format = date_nameformat[0] + date_nameformat[2] + date_nameformat[1]

            with open(f"/home/generator/csv/DailySales_{new_format}_{resellerid}.csv", 'w', newline='') as output_file:
                dict_writer = csv.DictWriter(output_file, keys)
                dict_writer.writeheader()
                dict_writer.writerows(data)


def generate_xml(resellerid, n=50000):
    print('Generating XML data')
    export = []

    for i in range(n):
        product = choice(PRODUCTS)
        qty = randrange(1, 10)
        bought = random_date(datetime(2023, 1, 1), datetime(2024, 6, 1))
        boughtdate = str(bought).replace('-', '')
        first_name = choice(FIRST_NAMES)
        last_name = choice(LAST_NAMES)

        transaction = {
            'date': boughtdate,
            'reseller-id': resellerid,
            'productName': product['name'],
            'productID': f'PA{product["product_id"]}',  # Ensure product_id starts with 'PA'
            'qty': qty,
            'totalAmount': qty * product['price'] * 1.0,
            'salesChannel': f'CH{choice(get_channel_distribution("reseller"))}',  # Ensure channel_id starts with 'CH'
            'customer': {'firstname': first_name, 'lastname': last_name, 'email': f'{first_name}.{last_name}@example.com'},
            'dateCreated': boughtdate,
            'seriesCity': product['city'],
            'Created Date': str(bought)
        }

        export.append(transaction)
    return export


def insert_xml():
    print('Inserting XML data')

    transaction_processor = xml.dictionary('transaction', [
        xml.string('.', attribute='date'),
        xml.integer('.', attribute='reseller-id'),
        xml.string('transactionId'),  # Ensure Transaction ID is a string
        xml.string('productName'),
        xml.string('productID'),  # Ensure product_id starts with 'PA'
        xml.integer('qty'),
        xml.floating_point('totalAmount'),
        xml.string('salesChannel'),  # Ensure channel_id starts with 'CH'
        xml.dictionary('customer', [xml.string('firstname'), xml.string('lastname'), xml.string('email')]),
        xml.string('dateCreated'),
        xml.string('seriesCity')
    ])

    for resellerid in XML_RESELLERS:

        tran_id = 0

        export = generate_xml(resellerid)

        for day in ALL_DAYS:

            data = [tran for tran in export if tran['Created Date'] == day]

            for entry in data:
                entry['transactionId'] = f'TX{tran_id}'  # Ensure Transaction ID starts with 'A'
                tran_id += 1

            result = []

            result.append('<?xml version="1.0" encoding="utf-8"?>')
            result.append('<transactions>')
            for tran in data:
                xml_str = xml.serialize_to_string(transaction_processor, tran, indent='    ')
                splitted = xml_str.split('\n')
                result += splitted[1:]

            result.append('</transactions>')

            date_nameformat = day.split('-')
            new_format = date_nameformat[0] + date_nameformat[2] + date_nameformat[1]

            with open(f"/home/generator/xml/rawDailySales_{new_format}_{resellerid}.xml", 'w') as output_file:
                output_file.write('\n'.join(result))


def cleanup(directory, ext):
    filelist = [f for f in os.listdir(directory) if f.endswith(ext)]
    for f in filelist:
        os.remove(os.path.join(directory, f))


cleanup('/home/generator/xml', 'xml')
cleanup('/home/generator/csv', 'csv')
insert_oltp_channels()
insert_oltp_customers()
insert_oltp_products()
insert_oltp_resellers()
insert_oltp_transactions()
insert_csv()
insert_xml()
