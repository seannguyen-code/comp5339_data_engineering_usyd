import itertools
from random import randrange, choice, randint, sample
from datetime import date, timedelta, datetime
import names

sdate = date(2023, 1, 1)   # start date
edate = date(2024, 6, 30)   # end date

delta = edate - sdate       # as timedelta

ALL_DAYS = [str(sdate + timedelta(days=i)) for i in range(delta.days + 1)]

cities = [
    'tokyo',
    'osaka',
    'nagoya',
    'sapporo',
    'fukuoka',
    'kobe',
    'kyoto',
    'sendai',
    'hiroshima',
    'saitama',
    'chiba',
    'kitakyushu',
    'kawasaki',
    'sagamihara',
    'okayama',
    'naha',
    'adelaide',
    'brisbane',
    'canberra',
    'darwin',
    'hobart',
    'melbourne',
    'perth',
    'sydney',
    'seoul',
    'busan',
    'incheon',
    'daegu',
    'dalian',
    'beijing',
    'shanghai',
    'shenzhen',
    'guangzhou',
    'chengdu',
    'wuhan',
    'chongqing',
    'tianjin',
    'hong kong',
    'taipei',
    'kaohsiung',
    'bangkok',
    'jakarta',
    'kuala lumpur',
    'manila',
    'hanoi',
    'ho chi minh city',
    'singapore',
    'mumbai',
    'delhi',
    'bangalore',
    'hyderabad',
    'kolkata',
    'chennai',
    'ahmedabad',
    'pune',
    'surat',
    'karachi',
    'lahore',
    'islamabad',
    'dhaka',
    'yangon',
    'kathmandu',
    'colombo',
    'phnom penh',
    'vientiane',
    'ulaanbaatar'
]

CITIES = [i.title() for i in sample(cities, 20)]

PRODUCT_FORMATS = ['Fresh Produce', 'Dairy', 'Meat', 'Bakery', 'Beverages', 'Frozen Foods', 
                   'Snacks', 'Canned Goods', 'Pasta & Rice', 'Condiments', 'Cereals', 'Personal Care']

PRODUCT_TEXT = ['Apples', 'Milk', 'Chicken Breast', 'Bread', 'Orange Juice', 'Ice Cream', 
                'Potato Chips', 'Canned Tomatoes', 'Spaghetti', 'Ketchup', 'Oatmeal', 'Shampoo', 
                'Bananas', 'Yogurt', 'Ground Beef', 'Croissant', 'Coffee', 'Frozen Pizza', 
                'Chocolate Bar', 'Canned Beans', 'Rice', 'Mustard', 'Corn Flakes', 'Toothpaste', 
                'Lettuce', 'Cheese', 'Pork Chops', 'Donut', 'Soda', 'Frozen Vegetables', 
                'Pretzels', 'Canned Corn', 'Macaroni', 'Mayonnaise', 'Granola', 'Soap']


def get_channel_distribution(channel):
    if channel == 'direct':
        return [*2*('in-store',), *2*('web',), *3*('mobile app',) ]
    elif channel == 'reseller':
        return [*1*('in-store',), *3*('web',), *3*('mobile app',) ]

CHANNELS = [{'channel_name':  'in-store', 'channel_id': 1},
          {'channel_name':  'web', 'channel_id': 2},
          {'channel_name':  'mobile app', 'channel_id': 3}]


def random_date(start=datetime(2023,1,1), end=datetime(2024,6,30)):
    """Generate a random datetime between `start` and `end`"""
    result = start + timedelta(
        # Get a random amount of seconds between `start` and `end`
        seconds=randint(0, int((end - start).total_seconds())),)

    return result.date()

# Match PRODUCT_FORMATS with PRODUCT_TEXT
product_data = [ (PRODUCT_FORMATS[i % len(PRODUCT_FORMATS)], PRODUCT_TEXT[i % len(PRODUCT_TEXT)], city) 
                for i, city in enumerate(CITIES * len(PRODUCT_FORMATS))]

PRODUCTS = []

id = 1
for e in product_data:
    PRODUCTS.append({'name': f'{e[0]} {e[1]} {e[2]}', 'city': e[2], 'price': randrange(15, 45) / 10.0, 'product_id': id })
    id += 1

FIRST_NAMES = [names.get_first_name() for i in range(1000)]
LAST_NAMES = [names.get_last_name() for i in range(1000)]

XML_RESELLERS = [1001, 1002]
CSV_RESELLERS = [1003, 1004]

RESELLERS_TRANSACTIONS = [
    {'reseller_id': 1001, 'reseller_name': 'Wal Grocer', 'commission_pct': 0.10},
    {'reseller_id': 1002, 'reseller_name': 'Wool Everyday', 'commission_pct': 0.17},
    {'reseller_id': 1003, 'reseller_name': 'Costko', 'commission_pct': 0.14},
    {'reseller_id': 1004, 'reseller_name': 'Aldin', 'commission_pct': 0.16}
]