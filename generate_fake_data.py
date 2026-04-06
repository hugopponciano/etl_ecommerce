from __future__ import annotations

from pathlib import Path
import random
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker


RAW_DIR = Path(__file__).resolve().parent / 'data' / 'raw'
FAKER_LOCALE = 'pt_BR'
SEED = 42

N_CUSTOMERS = 3000
N_PRODUCTS = 300
N_ORDERS = 10000
N_ORDER_ITEMS = 20000

CATEGORIES = [
    'Eletrônicos', 'Moda', 'Casa', 'Beleza', 'Esporte',
    'Livros', 'Brinquedos', 'Informática', 'Mercado', 'Pet'
]

ORDER_STATUSES = ['pending', 'paid', 'shipped', 'delivered', 'canceled']

PRODUCT_NAMES = [
    'Smartphone', 'Notebook', 'Teclado', 'Mouse', 'Monitor', 'Camiseta', 'Tênis', 'Jaqueta',
    'Cafeteira', 'Liquidificador', 'Panela', 'Perfume', 'Shampoo', 'Bola', 'Bicicleta',
    'Livro', 'Caderno', 'Boneca', 'Carrinho', 'Ração', 'Coleira', 'Fone', 'Webcam',
    'Mochila', 'Relógio', 'Óculos', 'Luminária', 'Almofada', 'Sabonete', 'Suplemento'
]


def random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))



def generate_customers(fake: Faker) -> pd.DataFrame:
    rows = []
    start = datetime(2021, 1, 1)
    end = datetime(2025, 12, 31)
    for customer_id in range(1, N_CUSTOMERS + 1):
        rows.append({
            'customer_id': customer_id,
            'customer_name': fake.name(),
            'city': fake.city(),
            'state': fake.estado_sigla(),
            'signup_date': random_date(start, end).date().isoformat(),
        })
    return pd.DataFrame(rows)



def generate_products() -> pd.DataFrame:
    rows = []
    for product_id in range(1, N_PRODUCTS + 1):
        base_name = random.choice(PRODUCT_NAMES)
        category = random.choice(CATEGORIES)
        brand_code = random.randint(100, 999)
        price = round(random.uniform(15, 3500), 2)
        rows.append({
            'product_id': product_id,
            'product_name': f'{base_name} {brand_code}',
            'category': category,
            'price': price,
        })
    return pd.DataFrame(rows)



def generate_orders() -> pd.DataFrame:
    rows = []
    start = datetime(2024, 1, 1)
    end = datetime(2025, 12, 31)
    for order_id in range(1, N_ORDERS + 1):
        rows.append({
            'order_id': order_id,
            'customer_id': random.randint(1, N_CUSTOMERS),
            'order_date': random_date(start, end).strftime('%Y-%m-%d %H:%M:%S'),
            'status': random.choices(
                ORDER_STATUSES,
                weights=[0.07, 0.18, 0.18, 0.45, 0.12],
                k=1,
            )[0],
        })
    return pd.DataFrame(rows)



def generate_order_items(products: pd.DataFrame) -> pd.DataFrame:
    product_price_map = products.set_index('product_id')['price'].to_dict()
    rows = []
    for order_item_id in range(1, N_ORDER_ITEMS + 1):
        product_id = random.randint(1, N_PRODUCTS)
        base_price = product_price_map[product_id]
        price_factor = random.uniform(0.9, 1.1)
        rows.append({
            'order_item_id': order_item_id,
            'order_id': random.randint(1, N_ORDERS),
            'product_id': product_id,
            'quantity': random.randint(1, 5),
            'unit_price': round(base_price * price_factor, 2),
        })
    return pd.DataFrame(rows)



def inject_quality_issues(
    customers: pd.DataFrame,
    products: pd.DataFrame,
    orders: pd.DataFrame,
    order_items: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    customers.loc[customers.sample(20, random_state=SEED).index, 'city'] = None
    customers.loc[customers.sample(10, random_state=SEED + 1).index, 'state'] = None

    products.loc[products.sample(8, random_state=SEED + 2).index, 'category'] = None
    products.loc[products.sample(5, random_state=SEED + 3).index, 'price'] = None

    orders.loc[orders.sample(15, random_state=SEED + 4).index, 'status'] = None

    order_items.loc[order_items.sample(25, random_state=SEED + 5).index, 'quantity'] = None
    order_items.loc[order_items.sample(25, random_state=SEED + 6).index, 'unit_price'] = None

    return customers, products, orders, order_items



def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    fake = Faker(FAKER_LOCALE)
    Faker.seed(SEED)
    random.seed(SEED)

    customers = generate_customers(fake)
    products = generate_products()
    orders = generate_orders()
    order_items = generate_order_items(products)

    customers, products, orders, order_items = inject_quality_issues(
        customers, products, orders, order_items
    )

    customers.to_csv(RAW_DIR / 'customers.csv', index=False)
    products.to_csv(RAW_DIR / 'products.csv', index=False)
    orders.to_csv(RAW_DIR / 'orders.csv', index=False)
    order_items.to_csv(RAW_DIR / 'order_items.csv', index=False)

    print('Arquivos gerados com sucesso em data/raw/')
    print(f'customers.csv: {len(customers)} registros')
    print(f'products.csv: {len(products)} registros')
    print(f'orders.csv: {len(orders)} registros')
    print(f'order_items.csv: {len(order_items)} registros')


if __name__ == '__main__':
    main()
