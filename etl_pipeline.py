from __future__ import annotations

from pathlib import Path
import duckdb
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / 'data' / 'raw'
PROCESSED_DIR = BASE_DIR / 'data' / 'processed'
DB_PATH = BASE_DIR / 'ecommerce_analytics.duckdb'



def extract() -> dict[str, pd.DataFrame]:
    return {
        'customers': pd.read_csv(RAW_DIR / 'customers.csv'),
        'products': pd.read_csv(RAW_DIR / 'products.csv'),
        'orders': pd.read_csv(RAW_DIR / 'orders.csv'),
        'order_items': pd.read_csv(RAW_DIR / 'order_items.csv'),
    }



def transform(data: dict[str, pd.DataFrame]) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    customers = data['customers'].copy()
    products = data['products'].copy()
    orders = data['orders'].copy()
    order_items = data['order_items'].copy()

    customers['customer_id'] = customers['customer_id'].astype('int64')
    customers['customer_name'] = customers['customer_name'].astype(str).str.strip()
    customers['city'] = customers['city'].fillna('Cidade não informada')
    customers['state'] = customers['state'].fillna('NI')
    customers['signup_date'] = pd.to_datetime(customers['signup_date'])

    products['product_id'] = products['product_id'].astype('int64')
    products['product_name'] = products['product_name'].astype(str).str.strip()
    products['category'] = products['category'].fillna('Sem categoria')
    products['price'] = pd.to_numeric(products['price'], errors='coerce')
    products['price'] = products['price'].fillna(products['price'].median()).round(2)

    orders['order_id'] = orders['order_id'].astype('int64')
    orders['customer_id'] = orders['customer_id'].astype('int64')
    orders['order_date'] = pd.to_datetime(orders['order_date'])
    orders['status'] = orders['status'].fillna('unknown').astype(str)

    order_items['order_item_id'] = order_items['order_item_id'].astype('int64')
    order_items['order_id'] = order_items['order_id'].astype('int64')
    order_items['product_id'] = order_items['product_id'].astype('int64')
    order_items['quantity'] = pd.to_numeric(order_items['quantity'], errors='coerce').fillna(1).astype('int64')
    order_items['unit_price'] = pd.to_numeric(order_items['unit_price'], errors='coerce')
    order_items = order_items.merge(
        products[['product_id', 'price']],
        on='product_id',
        how='left'
    )
    order_items['unit_price'] = order_items['unit_price'].fillna(order_items['price']).round(2)
    order_items.drop(columns=['price'], inplace=True)
    order_items['item_total'] = (order_items['quantity'] * order_items['unit_price']).round(2)

    consolidated = (
        order_items
        .merge(orders, on='order_id', how='left')
        .merge(customers, on='customer_id', how='left')
        .merge(products, on='product_id', how='left', suffixes=('', '_product'))
    )

    consolidated['order_month'] = consolidated['order_date'].dt.to_period('M').astype(str)
    consolidated['is_valid_sale'] = ~consolidated['status'].isin(['canceled', 'unknown'])

    transformed = {
        'customers': customers,
        'products': products,
        'orders': orders,
        'order_items': order_items,
    }
    return transformed, consolidated



def load(raw: dict[str, pd.DataFrame], transformed: dict[str, pd.DataFrame], consolidated: pd.DataFrame) -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))

    con.register('raw_orders_df', raw['orders'])
    con.register('treated_order_items_df', transformed['order_items'])
    con.register('sales_analytics_df', consolidated)

    con.execute('DROP TABLE IF EXISTS raw_orders')
    con.execute('DROP TABLE IF EXISTS treated_order_items')
    con.execute('DROP TABLE IF EXISTS sales_analytics')

    con.execute('CREATE TABLE raw_orders AS SELECT * FROM raw_orders_df')
    con.execute('CREATE TABLE treated_order_items AS SELECT * FROM treated_order_items_df')
    con.execute('CREATE TABLE sales_analytics AS SELECT * FROM sales_analytics_df')

    consolidated.to_csv(PROCESSED_DIR / 'sales_analytics.csv', index=False)
    con.close()



def run_analytics() -> dict[str, pd.DataFrame]:
    con = duckdb.connect(str(DB_PATH))

    queries = {
        'faturamento_total_por_mes': """
            SELECT
                order_month,
                ROUND(SUM(item_total), 2) AS faturamento_total
            FROM sales_analytics
            WHERE is_valid_sale = TRUE
            GROUP BY order_month
            ORDER BY order_month
        """,
        'faturamento_por_categoria': """
            SELECT
                category,
                ROUND(SUM(item_total), 2) AS faturamento_total
            FROM sales_analytics
            WHERE is_valid_sale = TRUE
            GROUP BY category
            ORDER BY faturamento_total DESC
        """,
        'quantidade_pedidos_por_estado': """
            SELECT
                state,
                COUNT(DISTINCT order_id) AS quantidade_pedidos
            FROM sales_analytics
            GROUP BY state
            ORDER BY quantidade_pedidos DESC
        """,
        'ticket_medio_por_cliente': """
            WITH orders_total AS (
                SELECT
                    customer_id,
                    customer_name,
                    order_id,
                    SUM(item_total) AS order_total
                FROM sales_analytics
                WHERE is_valid_sale = TRUE
                GROUP BY customer_id, customer_name, order_id
            )
            SELECT
                customer_id,
                customer_name,
                ROUND(AVG(order_total), 2) AS ticket_medio
            FROM orders_total
            GROUP BY customer_id, customer_name
            ORDER BY ticket_medio DESC
            LIMIT 20
        """,
        'top_10_produtos_mais_vendidos': """
            SELECT
                product_id,
                product_name,
                SUM(quantity) AS quantidade_vendida,
                ROUND(SUM(item_total), 2) AS faturamento_total
            FROM sales_analytics
            WHERE is_valid_sale = TRUE
            GROUP BY product_id, product_name
            ORDER BY quantidade_vendida DESC, faturamento_total DESC
            LIMIT 10
        """,
    }

    results = {name: con.execute(sql).df() for name, sql in queries.items()}
    con.close()
    return results



def main() -> None:
    raw = extract()
    transformed, consolidated = transform(raw)
    load(raw, transformed, consolidated)

    print('Pipeline ETL executado com sucesso.')
    print(f'Banco criado em: {DB_PATH}')
    print(f'Tabela analítica consolidada com {len(consolidated)} registros.')

    analytics = run_analytics()
    for name, df in analytics.items():
        print(f'\n=== {name.upper()} ===')
        print(df.head(10).to_string(index=False))


if __name__ == '__main__':
    main()
