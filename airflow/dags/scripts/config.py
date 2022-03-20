DB_USER="cayena"
DB_DATABASE="cayena"
DB_PASS="cayena"
DB_HOST="postgres-cayena"
DB_PORT=5432

CREATE_CAYENA_SCHEMA = """CREATE SCHEMA IF NOT EXISTS analytics;"""

CREATE_BOOKS_TABLE = """
CREATE TABLE IF NOT EXISTS cayena.analytics.books (
	upc VARCHAR(25) UNIQUE
	, item_type VARCHAR(25)
	, category VARCHAR(50)
	, rate INT
	, price REAL
	, price_excl_tax REAL
	, price_incl_tax REAL
	, tax REAL
	, stock INT
	, reviews INT
	, created TIMESTAMP DEFAULT NOW()
	, updated TIMESTAMP DEFAULT NOW()
    , PRIMARY KEY(upc)
);
"""

INSTRUCTIONS = {
    'movies' : {
		"table_name": "cayena.analytics.books",
		"primary_key": "upc",
        "dtype" : {
            'upc': str,
            'item_type': str,
            'category': str,
            'rate': int,
            'price': float,
            'price_excl_tax': float,
            'price_incl_tax': float,
            'stock': int,
            'reviews': int
        }
	}
}