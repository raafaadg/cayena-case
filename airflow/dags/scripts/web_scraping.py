from bs4 import BeautifulSoup
from urllib.request import urlopen
import pandas as pd
import numpy as np
import re
import dill as pickle
import psycopg2
import scripts.config as config
import multiprocessing as mp
from os import path


class WebScraping():
    def __init__(self, query_path):
        self.QUERY = query_path
        self.TABLE_NAME = 'cayena.analytics.books'
        self.TABLE_NAME_TEMP = 'cayena.analytics._books'
        self.PRIMARY_KEY = 'upc'
        self.DIR_PICKLE = 'cayena.pkl'
        self.URL_BASE = "https://books.toscrape.com/catalogue/"
        self.URL_LINKS = self.URL_BASE + "page-%d.html"
        self.DESC = ["Getting books HTML", "Getting books Links", "Getting books informations"]
        self.rate = {
            "one": 1,
            "two": 2,
            "three": 3,
            "four": 4,
            "five": 5
        }
        
    def _parallelize_job(self, df, f):
    
        n_cores = mp.cpu_count() - 1
        df_split = np.array_split(df, n_cores)
        
        pool = mp.Pool(n_cores)
        
        p = pool.map(f, df_split)
        
        df = pd.concat(p)
        pool.close()        
        return df
        
    def _connect(self):
        try:
            self.connection = psycopg2.connect(
                database=config.DB_DATABASE,
                user=config.DB_USER,
                password=config.DB_PASS,
                host=config.DB_HOST,
                port=config.DB_PORT)
            self.cursor = self.connection.cursor()

        except Exception as err:
            print('Database connection failed for '+config.DB_USER+'@'+config.DB_HOST+'/'+config.DB_DATABASE)
            print(err)
            
    def _close(self):
        self.cursor.close()
        self.connection.close()
                
    def _save_pickle(self, data):
        with open(self.DIR_PICKLE, 'wb') as file:
            print("Save Pickle")
            pickle.dump(data, file)
            
    def _load_pickle(self):
        with open(self.DIR_PICKLE, 'rb') as file:
            print("Load Pickle")
            return pickle.load(file)      
        
    def execute_result_query(self):
        with open(path.join(self.QUERY, 'questions.sql'), 'r') as query:
            sql = query.read()
            self._connect()
            response = self.cursor.execute(sql)
            self.connection.commit()
            self._close()
            return response
          
    def create_resources_task(self):
        self._connect()
        self.cursor.execute(config.CREATE_CAYENA_SCHEMA)
        self.cursor.execute(config.CREATE_BOOKS_TABLE)
        self.connection.commit()
        self._close()
        
    def get_books_url_task(self):
        run = True
        htmls = []
        cont = 1
        while run:
            try :
                htmls.append(self._get_all_books_html(self.URL_LINKS % cont))
                cont = cont + 1
            except:
                run = False
        urls = self._generate_books_links_task(sum(htmls, []))
        # df = self._parallelize_job(urls, self._get_books_infos_as_df_task)
        df = self._get_books_infos_as_df_task(urls)
        self._save_pickle(df)
        
    def _generate_books_links_task(self, htmls):
        return [self.URL_BASE + html.find_all("a")[0]['href'] for html in htmls]
    
    def _get_books_infos_as_df_task(self, urls):
        return pd.DataFrame([self._get_book_info(url) for url in urls])
        
    def upload_data_to_postgres_task(self):
        df = self._load_pickle()
        
        self._connect()
            
        self._create_temp_table()

        self._insert_values(df)
        
        self.cursor.execute(f"LOCK TABLE {self.TABLE_NAME} IN EXCLUSIVE MODE;")

        self._merge_values(df)
        
        self.cursor.execute(f"""DROP TABLE IF EXISTS {self.TABLE_NAME_TEMP};""")
        self.connection.commit()
            
        self._close()    
        self._save_pickle(df)
            
    def _create_temp_table(self):
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.TABLE_NAME_TEMP}(
                like {self.TABLE_NAME} including all
            )
        """)
        self.connection.commit()
        
    def _insert_values(self, df):
        data = [tuple(x) for x in df.to_numpy()]
        cols = ','.join(list(df.columns))
        query  = "INSERT INTO %s(%s) VALUES %%s" % (self.TABLE_NAME_TEMP, cols)
        try:
            psycopg2.extras.execute_values(self.cursor, query, data)
            self.connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.connection.rollback()
            
    def _merge_values(self, df):
        cols = ','.join(list(df.columns))
        source_cols = ','.join([f"Source.{c}" for c in list(df.columns)])
        update_cols = ','.join([f"{c} = Source.{c}" for c in list(df.columns)])        
        
        sql_update = f"""
        UPDATE {self.TABLE_NAME}
        SET  {update_cols}
        FROM {self.TABLE_NAME_TEMP} AS Source
        WHERE Source.{self.PRIMARY_KEY} = {self.TABLE_NAME}.{self.PRIMARY_KEY};
        """
        self.cursor.execute(sql_update)
        
        sql_insert = f"""
        INSERT INTO {self.TABLE_NAME}({cols})
        SELECT {source_cols}
        FROM {self.TABLE_NAME_TEMP} AS Source
        LEFT OUTER JOIN {self.TABLE_NAME} ON (Source.{self.PRIMARY_KEY} = {self.TABLE_NAME}.{self.PRIMARY_KEY})
        WHERE {self.TABLE_NAME}.{self.PRIMARY_KEY} IS NULL;
        """
        
        self.cursor.execute(sql_insert)
        self.connection.commit()
                 
    def _get_html_from_url(self, url):
        return BeautifulSoup(urlopen(url).read(), "html.parser")
            
    def _get_all_books_html(self, url):
        html = self._get_html_from_url(url)
        return html.find_all("li", class_="col-xs-6 col-sm-4 col-md-3 col-lg-3" )

    def _get_rate(self, html):
        return self.rate[re.search(r'"star-rating (.*?)"', str(html)).group(1).strip().lower()]

    def _get_table_info_from_html(self, html):
        info = html.find_all('table', class_="table table-striped")[0].find_all('td')
        
        upc            = (re.search(r'<td>(.*?)</td>', str(info.pop(0))).group(1)).lower()
        item_type      = (re.search(r'<td>(.*?)</td>', str(info.pop(0))).group(1)).lower()
        price_excl_tax = (re.search(r'<td>£(.*?)</td>', str(info.pop(0))).group(1)).lower()
        price_incl_tax = (re.search(r'<td>£(.*?)</td>', str(info.pop(0))).group(1)).lower()
        tax            = (re.search(r'<td>£(.*?)</td>', str(info.pop(0))).group(1)).lower()
        stock          = int(re.search(r'stock \((.*?) available\)</td>', str(info.pop(0))).group(1))
        reviews        = int(re.search(r'<td>(.*?)</td>', str(info.pop(0))).group(1))
        
        return {
            "upc": upc,
            "item_type": item_type,
            "price_excl_tax": price_excl_tax,
            "price_incl_tax": price_incl_tax,
            "tax": tax,
            "stock": stock,
            "reviews": reviews,
        }

    def _get_category(self, html):
        return (re.search(r'/books/(.*?)_', str(html.find_all('ul', class_="breadcrumb")[0])).group(1)).lower()

    def _get_price(self, html):
        return float(re.search(r'<p class="price_color">£(.*?)</p>', str(html)).group(1).strip())
        
    def _get_book_info(self, url):
        html = self._get_html_from_url(url)

        table = self._get_table_info_from_html(html)

        return {
            "upc": table['upc'],
            "item_type": table['item_type'],
            "category": self._get_category(html),
            "rate": self._get_rate(html),
            "price": self._get_price(html),
            "price_excl_tax": table['price_excl_tax'],
            "price_incl_tax": table['price_incl_tax'],
            "tax": table['tax'],
            "stock": table['stock'],
            "reviews": table['reviews']
        }