import requests
from bs4 import BeautifulSoup
import sqlite3
import schedule
import time

class QuoteScraper:
    def __init__(self, base_url):
        self.base_url = base_url
        self.db_name = 'quotes.db'
        self.create_database()

    def create_database(self):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS quotes
                          (id INTEGER PRIMARY KEY AUTOINCREMENT,
                           text TEXT,
                           author TEXT,
                           tags TEXT)''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS authors
                          (id INTEGER PRIMARY KEY AUTOINCREMENT,
                           name TEXT,
                           about TEXT)''')
        conn.commit()
        conn.close()

    def scrape_quotes(self):
        quotes = []
        page = 1
        while True:
            response = requests.get(f'{self.base_url}/page/{page}/')
            if response.status_code != 200:
                break
            soup = BeautifulSoup(response.text, 'html.parser')
            quote_divs = soup.find_all('div', class_='quote')
            if not quote_divs:
                break
            for quote in quote_divs:
                text = quote.find('span', class_='text').text
                author = quote.find('small', class_='author').text
                tags = ', '.join([tag.text for tag in quote.find_all('a', class_='tag')])
                quotes.append((text, author, tags))
            page += 1
        return quotes

    def scrape_author_info(self, author_name):
        author_url = f'{self.base_url}/author/{author_name.replace(" ", "-")}/'
        response = requests.get(author_url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            about = soup.find('div', class_='author-description').text.strip()
            return about
        return None

    def update_database(self):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

        quotes = self.scrape_quotes()
        for quote in quotes:
            cursor.execute('INSERT OR REPLACE INTO quotes (text, author, tags) VALUES (?, ?, ?)', quote)

            # Check if author exists in authors table
            cursor.execute('SELECT * FROM authors WHERE name = ?', (quote[1],))
            if not cursor.fetchone():
                about = self.scrape_author_info(quote[1])
                if about:
                    cursor.execute('INSERT INTO authors (name, about) VALUES (?, ?)', (quote[1], about))

        conn.commit()
        conn.close()
        print(f"Base de datos actualizada con {len(quotes)} citas.")

def run_scraper():
    scraper = QuoteScraper('https://quotes.toscrape.com')
    scraper.update_database()

if __name__ == '__main__':
    run_scraper()  # Ejecutar inmediatamente al inicio
    schedule.every(24).hours.do(run_scraper)  # Programar para ejecutar cada 24 horas

    while True:
        schedule.run_pending()
        time.sleep(1)