import requests
from bs4 import BeautifulSoup
from database import SessionLocal
import models
from logger import logger
from ratelimit import limits, sleep_and_retry
from robotexclusionrulesparser import RobotFileParserLookalike
import time

class QuoteScraper:
    def __init__(self, urls):
        self.urls = urls
        self.robots_parsers = {}
        self.initialize_robots_parsers()

    def initialize_robots_parsers(self):
        for url in self.urls:
            rp = RobotFileParserLookalike()
            robots_url = f"{url}/robots.txt"
            rp.set_url(robots_url)
            rp.read()
            self.robots_parsers[url] = rp

    def can_fetch(self, url, user_agent="*"):
        parsed_url = requests.utils.urlparse(url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        if base_url in self.robots_parsers:
            return self.robots_parsers[base_url].can_fetch(user_agent, url)
        return True

    @sleep_and_retry
    @limits(calls=1, period=2)  # 1 call every 2 seconds
    def rate_limited_request(self, url):
        if not self.can_fetch(url):
            logger.warning(f"No se permite el scraping de {url} según robots.txt")
            return None
        response = requests.get(url)
        response.raise_for_status()
        return response

    def scrape_quotes(self):
        all_quotes = []
        for url in self.urls:
            logger.info(f"Iniciando scraping de {url}")
            if "toscrape" in url:
                all_quotes.extend(self.scrape_toscrape(url))
            elif "goodreads" in url:
                all_quotes.extend(self.scrape_goodreads(url))
        logger.info(f"Scraping completado. Total de citas obtenidas: {len(all_quotes)}")
        return all_quotes

    def scrape_toscrape(self, base_url):
        quotes = []
        page = 1
        while True:
            try:
                url = f'{base_url}/page/{page}/'
                response = self.rate_limited_request(url)
                if response is None:
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
                logger.info(f"Página {page} de {base_url} procesada. Citas obtenidas: {len(quotes)}")
            except requests.RequestException as e:
                logger.error(f"Error al procesar {base_url}/page/{page}/: {str(e)}")
                break
        return quotes

    def scrape_goodreads(self, url):
        quotes = []
        try:
            response = self.rate_limited_request(url)
            if response is None:
                return quotes
            soup = BeautifulSoup(response.text, 'html.parser')
            quote_divs = soup.find_all('div', class_='quote')
            for quote in quote_divs:
                text = quote.find('div', class_='quoteText').text.strip().split('\n')[0]
                author = quote.find('span', class_='authorOrTitle').text.strip()
                tags = ', '.join([tag.text for tag in quote.find_all('a', class_='greyText smallText')])
                quotes.append((text, author, tags))
            logger.info(f"Scraping de {url} completado. Citas obtenidas: {len(quotes)}")
        except requests.RequestException as e:
            logger.error(f"Error al procesar {url}: {str(e)}")
        return quotes

    def update_database(self):
        db = SessionLocal()
        try:
            quotes = self.scrape_quotes()
            for quote in quotes:
                db_quote = models.Quote(text=quote[0], author=quote[1], tags=quote[2])
                db.add(db_quote)
            db.commit()
            logger.info(f"Base de datos actualizada con {len(quotes)} citas.")
        except Exception as e:
            logger.error(f"Error al actualizar la base de datos: {str(e)}")
            db.rollback()
        finally:
            db.close()