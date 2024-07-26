import re
import threading
from dataclasses import dataclass

from lxml import html

from logging import Logger

from selenium import webdriver
from selenium.common import NoSuchElementException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


@dataclass
class Stats:
    """
    Represents a company stats

    Args:
        sector: Company sector
        industry: Company industry
        employees: Company employees
    """
    def __init__(self, sector: str, industry: str, employees: str = None, **kwargs):
        self.sector = sector
        self.industry = industry
        self.employees = employees
        self.__dict__.update(kwargs)

    def __str__(self):
        return f"{self.sector} - {self.industry} - {self.employees}"


@dataclass
class Company:
    """
    Represents a company profile

    Args:
        name: Company name
        equity_ticker: Company equity ticker
        stats: Company stats
        info: Company info
        description: Company
    """
    def __init__(self, name: str, equity_ticker: str, stats: Stats, info: list, description: str):
        self.name = name
        self.equity_ticker = equity_ticker
        self.stats = stats
        self.info = info
        self.description = description

    def __str__(self):
        return f"{self.name} ({self.equity_ticker})"

    def describe(self):
        return f"{self.name} ({self.equity_ticker})\n" \
               f"Stats: {self.stats}\n" \
               f"Info: {self.info}\n" \
               f"Description: {self.description}"


def format_string(text: str) -> str:
    return text.lower().replace(' ', '_')


class YFinanceScrapper:
    def __init__(self, path="/usr/lib/chromium-browser/chromedriver"):
        service = Service(path)
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.page_load_strategy = 'eager'
        self.driver = webdriver.Chrome(service=service, options=options)
        self.logger = Logger("YFinanceScrapper")

    def company(self, ticker="PETR4.SA"):
        try:
            url = f"https://finance.yahoo.com/quote/{ticker}/profile/"
            self.driver.get(url)

            self.logger.info(f"Scraping {url}")
            present = EC.presence_of_element_located((By.CLASS_NAME, "main"))
            WebDriverWait(self.driver, 5).until(present)
            title = self.driver.find_elements(By.TAG_NAME, "h1")[1].text

            self.logger.info(f"Scrapped {title} profile")
            element_company_details = self.driver.find_element(By.CLASS_NAME, "company-details")
            element_info = element_company_details.find_element(By.CLASS_NAME, "company-info")
            element_stats = element_company_details.find_element(By.CLASS_NAME, "company-stats")

            info = element_info.text.split("\n")

            stats = {}
            for row in html.fromstring(element_stats.get_attribute("innerHTML")):
                key, value = tuple(row)
                key = re.sub('[:\\n]', '', key.text).strip()
                value = (value.text or list(value)[0].text).strip()

                stats[format_string(key)] = format_string(value)

            description_element = self.driver.find_element(By.XPATH, '//*[@data-testid="description"]')

            description_text = description_element.text

            company = Company(
                name=title,
                equity_ticker=ticker,
                stats=Stats(
                    **stats
                ),
                info=info,
                description=description_text
            )

            return company
        except Exception as e:
            self.logger.error(f"Error: {e}")

    def lookup(self, query: str, n: int = 25):
        # pass query and return top n results
        url = f"https://finance.yahoo.com/lookup/all?s={query}&t=A&b=0&c={n}"

        self.driver.get(url)

        table = self.driver.find_element(By.CLASS_NAME, "lookup-table")
        body = table.find_element(By.TAG_NAME, "tbody")

        results = []

        for row in body.find_elements(By.TAG_NAME, "tr"):
            symbol, name, last_price, sector, type, exchange = row.find_elements(By.TAG_NAME, "td")
            symbol = symbol.find_element(By.TAG_NAME, "a")
            try:
                sector = sector.find_element(By.TAG_NAME, "a").text
            except NoSuchElementException:
                sector = None
            results.append({
                "symbol": symbol.text,
                "name": name.text,
                "last_price": last_price.text,
                "sector": sector,
                "type": type.text,
                "exchange": exchange.text
            })

        return results

    def lookup_threaded(self, query: str, n: int = 25):
        # use threading to allow users to type while searching
        thread = threading.Thread(target=self.lookup, args=(query, n))
        thread.start()
        return thread


if __name__ == "__main__":
    yf = YFinanceScrapper()
    # company = yf.company("PETR4.SA")
    # print(company.describe())

    results = yf.lookup("PETR4.SA", 5)
    print("\n".join([f"{r['symbol']} - {r['name']} - {r['sector']}" for r in results]))
