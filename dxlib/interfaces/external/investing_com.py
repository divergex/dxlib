from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import json

chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36")


def historical(security_id: str, start_date: str = "2020-01-21", end_date: str = "2024-09-17"):
    driver = webdriver.Chrome(options=chrome_options)

    url = (f"https://api.investing.com/api/financialdata/historical/{security_id}?"
           f"start-date={start_date}&end-date={end_date}&time-frame=Daily&add-missing-rows=false")

    headers = '{ headers: { "domain-id": "www" }, method: "GET" }'

    script = (f"return fetch('{url}', {headers})"
              ".then(response => response.json())"
              ".then(data => { return JSON.stringify(data) })"
              ".catch(error => { return JSON.stringify({error: error.message}) });"
              )

    result = driver.execute_script(script)
    parsed_result = json.loads(result)
    print(json.dumps(parsed_result, indent=2))

    # size:
    print(len(parsed_result["data"]))

    driver.quit()


historical("1089886")
