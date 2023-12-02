import asyncio
import json
import platform
import sys
import time
from aiohttp.client_exceptions import ClientConnectorError

from selenium import webdriver
from selenium.webdriver.firefox.webdriver import WebDriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from nextcord.ext import ipc
from nextcord.ext.ipc.client import Client

async def main():
    try:
        with open("creds.txt", "r", encoding="utf-8") as f:
            CREDS = f.read().splitlines()

        with open("page_ids.json", "r", encoding="utf-8") as f:
            PAGE_IDS: dict[str, str] = json.load(f)
    except FileNotFoundError:
        print("It seems that you don't have the required files. Please, read README.md#installation.")
        sys.exit(1)
    
    try:
        with open("data.json", "r", encoding="utf-8") as f:
            data: dict[str, list[dict[str, str]]] = json.load(f)
    except FileNotFoundError:
        data = dict()
        for page_id in PAGE_IDS.keys():
            data[page_id] = list()
        update(data)

    ipc_client = ipc.Client(secret_key=CREDS[2])

    options = Options()
    # options.add_argument('--headless')  # Ejecutar en modo headless

    platform_name = platform.system().lower()
    match platform_name:
        case "linux":
            driver = webdriver.Firefox(options=options, executable_path=r'geckodriver')
        case "windows":
            print("Using Windows. This is not recommended.")
            driver = webdriver.Firefox(options=options, executable_path=r'geckodriver.exe')
        case _:
            print("Your OS is not supported, please use Linux.")
            sys.exit(1)

    login(driver, CREDS[0], CREDS[1])

    title = driver.title
    print(f'Page: {title}')

    failed_tries = 0
    while failed_tries < 10:
        if failed_tries > 0:
            ipc_client = ipc.Client(secret_key=CREDS[2])
        try:
            ipc_status = await ipc_client.request("status")
            if ipc_status == 200:
                failed_tries = 0
                await cycle(driver, PAGE_IDS, data, ipc_client)
                time.sleep(5)
            else:
                failed_tries += 1
                print("IPC server not ready")
                time.sleep(5)
        except ClientConnectorError:
            failed_tries += 1
            print("IPC server not responding, trying again in 10 seconds...")
            time.sleep(10)

    print("It's seems that the IPC server is not responding, exiting...")
    driver.quit()
    sys.exit(1)

def login(driver: WebDriver, user: str, password: str) -> None:
    print(f"Attempting to login with {user=:}...")
    url = 'https://www3.gobiernodecanarias.org/educacion/cau_ce/cas/login?service=https%3A%2F%2Fwww3.gobiernodecanarias.org%2Fmedusa%2Feforma%2Fcampus%2Flogin%2Findex.php'
    driver.get(url)

    # userForm = WebDriverWait(driver, 30).until(lambda x: x.find_element(By.ID, "fm1"))
    user_input = WebDriverWait(driver, 30).until(lambda x: x.find_element(By.ID, "username"))
    pass_input = WebDriverWait(driver, 30).until(lambda x: x.find_element(By.ID, "password"))
    button = WebDriverWait(driver, 30).until(lambda x: x.find_element(By.ID, "boton-login"))

    user_input.send_keys(user)
    pass_input.send_keys(password)
    # userForm.submit()
    button.click()

async def cycle(driver: WebDriver, PAGE_IDS: dict[str, str], data: dict[str, list[dict[str, str]]], ipc_client: Client) -> None:
    print("Starting cycle...")
    for page_id, page_name in PAGE_IDS.items():
        page_data = data[page_id]
        new_page = computeRow(driver, page_id, PAGE_IDS)
        time.sleep(1)

        if page_data != new_page:
            print(f"Page with {page_id=:} has changed.")

            diff = difference(page_data, new_page)

            if diff:
                data[page_id] = new_page
                print("There is a difference.")
                for row in diff:
                    page = scrapePage(driver, row["link"])
                    page["link"] = row["link"]
                    page["name"] = page_name
                    await publish(page, ipc_client)
                    update(data)
                    time.sleep(2)
        time.sleep(5)
def computeRow(driver: WebDriver, page_id: str, PAGE_IDS: dict[str, str]) -> list:
    driver.get(f"https://www3.gobiernodecanarias.org/medusa/eforma/campus/mod/assign/index.php?id={page_id}")
    title = driver.title
    print(f'Page: {title}. Id: {page_id}. Name: {PAGE_IDS[page_id]}.')

    main_content = WebDriverWait(driver, 30).until(lambda x: x.find_element(By.CLASS_NAME, "generaltable"))
    table_body = main_content.find_element(By.TAG_NAME, "tbody")
    rows = list()
    for row in table_body.find_elements(By.TAG_NAME, "tr"):
        try:
            row_content = dict()
            for cell in row.find_elements(By.TAG_NAME, "td"):
                colspan = cell.get_attribute("colspan")
                if colspan:
                    break
                if cell.get_attribute("class") == "cell c1":
                    row_content["title"] = cell.text
                    row_content["link"] = cell.find_element(By.TAG_NAME, "a").get_attribute("href")
                elif cell.get_attribute("class") == "cell c2":
                    if cell.text == "-":
                        row_content["date"] = "Todavía no hay fecha de entrega."
                    else:
                        row_content["date"] = cell.text
        except NoSuchElementException as e:
            print(e)
            continue
        if row_content:
            rows.append(row_content)
    return rows

def scrapePage(driver: WebDriver, url: str) -> dict[str, str | list[dict[str, str]]]:
    driver.get(url)
    title = driver.title
    print(f'Page: {title}')

    page = dict()
    urls = list()

    main_content = WebDriverWait(driver, 30).until(lambda x: x.find_element(By.ID, "region-main"))
    page["title"] = main_content.find_element(By.TAG_NAME, "h2").text
    header = main_content.find_element(By.CLASS_NAME, "activity-header")
    page["content"] = header.text
    current_urls = header.find_elements(By.TAG_NAME, "a")

    for possible_urls in current_urls:
        if possible_urls.get_attribute("class") != "action-icon":
            urls.append({"text": possible_urls.text, "link": possible_urls.get_attribute("href")})
    page["urls"] = urls
    return page

def difference(old: list[dict[str, str]], new: list[dict[str, str]]) -> list:
    old_set = set(tuple(sorted(d.items())) for d in old)
    new_set = set(tuple(sorted(d.items())) for d in new)

    diff_set = new_set - old_set

    diff_list = [dict(item) for item in diff_set]

    return diff_list

async def publish(page_data: dict[str, str], ipc_client: Client) -> None:
    print("Attempting to publish...")
    await ipc_client.request("publish", page_data=page_data)

def update(data: dict[str, list[dict[str, str]]]) -> None:
    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

if __name__ == "__main__":
    asyncio.run(main())