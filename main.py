import asyncio
import json
import sys
import time
from math import e
from pprint import pp, pprint
from aiohttp.client_exceptions import ClientConnectorError

from selenium import webdriver
# from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.firefox.webdriver import WebDriver
from selenium.common.exceptions import NoSuchElementException
# from selenium.webdriver.chrome.options import Options
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from nextcord.ext import ipc
from nextcord.ext.ipc.client import Client

async def main():
    with open("creds.txt", "r", encoding="utf-8") as f:
        CREDS = f.read().splitlines()

    with open("page_ids.json", "r", encoding="utf-8") as f:
        PAGE_IDS: dict[str, str] = json.load(f)
    
    try:
        with open("data.json", "r", encoding="utf-8") as f:
            data: dict[str, list[dict[str, str]]] = json.load(f)
    except FileNotFoundError:
        data = dict()
        for id in PAGE_IDS.keys():
            data[id] = list()
        update(data)
    
    ipc_client = ipc.Client(secret_key=CREDS[2])
    
    # Configurar las opciones de Chrome para el modo headless
    firefox_options = Options()
    # firefox_options.add_argument('--headless')  # Ejecutar en modo headless

    # Crear una instancia del navegador
    driver = webdriver.Firefox(options=firefox_options, executable_path=r'./geckodriver.exe')
    driver.implicitly_wait(30)

    # Iniciar sesión
    login(driver, CREDS[0], CREDS[1])
    
    title = driver.title
    print(f'Page: {title}')

    failedTrys = 0
    while failedTrys < 10:
        if failedTrys > 0:
            ipc_client = ipc.Client(secret_key=CREDS[2])
        try:
            ipc_status = await ipc_client.request("status")
            if ipc_status == 200:
                failedTrys = 0
                await cycle(driver, PAGE_IDS, data, ipc_client)
                time.sleep(5)
            else:
                failedTrys += 1
                print("IPC server not ready")
                time.sleep(5)
        except ClientConnectorError as e:
            failedTrys += 1
            print("IPC server not responding, trying again in 10 seconds...")
            time.sleep(10)

    # Cerrar el navegador al finalizar
    print("It's seems that the IPC server is not responding, closing the program...")
    driver.quit()
    sys.exit(1)

def login(driver: WebDriver, user: str, password: str) -> None:
    print(f"Attempting to login with {user=:}...")
    # URL que deseas hacer scraping
    url = 'https://www3.gobiernodecanarias.org/educacion/cau_ce/cas/login?service=https%3A%2F%2Fwww3.gobiernodecanarias.org%2Fmedusa%2Feforma%2Fcampus%2Flogin%2Findex.php'
    # url = "https://www.google.com"
    driver.get(url)

    userForm = WebDriverWait(driver, 30).until(lambda x: x.find_element(By.ID, "fm1"))
    userInput = WebDriverWait(driver, 30).until(lambda x: x.find_element(By.ID, "username"))
    passInput = WebDriverWait(driver, 30).until(lambda x: x.find_element(By.ID, "password"))
    button = WebDriverWait(driver, 30).until(lambda x: x.find_element(By.ID, "boton-login"))

    userInput.send_keys(user)
    passInput.send_keys(password)
    # userForm.submit()
    button.click()

async def cycle(driver: WebDriver, PAGE_IDS: dict[str, str], data: dict[str, list[dict[str, str]]], ipc_client: Client) -> None:
    print("Starting cycle...")
    for id, name in PAGE_IDS.items():
        pageData = data[id]
        newPage = computeRow(driver, id, PAGE_IDS)
        time.sleep(1)

        if pageData != newPage:
            print(f"Page with {id=:} has changed.")

            # Diferencia entre las dos listas
            diff = difference(pageData, newPage)

            if diff:
                data[id] = newPage
                print("There is a difference.")
                for row in diff:
                    page = scrapePage(driver, row["link"])
                    page["link"] = row["link"]
                    page["name"] = name
                    await publish(page, ipc_client)
                    update(data)
                    time.sleep(2)
        time.sleep(5)
def computeRow(driver: WebDriver, id: str, PAGE_IDS: dict[str, str]) -> list:
    driver.get(f"https://www3.gobiernodecanarias.org/medusa/eforma/campus/mod/assign/index.php?id={id}")
    title = driver.title
    print(f'Page: {title}. Id: {id}. Name: {PAGE_IDS[id]}.')

    mainContent = WebDriverWait(driver, 30).until(lambda x: x.find_element(By.CLASS_NAME, "generaltable"))
    tbody = mainContent.find_element(By.TAG_NAME, "tbody")
    rows = list()
    for row in tbody.find_elements(By.TAG_NAME, "tr"):
        try:
            rowContent = dict()
            for cell in row.find_elements(By.TAG_NAME, "td"):
                colspan = cell.get_attribute("colspan")
                if colspan:
                    break
                if cell.get_attribute("class") == "cell c1":
                    rowContent["title"] = cell.text
                    rowContent["link"] = cell.find_element(By.TAG_NAME, "a").get_attribute("href")
                elif cell.get_attribute("class") == "cell c2":
                    if cell.text == "-":
                        rowContent["date"] = "Todavía no hay fecha de entrega."
                    else:
                        rowContent["date"] = cell.text
        except NoSuchElementException as e:
            print(e)
            continue
        if rowContent:
            rows.append(rowContent)
    return rows

def scrapePage(driver: WebDriver, url: str) -> dict[str, str | list[dict[str, str]]]:
    driver.get(url)
    title = driver.title
    print(f'Page: {title}')

    page = dict()
    urls = list()

    mainContent = WebDriverWait(driver, 30).until(lambda x: x.find_element(By.ID, "region-main"))
    page["title"] = mainContent.find_element(By.TAG_NAME, "h2").text
    header = mainContent.find_element(By.CLASS_NAME, "activity-header")
    page["content"] = header.text
    currentUrls = header.find_elements(By.TAG_NAME, "a")

    for posibleUrl in currentUrls:
        if posibleUrl.get_attribute("class") != "action-icon":
            urls.append({"text": posibleUrl.text, "link": posibleUrl.get_attribute("href")})
    page["urls"] = urls
    return page

def difference(old: list[dict[str, str]], new: list[dict[str, str]]) -> list:
    old_set = set(tuple(sorted(d.items())) for d in old)
    new_set = set(tuple(sorted(d.items())) for d in new)

    diff_set = new_set - old_set

    diff_list = [dict(item) for item in diff_set]

    return diff_list

async def publish(pageData: dict[str, str], ipc_client: Client) -> None:
    print("Attempting to publish...")
    await ipc_client.request("publish", pageData=pageData)

def update(data: dict[str, list[dict[str, str]]]) -> None:
    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

if __name__ == "__main__":
    asyncio.run(main())