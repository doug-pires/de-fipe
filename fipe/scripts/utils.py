# import time

import time

from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webelement import WebElement
from webdriver_manager.chrome import ChromeDriverManager

from fipe.scripts.loggers import get_logger

logger = get_logger(__name__)


def open_chrome(url: str):
    options = webdriver.ChromeOptions()
    options.add_experimental_option("detach", True)
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(
        service=ChromeService(ChromeDriverManager().install()),
        options=options,
    )
    try:
        logger.info("Loading Google Chrome")
        driver.get(url)
        time.sleep(3)
        logger.info("Loaded Google Chrome")
        return driver
    except [WebDriverException]:
        logger.error("This site can't be reached")
        return close_browser(driver)


def close_browser(driver: WebDriver):
    return driver.close()


# General Functions
def locate_bt(driver: WebDriver, xpath: str) -> WebElement:
    bt_general = driver.find_element(By.XPATH, xpath)
    return bt_general


def scroll_to_element(driver: WebDriver, xpath: str) -> WebElement | None:
    js_code = "arguments[0].scrollIntoView();"
    try:
        bt = driver.find_element(By.XPATH, xpath)
        return driver.execute_script(js_code, bt)
    except ConnectionRefusedError:
        return logger.error("Not locating element on Browser")


def click_with_driver(driver: WebDriver, bt_or_box: str):
    js_code = "arguments[0].click();"
    return driver.execute_script(js_code, bt_or_box)


def click(bt_or_box):
    return bt_or_box.click()


def add_on(bt_or_box, info: str):
    bt_or_box.send_keys(info)
    return bt_or_box.send_keys(Keys.ENTER)


if __name__ == "__main__":
    open_chrome("https://www.youtube.com/")
