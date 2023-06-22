# import time
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
import time
from fipe.scripts.loggers import get_logger
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

logger = get_logger(__name__)


def get_config_file(path):
    ...


def open_chrome(url):
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
    except [WebDriverException, AttributeError]:
        logger.error("This site can't be reached")
        return close_browser(driver)


def close_browser(driver):
    return driver.close()


# General Functions
def locate_bt(driver: ChromeDriverManager, xpath: str):
    bt_general = driver.find_element(By.XPATH, xpath)
    return bt_general


def scroll_to_element(driver, xpath):
    js_code = "arguments[0].scrollIntoView();"
    try:
        bt = driver.find_element(By.XPATH, xpath)
        return driver.execute_script(js_code, bt)
    except ConnectionRefusedError:
        return logger.error("Not locating element on Browser")


def click_with_driver(driver: ChromeDriverManager, bt_or_box):
    js_code = "arguments[0].click();"
    return driver.execute_script(js_code, bt_or_box)


def click(bt_or_box):
    return bt_or_box.click()


def add_on(bt_or_box, info: str):
    bt_or_box.send_keys(info)
    return bt_or_box.send_keys(Keys.ENTER)


if __name__ == "__main__":
    ...
