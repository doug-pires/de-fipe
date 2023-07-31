# import time

import time

import chromedriver_autoinstaller
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webelement import WebElement
from webdriver_manager.chrome import ChromeDriverManager

from fipe.scripts.loggers import get_logger

logger = get_logger(__name__)


def retry_search(max_attempts, delay=2):
    """
    A decorator function that retries the execution of a given function in case of exceptions.

    Args:
        max_attempts (int): The maximum number of attempts to execute the function.
        delay (int, optional): The delay (in seconds) between each retry attempt. Defaults to 2.

    Returns:
        function: A wrapper function that performs the retry logic.

    Example usage:
        @retry_search(max_attempts=3, delay=5)
        def search(query):
            # Function implementation

    """

    def decorator(original_function):
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return original_function(*args, **kwargs)
                except Exception:
                    attempts += 1
                    logger.error(f"Attempt {attempts} failed to connect")
                    time.sleep(delay)
            logger.critical(
                f"Data extraction stopped, because Function {original_function.__name__} failed after {max_attempts} attempts"
            )

        return wrapper

    return decorator


# @retry_search(max_attempts=3, delay=3)
def open_chrome(url: str, headless: bool = True):
    """Open Google Chrome browser and navigate to the specified URL.

    Args:
        url (str): The URL to navigate to.
        headless (bool, optional): Whether to run the browser in headless mode. Defaults to True.

    Returns:
        webdriver.Chrome: The Chrome WebDriver instance.
    """

    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless")
    options.add_experimental_option("detach", True)
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_argument("--disable-dev-shm-usage")

    options.add_argument("--no-sandbox")
    try:
        driver = webdriver.Chrome(
            service=ChromeService(ChromeDriverManager().install()),
            options=options,
        )
    except ValueError:
        # Always check the Chrome Version.
        chromedriver_autoinstaller.install()

        driver = webdriver.Chrome(
            options=options,
        )
    try:
        logger.info("Loading Google Chrome")
        driver.get(url)
        time.sleep(3)
        logger.info("Loaded Google Chrome")
        return driver
    except Exception:
        logger.error("This site can't be reached")
        close_browser(driver)
        raise Exception


def close_browser(driver: WebDriver):
    """Close the browser.

    Args:
        driver (WebDriver): The Selenium WebDriver instance.

    Returns:
        None
    """
    return driver.close()


def locate_bt(driver: WebDriver, xpath: str) -> WebElement:
    """Locate a button element on the page using XPath.

    Args:
        driver (WebDriver): The Selenium WebDriver instance.
        xpath (str): The XPath expression to locate the button element.

    Returns:
        WebElement: The located button element.
    """
    bt_general = driver.find_element(By.XPATH, xpath)
    return bt_general


def scroll_to_element(driver: WebDriver, xpath: str) -> WebElement | None:
    """Scroll to an element on the page using JavaScript.

    Args:
        driver (WebDriver): The Selenium WebDriver instance.
        xpath (str): The XPath expression to locate the element.

    Returns:
        WebElement or None: The located element after scrolling or None if the element is not found.
    """
    js_code = "arguments[0].scrollIntoView();"
    try:
        bt = driver.find_element(By.XPATH, xpath)
        return driver.execute_script(js_code, bt)
    except ConnectionRefusedError:
        return logger.error("Not locating element on Browser")


def click_with_driver(driver: WebDriver, bt_or_box: str):
    """Click on an element using the Selenium WebDriver.

    Args:
        driver (WebDriver): The Selenium WebDriver instance.
        bt_or_box (str): The element to click, identified by a CSS selector, XPath expression, or other locator.

    Returns:
        None
    """
    js_code = "arguments[0].click();"
    return driver.execute_script(js_code, bt_or_box)


@retry_search(max_attempts=3, delay=2)
def click(bt_or_box):
    """Click on a WebElement.

    Args:
        bt_or_box (WebElement): The WebElement to click.

    Returns:
        None
    """
    return bt_or_box.click()


def add_on(bt_or_box, info: str):
    """Add information to an element and press Enter.

    Args:
        bt_or_box (WebElement): The WebElement to interact with.
        info (str): The information to be added.

    Returns:
        None
    """
    bt_or_box.send_keys(info)
    return bt_or_box.send_keys(Keys.ENTER)


if __name__ == "__main__":
    open_chrome("https://www.google.com/", False)
