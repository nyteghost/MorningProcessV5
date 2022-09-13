import time
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium_stealth import stealth
from selenium.webdriver.chrome.options import Options
import getpass
import fnmatch
import os
from mpConfigs.doorKey import config

# from logger_setup import setup_logger, log_location
# importlog = setup_logger('upsoutboundscrape', log_location + "\\" + "UPS Outbound" + "\\" + 'UPSOutboundScrape.log')

logFolder = os.path.join(os.path.normpath(os.getcwd() + os.sep + os.pardir), 'logs')

print("Setting Profile for connection, please be patient")

prefix = r"C:\Users"
localuser = getpass.getuser()
suffix = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\Pre-Database"
excel_relative_file_path = prefix + "\\" + localuser + suffix
dl_path = r"\Downloads"

# Chrome Settings
options = Options()
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)
options.add_argument("start-maximized")

# Chrome Settings
options.add_experimental_option(
    "prefs", {
        "download.default_directory": excel_relative_file_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True})

# Chrome is controlled by automated test software
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)
# s = Service('C:\\BrowserDrivers\\chromedriver.exe')
driver = webdriver.Chrome(options=options)
# driver = webdriver.Chrome()


# Selenium Stealth settings
stealth(driver,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
        )

wait = WebDriverWait(driver, 30)
# UPS Claims Scrape
print("Connecting to UPS Outbound.")
# driver.set_window_size(1920, 1080)
driver.get("https://www.ups.com/webqvm/?loc=en_US#/outbound")  ## Website to open
print("Connected to site.\nLogging in.")
time.sleep(10)

# Find and Input Username and Password and login
username = wait.until(EC.presence_of_element_located((By.ID, "email")))
password = driver.find_element(By.ID, "pwd")
username.send_keys(config['ups']['name'])
password.send_keys(config['ups']['password'])
driver.find_element(By.ID, 'submitBtn').click()  # Presses Log In Button

print("Logged in.\nDownloading outbound shipment information.")

retries = 0
while retries < 5:
    try:
        outbound_download = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="downloadCSVBtn"]')))
        driver.execute_script("arguments[0].click();", outbound_download)
    except TimeoutException:
        retries += 1
        driver.refresh()
    else:
        retries = 5

counter = 0
while counter < 3:
    time.sleep(10)
    for file in os.listdir(excel_relative_file_path):
        if fnmatch.fnmatch(file, 'outbound*.csv'):
            print('Outbound Found')
            counter += 3
        else:
            print('Trying to find Report')
            counter += 1

print("Outbound Shipment info downloaded.")
# importlog.info('Outbound Shipment info downloaded.')

driver.close()
driver.quit()
