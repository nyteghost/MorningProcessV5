import time
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium_stealth import stealth
from selenium.webdriver.chrome.options import Options
import getpass
import fnmatch
import os
from mpConfigs.doorKey import config

logFolder = os.path.join(os.path.normpath(os.getcwd() + os.sep + os.pardir), 'logs')

prefix = r"C:\Users"
localuser = getpass.getuser()
suffix = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\Pre-Database"
excel_relative_file_path = prefix + "\\" + localuser + suffix
dl_path = r"\Downloads"

# Chrome Settings
options = Options()
options.binary_location = "C:/Program Files/Google/Chrome Beta/Application/chrome.exe"
options.add_argument("start-maximized")
# Chrome Settings
options.add_experimental_option("prefs", {
    "download.default_directory": excel_relative_file_path,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})

# Chrome is controlled by automated test software
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)
# s = Service('C:\\BrowserDrivers\\chromedriver.exe')
driver = webdriver.Chrome(options=options)

# Selenium Stealth settings
stealth(driver,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
        )

driver.get("https://www.ups.com/claims?loc=en_US")

username = driver.find_element(By.ID, "email")
password = driver.find_element(By.ID, "pwd")
time.sleep(1)
username.send_keys(config['ups']['name'])
time.sleep(2)
password.send_keys(config['ups']['password'])
time.sleep(1)
driver.find_element(By.ID, 'submitBtn').click()  # Presses Log In Button

# Find Drop Down List
print("Claims Dashboard loading. Waiting for Selection.")

wait = WebDriverWait(driver, 30)
time.sleep(20)

# Click Export Data
wait.until(EC.element_to_be_clickable((By.LINK_TEXT, 'Export Table'))).click()

# Once box opens with choices for download, selects xlsx format
xlsx = driver.find_element(By.ID, 'ups-xlsx')
driver.execute_script("arguments[0].click();", xlsx)

# Click download button
driver.find_element(By.CSS_SELECTOR, "div.ups-form_ctaGroup:nth-child(3) > button:nth-child(1)").click()

counter = 0
while counter < 3:
    print('counter =' + str(counter))
    time.sleep(30)

    for file in os.listdir(excel_relative_file_path):
        if fnmatch.fnmatch(file, 'Report*.xlsx'):
            print('Found Report.xlsx')
            counter += 3
        else:
            print('Trying to find Report')
            counter += 1

print("Report downloaded.")

driver.close()
driver.quit()