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

# importlog Settings
print("Setting Profile for connection, please be patient")

# Chrome Settings
options = Options()
# options.binary_location = "C:/Program Files/Google/Chrome Beta/Application/chrome.exe"
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

driver.get("https://online.upscapital.com/claims")  # Website to open

# Find and Input Username and Password and login
username = driver.find_element(By.XPATH, "//*[@id='mat-input-0']")
password = driver.find_element(By.XPATH, "//*[@id='mat-input-1']")
username.send_keys(config['capitalclaims']['name'])
password.send_keys(config['capitalclaims']['password'])
driver.find_element(By.CLASS_NAME, 'upsc-button').click()  # Opens Add-ons

attempts = 0
counter = 0

# Wait for and Locate Export Button
wait = WebDriverWait(driver, 60)  # Wait time setup for 1200 Seconds or 20 minutes. Should Never take this long.
export_button = wait.until(EC.element_to_be_clickable((By.XPATH,'/html/body/upsc-root/upsc-layout/div[3]/div/div/div/upsc-claims/div/upsc-claim-list/div/div[1]/div[2]/div[1]')))
export_button.click()
driver.implicitly_wait(5)
driver.find_element(By.XPATH, '//*[@id="mat-dialog-0"]/upsc-export/div/mat-dialog-actions/div/a[2]').click()
print('Downloading Claims File')
time.sleep(5)
while counter < 3:
    for file in os.listdir(excel_relative_file_path):
        if fnmatch.fnmatch(file, 'claim_submission_summary.xlsx'):
            print('Found Claim Submission Summary')
            # upsclaimsscraperlog.info('Claim File Downloaded')
            counter += 3
            break
        break
    else:
        time.sleep(30)
        counter += 1

driver.close()
driver.quit()
# Move all files from Downloads to Pre-Database
