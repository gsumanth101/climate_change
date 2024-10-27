# ----------IMPORTS----------
import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from splinter import Browser
from webdriver_manager.chrome import ChromeDriverManager
import numpy as np
from selenium import webdriver
from selenium.webdriver.chrome.service import Service

# Set up Splinter with WebDriver Manager
service = Service(ChromeDriverManager().install())
options = webdriver.ChromeOptions()
browser = Browser('chrome', options=options, service=service, headless=False)

# ---------OPEN BROWSER AND NAVIGATE TO WEBSITE------------
# FOR COUNTRY FLAGS
images_url = "https://www.worldometers.info/geography/flags-of-the-world/"
browser.visit(images_url)
myhtml = browser.html
soup = BeautifulSoup(myhtml, 'html.parser')

# --------CREATE NEW LIST----------
country_flags = []

# --------USING BEAUTIFUL SOUP TO GET INTO HTML------------
divs = soup.find_all('div', class_="col-md-4")

# --------LOOPING THROUGH THE DIVS TO PULL IMAGE HREF AND COUNTRY NAME----
for div in divs:
    try:
        # Your existing code here
        pass
    except Exception as e:
        print(e)