# test_browser.py
import os
import sys
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

print("Python version:", sys.version)
print("Current working directory:", os.getcwd())

try:
    print("Initializing webdriver...")
    chrome_path = "/usr/local/bin/chromedriver"
    print(f"ChromeDriver path: {chrome_path}")
    print(f"ChromeDriver exists: {os.path.exists(chrome_path)}")
    print(f"ChromeDriver is executable: {os.access(chrome_path, os.X_OK)}")
    
    options = Options()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--headless')  # Add headless for testing
    
    service = Service(executable_path=chrome_path)
    driver = webdriver.Chrome(service=service, options=options)
    
    print("WebDriver initialized successfully!")
    driver.get("https://www.google.com")
    print(f"Current URL: {driver.get_current_url()}")
    
    driver.quit()
    print("Test completed successfully!")
except Exception as e:
    print(f"Error: {type(e).__name__}: {str(e)}")
    import traceback
    traceback.print_exc()
