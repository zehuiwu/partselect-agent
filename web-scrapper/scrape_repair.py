import requests
from bs4 import BeautifulSoup
import csv
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException, NoSuchElementException, WebDriverException
import urllib.parse
import socket
import random

def setup_driver():
    """Set up and return a configured Chrome driver."""
    try:
        print("Setting up Chrome options...")
        chrome_options = Options()
        
        # Set a realistic user agent
        user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        chrome_options.add_argument(f'user-agent={user_agent}')
        
        # Add headers to appear more like a real browser
        chrome_options.add_argument('--accept-language=en-US,en;q=0.9')
        chrome_options.add_argument('--accept=text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8')
        
        # Basic options for stability
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-gpu")
        
        # Add unique user data directory
        import tempfile
        import os
        temp_dir = os.path.join(tempfile.gettempdir(), f"chrome_temp_{os.getpid()}")
        chrome_options.add_argument(f"--user-data-dir={temp_dir}")
        print(f"Using temporary directory: {temp_dir}")
        
        # Additional stability options
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-notifications")
        
        # Disable automation flags
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Page load strategy
        chrome_options.page_load_strategy = 'normal'
        
        print("Chrome options configured. Attempting to create driver...")
        driver = webdriver.Chrome(options=chrome_options)
        
        # Execute CDP commands to disable automation
        driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": user_agent})
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
            """
        })
        
        print("Chrome driver created successfully")
        
        # Set timeouts
        print("Setting timeouts...")
        driver.set_page_load_timeout(60)  # Increased timeout
        driver.set_script_timeout(30)
        driver.implicitly_wait(20)  # Increased implicit wait
        
        return driver
    except Exception as e:
        print(f"Failed to create driver: {str(e)}")
        raise

def safe_get_text(element):
    """Safely get text from an element."""
    try:
        return element.text.strip()
    except:
        return ""

def safe_get_attribute(element, attribute):
    """Safely get attribute from an element."""
    try:
        return element.get_attribute(attribute)
    except:
        return ""

def wait_for_element(driver, by, value, timeout=10):
    """Wait for an element to be present."""
    try:
        wait = WebDriverWait(driver, timeout)
        element = wait.until(EC.presence_of_element_located((by, value)))
        return element
    except:
        return None

def extract_percentage(text):
    """Extract percentage from text."""
    try:
        return text.split("%")[0].strip()
    except:
        return "0"

def get_symptom_data(symptom_element):
    """Extract data from a symptom element."""
    try:
        print("Extracting symptom data...")
        
        # Get the URL first as it's most likely to succeed
        print("Getting URL...")
        url = safe_get_attribute(symptom_element, "href")
        print(f"Found URL: {url}")
        
        # Get the symptom name
        print("Getting symptom name...")
        try:
            title = symptom_element.find_element(By.CLASS_NAME, "title-md")
            symptom = safe_get_text(title)
            print(f"Found symptom: {symptom}")
        except Exception as e:
            print(f"Error getting symptom name: {e}")
            return None
        
        # Get the description
        print("Getting description...")
        try:
            description = safe_get_text(symptom_element.find_element(By.TAG_NAME, "p"))
            print(f"Found description: {description[:50]}...")  # Print first 50 chars
        except Exception as e:
            print(f"Error getting description: {e}")
            return None
        
        # Get the percentage
        print("Getting percentage...")
        try:
            percentage_element = symptom_element.find_element(By.CLASS_NAME, "symptom-list__reported-by")
            percentage = extract_percentage(safe_get_text(percentage_element))
            print(f"Found percentage: {percentage}")
        except Exception as e:
            print(f"Error getting percentage: {e}")
            return None
        
        return {
            'symptom': symptom,
            'description': description,
            'percentage': percentage,
            'symptom_detail_url': url
        }
    except Exception as e:
        print(f"Error extracting symptom data: {e}")
        return None

def get_repair_details(driver, url):
    """Get repair details from a symptom page."""
    try:
        driver.get(url)
        wait_for_element(driver, By.CLASS_NAME, "repair__intro")
        
        # Get difficulty
        difficulty = ""
        difficulty_item = driver.find_element(By.CSS_SELECTOR, "ul.list-disc li")
        if difficulty_item:
            difficulty = safe_get_text(difficulty_item)
            difficulty = difficulty.replace("Rated as", "").strip()
        
        # Get parts
        parts = []
        part_links = driver.find_elements(By.CSS_SELECTOR, "div.repair__intro a.js-scrollTrigger")
        for link in part_links:
            part_name = safe_get_text(link)
            if part_name:
                parts.append(part_name)
        
        # Get video URL
        video_url = ""
        try:
            video_element = driver.find_element(By.CSS_SELECTOR, "div[data-yt-init]")
            if video_element:
                video_id = safe_get_attribute(video_element, "data-yt-init")
                if video_id:
                    video_url = f"https://www.youtube.com/watch?v={video_id}"
                    print(f"Found video URL: {video_url}")
        except Exception as e:
            print(f"No video found or error getting video URL: {e}")
        
        return {
            'parts': ", ".join(parts),
            'difficulty': difficulty,
            'repair_video_url': video_url
        }
    except Exception as e:
        print(f"Error getting repair details: {e}")
        return {'parts': '', 'difficulty': '', 'repair_video_url': ''}

def safe_navigate(driver, url, max_retries=3):
    """Safely navigate to a URL with retries and ensure page is fully loaded."""
    for attempt in range(max_retries):
        try:
            print(f"Navigating to {url} (attempt {attempt+1}/{max_retries})")
            
            # Add a random delay between attempts
            if attempt > 0:
                delay = random.uniform(3, 7)
                print(f"Waiting {delay:.1f} seconds before retry...")
                time.sleep(delay)
            
            driver.get(url)
            
            # Wait for document ready state to be complete
            wait = WebDriverWait(driver, 30)
            wait.until(lambda driver: driver.execute_script('return document.readyState') == 'complete')
            print("Page load complete")
            
            # Check for access denied
            if "Access Denied" in driver.title:
                print("Access Denied page detected")
                if attempt < max_retries - 1:
                    print("Retrying with delay...")
                    continue
                return False
            
            # Verify we can find some basic elements
            try:
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                print("Found body element")
                return True
            except TimeoutException:
                print("Timeout waiting for body element")
                if attempt < max_retries - 1:
                    print("Retrying...")
                    continue
                
        except Exception as e:
            print(f"Navigation error (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                print("Retrying after error...")
            else:
                print(f"Failed to navigate to {url} after {max_retries} attempts")
                return False
    
    return False

def scrape_repairs(base_url, appliance_type):
    """
    Scrape repair information from the website.
    
    Args:
        base_url: The URL to scrape from
        appliance_type: The type of appliance (e.g., 'Dishwasher', 'Refrigerator')
    """
    repairs_data = []
    driver = None
    
    try:
        driver = setup_driver()
        print(f"\nProcessing {appliance_type} repairs...")
        print(f"Attempting to navigate to {base_url}")
        
        # Use safe navigation
        if not safe_navigate(driver, base_url):
            print(f"Failed to load the {appliance_type} base URL")
            return repairs_data
            
        print("Waiting for symptom list...")
        # Wait longer for the symptom list
        wait = WebDriverWait(driver, 30)  # Increased timeout
        try:
            symptom_list = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "symptom-list")))
            print("Found symptom list")
        except TimeoutException:
            print("Timeout waiting for symptom list")
            # Try to print the page source for debugging
            try:
                print("\nPage source:")
                print(driver.page_source[:500])  # Print first 500 chars
            except:
                print("Could not get page source")
            return repairs_data
        
        # Get all symptom elements and store their data immediately
        print("Collecting all symptom elements...")
        symptom_elements = symptom_list.find_elements(By.TAG_NAME, "a")
        print(f"Found {len(symptom_elements)} symptoms")
        
        # Store the initial data to prevent stale elements
        symptom_data_list = []
        for idx, element in enumerate(symptom_elements, 1):
            try:
                print(f"\nCollecting initial data for symptom {idx}/{len(symptom_elements)}")
                # Store the URL and href immediately
                url = safe_get_attribute(element, "href")
                if not url:
                    print("No URL found for symptom, skipping")
                    continue
                    
                # Try to get the HTML content of the element
                html_content = element.get_attribute('outerHTML')
                print(f"Element HTML: {html_content[:200]}...")  # Print first 200 chars
                
                symptom_data = get_symptom_data(element)
                if symptom_data:
                    symptom_data_list.append(symptom_data)
                    print(f"Successfully collected initial data for symptom: {symptom_data['symptom']}")
                else:
                    print("Failed to collect symptom data")
                
            except Exception as e:
                print(f"Error collecting initial symptom data: {e}")
                continue
        
        # Now process each collected symptom data
        print(f"\nProcessing {len(symptom_data_list)} collected symptoms")
        for idx, symptom_data in enumerate(symptom_data_list, 1):
            try:
                print(f"\nProcessing symptom {idx}/{len(symptom_data_list)}: {symptom_data['symptom']}")
                
                # Get repair details using the stored URL
                full_url = urllib.parse.urljoin(base_url, symptom_data['symptom_detail_url'])
                print(f"Getting repair details from: {full_url}")
                repair_details = get_repair_details(driver, full_url)
                
                # Combine all data
                repair_entry = {
                    'Product': appliance_type,
                    'symptom': symptom_data['symptom'],
                    'description': symptom_data['description'],
                    'percentage': symptom_data['percentage'],
                    'parts': repair_details['parts'],
                    'symptom_detail_url': full_url,
                    'difficulty': repair_details['difficulty'],
                    'repair_video_url': repair_details['repair_video_url']
                }
                
                repairs_data.append(repair_entry)
                print(f"Successfully processed: {repair_entry['symptom']}")
                print(f"Parts found: {repair_entry['parts']}")
                print(f"Difficulty: {repair_entry['difficulty']}")
                if repair_entry['repair_video_url']:
                    print(f"Video URL: {repair_entry['repair_video_url']}")
                
            except Exception as e:
                print(f"Error processing symptom: {e}")
                continue
            
            # Add a small delay between symptoms
            delay = random.uniform(2, 4)
            print(f"Waiting {delay:.1f} seconds before next symptom...")
            time.sleep(delay)
    
    except Exception as e:
        print(f"Error during scraping: {e}")
        # Try to print the page source for debugging
        try:
            if driver:
                print("\nPage source:")
                print(driver.page_source[:500])  # Print first 500 chars
        except:
            pass
    
    finally:
        if driver:
            driver.quit()
    
    return repairs_data

def process_appliance(appliance_type, base_url, output_file):
    """Process repairs for a specific appliance type."""
    print(f"\nStarting {appliance_type} repair information scraping...")
    repairs_data = scrape_repairs(base_url, appliance_type)
    
    if repairs_data:
        print(f"\nFound {len(repairs_data)} repair entries for {appliance_type}")
        save_to_csv(repairs_data, output_file)
        return len(repairs_data)
    else:
        print(f"No repair data was collected for {appliance_type}")
        return 0

def save_to_csv(data, filename):
    """Save the scraped data to a CSV file."""
    if not data:
        print("No data to save")
        return
    
    try:
        fieldnames = ['Product', 'symptom', 'description', 'percentage', 'parts', 'symptom_detail_url', 'difficulty', 'repair_video_url']
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        
        print(f"\nSuccessfully saved {len(data)} repairs to {filename}")
    
    except Exception as e:
        print(f"Error saving to CSV: {e}")

if __name__ == "__main__":
    appliances = [
        {
            'type': 'Dishwasher',
            'url': 'https://www.partselect.com/Repair/Dishwasher/',
            'output': 'dishwasher_repairs.csv'
        },
        {
            'type': 'Refrigerator',
            'url': 'https://www.partselect.com/Repair/Refrigerator/',
            'output': 'refrigerator_repairs.csv'
        }
    ]
    
    total_repairs = 0
    for appliance in appliances:
        repairs_count = process_appliance(
            appliance['type'],
            appliance['url'],
            appliance['output']
        )
        total_repairs += repairs_count
        
        # Add a longer delay between appliances
        if appliance != appliances[-1]:  # If not the last appliance
            delay = random.uniform(5, 10)
            print(f"\nWaiting {delay:.1f} seconds before processing next appliance...")
            time.sleep(delay)
    
    print(f"\nScraping completed. Total repairs found across all appliances: {total_repairs}") 