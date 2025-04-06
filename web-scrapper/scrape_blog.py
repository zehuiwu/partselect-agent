import csv
import time
import random
import os
from urllib.parse import urljoin
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager
import tempfile

def setup_driver():
    """Set up a Chrome WebDriver with appropriate options."""
    print("Setting up Chrome WebDriver...")
    
    # Create a unique temporary directory for Chrome data
    temp_dir = tempfile.mkdtemp()
    print(f"Using temporary directory: {temp_dir}")
    
    chrome_options = Options()
    
    # Add options to make the browser more stable
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-web-security")
    chrome_options.add_argument("--disable-features=IsolateOrigins,site-per-process")
    chrome_options.add_argument("--disable-site-isolation-trials")
    
    # Set window size
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Add user agent to mimic a real browser
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    
    # Add experimental options
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    
    # Set user data directory
    chrome_options.add_argument(f"--user-data-dir={temp_dir}")
    
    try:
        # Use webdriver_manager to handle driver installation
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Set page load timeout
        driver.set_page_load_timeout(60)
        driver.implicitly_wait(20)
        
        print("Chrome WebDriver setup successful")
        return driver
    except Exception as e:
        print(f"Error setting up Chrome WebDriver: {e}")
        return None

def safe_navigate(driver, url, max_retries=3):
    """Safely navigate to a URL with retry mechanism."""
    for attempt in range(max_retries):
        try:
            print(f"Navigating to {url} (attempt {attempt+1}/{max_retries})")
            driver.get(url)
            
            # Wait for the page to load
            WebDriverWait(driver, 30).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            
            # Check if we got an access denied page
            if "Access Denied" in driver.title or "Forbidden" in driver.title:
                print("Access denied page detected, retrying...")
                if attempt < max_retries - 1:
                    delay = random.uniform(5, 10)
                    print(f"Waiting {delay:.1f} seconds before retry...")
                    time.sleep(delay)
                    continue
                else:
                    print("Max retries reached for access denied page")
                    return False
            
            # Wait a bit longer for dynamic content to load
            time.sleep(3)
            
            # Scroll down to load more content
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            
            # Add a small random delay to appear more human-like
            time.sleep(random.uniform(1, 3))
            return True
            
        except Exception as e:
            print(f"Error navigating to {url}: {e}")
            if attempt < max_retries - 1:
                delay = random.uniform(2, 5)
                print(f"Retrying in {delay:.1f} seconds...")
                time.sleep(delay)
            else:
                print(f"Failed to navigate to {url} after {max_retries} attempts")
                return False

def extract_blog_data(driver, base_url):
    """Extract blog titles and URLs from the page."""
    blogs = []
    
    try:
        # Wait for the blog container to be present
        print("Waiting for blog container...")
        wait = WebDriverWait(driver, 30)
        blog_container = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='main'].blog.row"))
        )
        
        # Find all blog links - both hero articles and regular articles
        selectors = [
            "a.blog__hero-article",  # Featured blog
            "a.article-card"         # Regular blog entries
        ]
        
        blog_links = []
        for selector in selectors:
            try:
                print(f"Finding blogs with selector: {selector}")
                links = blog_container.find_elements(By.CSS_SELECTOR, selector)
                print(f"Found {len(links)} links with {selector}")
                blog_links.extend(links)
            except Exception as e:
                print(f"Error with selector {selector}: {e}")
        
        if not blog_links:
            print("No blog links found")
            return blogs
        
        print(f"Found total {len(blog_links)} blog links")
        
        for idx, link in enumerate(blog_links, 1):
            try:
                # Extract the URL
                url = link.get_attribute("href")
                if not url:
                    continue
                
                # Extract title from URL
                url_path = url.split('/blog/')[-1].rstrip('/')
                title = url_path.replace('-', ' ').title()
                
                if not title:
                    continue
                
                blogs.append({
                    'title': title,
                    'url': url
                })
                
            except Exception as e:
                print(f"Error extracting blog data: {e}")
                continue
        
    except Exception as e:
        print(f"Error in extract_blog_data: {e}")
    
    return blogs

def scrape_all_blogs(base_url, num_pages=18):
    """Scrape all blog pages and extract titles and URLs."""
    all_blogs = []
    driver = None
    
    try:
        driver = setup_driver()
        if not driver:
            print("Failed to set up WebDriver")
            return all_blogs
        
        for page_num in range(1, num_pages + 1):
            page_url = f"{base_url}?start={page_num}"
            print(f"\nProcessing page {page_num}/{num_pages}")
            
            if not safe_navigate(driver, page_url):
                print(f"Skipping page {page_num} due to navigation error")
                continue
            
            blogs = extract_blog_data(driver, base_url)
            all_blogs.extend(blogs)
            
            print(f"Found {len(blogs)} blogs on page {page_num}")
            print(f"Total blogs collected so far: {len(all_blogs)}")
            
            # Add a delay between pages to avoid being blocked
            if page_num < num_pages:
                delay = random.uniform(2, 4)
                print(f"Waiting {delay:.1f} seconds before next page...")
                time.sleep(delay)
    
    except Exception as e:
        print(f"Error during scraping: {e}")
    
    finally:
        if driver:
            driver.quit()
    
    return all_blogs

def save_to_csv(blogs, filename):
    """Save the scraped blog data to a CSV file."""
    if not blogs:
        print("No blog data to save")
        return
    
    try:
        fieldnames = ['title', 'url']
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(blogs)
        
        print(f"\nSuccessfully saved {len(blogs)} blogs to {filename}")
    
    except Exception as e:
        print(f"Error saving to CSV: {e}")

if __name__ == "__main__":
    base_url = "https://www.partselect.com/content/blog"
    output_file = "partselect_blogs.csv"
    
    print(f"Starting to scrape blogs from {base_url}")
    blogs = scrape_all_blogs(base_url)
    
    if blogs:
        print(f"\nTotal blogs collected: {len(blogs)}")
        save_to_csv(blogs, output_file)
    else:
        print("No blogs were collected") 