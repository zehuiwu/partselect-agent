import requests
from bs4 import BeautifulSoup
import csv
import time
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException, NoSuchElementException, WebDriverException
import urllib.parse
import socket
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed


def wait_and_find_element(driver, by, value, timeout=10):
    """Helper function to wait for an element and handle stale element exceptions"""
    wait = WebDriverWait(driver, timeout)
    try:
        element = wait.until(EC.presence_of_element_located((by, value)))
        return element
    except (TimeoutException, StaleElementReferenceException):
        return None


def wait_and_find_elements(driver, by, value, timeout=10):
    """Helper function to wait for elements and handle stale element exceptions"""
    wait = WebDriverWait(driver, timeout)
    try:
        elements = wait.until(EC.presence_of_all_elements_located((by, value)))
        return elements
    except (TimeoutException, StaleElementReferenceException):
        return []


def safe_get_text(element):
    """Safely get text from an element, handling stale element exceptions"""
    try:
        return element.text
    except StaleElementReferenceException:
        return "N/A"


def safe_get_attribute(element, attribute):
    """Safely get attribute from an element, handling stale element exceptions"""
    try:
        return element.get_attribute(attribute)
    except StaleElementReferenceException:
        return "N/A"
    

def is_valid_url(url):
    """Check if a URL is valid and can be resolved"""
    try:
        # Parse the URL
        parsed_url = urllib.parse.urlparse(url)
        # Check if the URL has a scheme and netloc
        if not parsed_url.scheme or not parsed_url.netloc:
            return False
        
        # Try to resolve the domain
        socket.gethostbyname(parsed_url.netloc)
        return True
    except (ValueError, socket.gaierror):
        return False


def safe_navigate(driver, url, max_retries=3):
    """Safely navigate to a URL with retries and ensure page is fully loaded"""
    for attempt in range(max_retries):
        try:
            #print(f"Navigating to {url} (attempt {attempt+1}/{max_retries})")
            driver.get(url)
            
            # Wait for document ready state to be complete
            wait = WebDriverWait(driver, 30)
            wait.until(lambda driver: driver.execute_script('return document.readyState') == 'complete')
            
            # Determine if this is a product page or category page based on URL
            is_product_page = "/PS" in url or ".htm" not in url
            
            # Wait for key elements that indicate the page has loaded
            try:
                if is_product_page:
                    # For product pages, wait for product-specific elements
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.pd__wrap")))
                    # Also wait for price container which is crucial
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "span.price.pd__price")))
                else:
                    # For category pages, wait for navigation and product list
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.container")))
                    wait.until(EC.presence_of_element_located((By.CLASS_NAME, "nf__links")))
                
                #print(f"Page loaded successfully: {url}")
                return True
            except TimeoutException as e:
                print(f"Timeout waiting for key elements to load: {str(e)}")
                # Check if the page actually loaded despite timeout
                try:
                    if is_product_page:
                        # Try alternative elements for product pages
                        if driver.find_elements(By.CSS_SELECTOR, "div.pd__wrap") or \
                           driver.find_elements(By.CSS_SELECTOR, "span.price"):
                            print("Page appears to be loaded despite timeout")
                            return True
                    else:
                        # Try alternative elements for category pages
                        if driver.find_elements(By.CSS_SELECTOR, "div.nf__part"):
                            print("Page appears to be loaded despite timeout")
                            return True
                except:
                    pass
                
                if attempt < max_retries - 1:
                    print("Retrying...")
                    time.sleep(5)
                continue
                
        except WebDriverException as e:
            print(f"Navigation error (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                print("Retrying after error...")
                time.sleep(5)
            else:
                print(f"Failed to navigate to {url} after {max_retries} attempts")
                return False
    
    return False


def extract_text_after_header(element, header_text):
    """Extract text after a header in an element"""
    try:
        full_text = safe_get_text(element)
        if header_text in full_text:
            return full_text.replace(header_text, "").strip()
        return full_text
    except Exception:
        return "N/A"


def scrape_part_info(driver, part_name, product_url):
    """
    Scrape information for a specific part from its product page.
    
    Args:
        driver: Selenium WebDriver instance
        part_name: Name of the part
        product_url: URL of the product page
        
    Returns:
        dict: Dictionary containing the part information
    """
    data = {
        'part_name': part_name,
        'part_id': 'N/A',
        'mpn_id': 'N/A',
        'part_price': 'N/A',
        'install_difficulty': 'N/A',
        'install_time': 'N/A',
        'symptoms': 'N/A',
        'product_types': 'N/A',
        'replace_parts': 'N/A',
        'brand': 'N/A',
        'availability': 'N/A',
        'install_video_url': 'N/A',
        'product_url': product_url
    }
    
    # Navigate to the product page
    if not safe_navigate(driver, product_url):
        print(f"Failed to navigate to product {part_name}. Skipping.")
        return data
    
    # Find product ID
    product_id_elements = wait_and_find_elements(driver, By.CSS_SELECTOR, "span[itemprop='productID']")
    if product_id_elements:
        data['part_id'] = safe_get_text(product_id_elements[0])
    
    # Find brand information
    brand_element = wait_and_find_element(driver, By.CSS_SELECTOR, "span[itemprop='brand'] span[itemprop='name']")
    if brand_element:
        data['brand'] = safe_get_text(brand_element)
    
    # Find availability information
    availability_element = wait_and_find_element(driver, By.CSS_SELECTOR, "span[itemprop='availability']")
    if availability_element:
        data['availability'] = safe_get_text(availability_element)
    
    # Find installation video URL
    video_container = wait_and_find_element(driver, By.CSS_SELECTOR, "div.yt-video")
    if video_container:
        video_id = safe_get_attribute(video_container, "data-yt-init")
        if video_id:
            data['install_video_url'] = f"https://www.youtube.com/watch?v={video_id}"
    
    # Find MPN ID
    mpn_elements = wait_and_find_elements(driver, By.CSS_SELECTOR, "span[itemprop='mpn']")
    if mpn_elements:
        data['mpn_id'] = safe_get_text(mpn_elements[0])
    
    # Find replace parts
    replace_parts_elements = wait_and_find_elements(driver, By.CSS_SELECTOR, "div[data-collapse-container='{\"targetClassToggle\":\"d-none\"}']")
    if replace_parts_elements:
        data['replace_parts'] = safe_get_text(replace_parts_elements[0])
    
    # Find part price
    wait = WebDriverWait(driver, 10)
    price_container = wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "span.price.pd__price"))
    )
    
    if price_container:
        # Wait a short time for any dynamic price updates
        time.sleep(1)
        
        # Try multiple approaches to get the price
        price_found = False
        
        # Approach 1: Direct js-partPrice
        price_element = price_container.find_element(By.CSS_SELECTOR, "span.js-partPrice")
        if price_element:
            price_text = safe_get_text(price_element)
            if price_text and price_text != "N/A":
                data['part_price'] = price_text
                price_found = True
        
        # Approach 2: Get from content attribute if direct text failed
        if not price_found:
            price_content = safe_get_attribute(price_container, "content")
            if price_content and price_content != "N/A":
                data['part_price'] = price_content
                price_found = True
        
        # Approach 3: Try getting complete text including currency symbol
        if not price_found:
            full_price = safe_get_text(price_container)
            if full_price and full_price != "N/A":
                data['part_price'] = full_price
                price_found = True
        
        if not price_found:
            print("Warning: Price element found but could not extract price text")
    
    # Find troubleshooting information
    pd_wrap = wait_and_find_element(driver, By.CSS_SELECTOR, "div.pd__wrap.row")
    if pd_wrap:
        # Find all col-md-6 mt-3 divs within the pd_wrap
        info_divs = pd_wrap.find_elements(By.CSS_SELECTOR, "div.col-md-6.mt-3")
        
        for div in info_divs:
            # Get the header text
            header = div.find_element(By.CSS_SELECTOR, "div.bold.mb-1")
            if not header:
                continue
                
            header_text = safe_get_text(header)
            
            # Check which type of information this div contains
            if "This part fixes the following symptoms:" in header_text:
                # Extract symptoms
                data['symptoms'] = extract_text_after_header(div, header_text)
            elif "This part works with the following products:" in header_text:
                # Extract product types
                data['product_types'] = extract_text_after_header(div, header_text)
    
    # Find install difficulty and time
    install_container = wait_and_find_element(driver, By.CSS_SELECTOR, "div.d-flex.flex-lg-grow-1.col-lg-7.col-12.justify-content-lg-between.mt-lg-0.mt-2")
    
    if install_container:
        # Find the two d-flex divs inside the container
        d_flex_divs = install_container.find_elements(By.CLASS_NAME, "d-flex")
        
        if len(d_flex_divs) >= 2:
            # First div contains difficulty
            difficulty_p = d_flex_divs[0].find_element(By.TAG_NAME, "p")
            if difficulty_p:
                data['install_difficulty'] = safe_get_text(difficulty_p)
            
            # Second div contains time
            time_p = d_flex_divs[1].find_element(By.TAG_NAME, "p")
            if time_p:
                data['install_time'] = safe_get_text(time_p)
    
    # Print all extracted data
    # print(f"Part ID: {data['part_id']}")
    # print(f"MPN ID: {data['mpn_id']}")
    # print(f"Part Price: {data['part_price']}")
    # print(f"Install Difficulty: {data['install_difficulty']}")
    # print(f"Install Time: {data['install_time']}")
    # print(f"Symptoms: {data['symptoms']}")
    # print(f"Product Types: {data['product_types']}")
    # print(f"Replace Parts: {data['replace_parts']}")
    # print(f"Brand: {data['brand']}")
    # print(f"Availability: {data['availability']}")
    # print(f"Install Video URL: {data['install_video_url']}")
    
    return data


def process_category_page(driver, link_url):
    """
    Process a category page and scrape all parts within it.
    
    Args:
        driver: Selenium WebDriver instance
        link_url: URL of the category page
        
    Returns:
        list: List of dictionaries containing part information
    """
    parts_data = []
    print(f"\nVisiting: {link_url}")
    
    # Navigate to the category page
    if not safe_navigate(driver, link_url):
        print(f"Failed to navigate to {link_url}. Skipping.")
        return parts_data
    
    # Find all divs with class name "nf__part mb-3" using CSS selector
    part_divs = wait_and_find_elements(driver, By.CSS_SELECTOR, "div.nf__part.mb-3")
    if not part_divs:
        print(f"No parts found in category {link_url}. Skipping.")
        return parts_data
        
    print(f"Found {len(part_divs)} parts")
    
    # Store part information to avoid stale element issues
    part_info = []
    for part_div in part_divs:
        a_tag = part_div.find_element(By.CLASS_NAME, "nf__part__detail__title")
        if not a_tag:
            continue
            
        part_name = safe_get_text(a_tag.find_element(By.TAG_NAME, "span"))
        href = safe_get_attribute(a_tag, "href")
        
        # Validate the URL
        if href and is_valid_url(href):
            part_info.append((part_name, href))
        else:
            print(f"Skipping invalid product URL: {href}")
    
    if not part_info:
        print(f"No valid parts found in category {link_url}. Skipping.")
        return parts_data
    
    # Process each part in the category
    parts_data = process_parts_in_category(driver, part_info, link_url)
    
    return parts_data

def process_parts_in_category(driver, part_info, category_url):
    """
    Process all parts in a category.
    
    Args:
        driver: Selenium WebDriver instance
        part_info: List of tuples containing (part_name, product_url)
        category_url: URL of the category page to return to
        
    Returns:
        list: List of dictionaries containing part information
    """
    parts_data = []
    for part_name, product_url in part_info:
        print(f"\nProcessing part: {part_name}")
        
        # Scrape part information
        part_data = scrape_part_info(driver, part_name, product_url)
        parts_data.append(part_data)
        
        # Go back to the category page
        if not safe_navigate(driver, category_url):
            print(f"Failed to return to category page. Skipping remaining parts.")
            return parts_data
    
    return parts_data

def setup_driver():
    """
    Set up and return a configured Chrome driver.
    
    Returns:
        webdriver.Chrome: Configured Chrome driver
    """
    try:
        print("Setting up Chrome options...")
        chrome_options = Options()
        
        # Start with minimal options for testing
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # Add page load strategy
        chrome_options.page_load_strategy = 'normal'
        
        print("Initializing Chrome driver...")
        driver = webdriver.Chrome(options=chrome_options)
        print("Chrome driver initialized successfully")
        
        # Set longer page load timeout
        print("Setting page load timeout...")
        driver.set_page_load_timeout(60)  # Increased timeout to 60 seconds
        
        # Set script timeout
        driver.set_script_timeout(30)
        
        return driver
    except Exception as e:
        print(f"Failed to create driver: {str(e)}")
        print("Please ensure Chrome is installed and chromedriver is in your PATH")
        raise

def process_brand_with_retry(brand_url, max_retries=3):
    """
    Process a brand page and its related pages with retry mechanism.
    
    Args:
        brand_url: URL of the brand page to process
        max_retries: Maximum number of retry attempts
        
    Returns:
        list: List of dictionaries containing part information
    """
    brand_parts_data = []
    driver = None
    
    for attempt in range(max_retries):
        try:
            # Set up driver for this brand
            driver = setup_driver()
            
            # Step 1: Navigate to brand page
            if not safe_navigate(driver, brand_url):
                print(f"Failed to navigate to brand page {brand_url}. Retrying...")
                if driver:
                    driver.quit()
                continue
            
            # Step 2: Process all products from the brand page
            print("Processing products from brand page...")
            brand_data = process_category_page(driver, brand_url)
            brand_parts_data.extend(brand_data)
            print(f"Found {len(brand_data)} products on brand page")
            
            # Step 3: Collect all Related part pages
            print("Collecting related part pages...")
            related_links = get_related_links(driver)
            print(f"Found {len(related_links)} related part pages")
            
            # Step 4: Process each related page sequentially
            for rel_idx, related_url in enumerate(related_links, 1):
                print(f"\nProcessing related page {rel_idx}/{len(related_links)}: {related_url}")
                if not safe_navigate(driver, related_url):
                    print(f"Failed to navigate to related page {related_url}. Skipping.")
                    continue
                
                related_data = process_category_page(driver, related_url)
                brand_parts_data.extend(related_data)
                print(f"Found {len(related_data)} products on related page")
                
                # Add a small delay between processing related pages
                time.sleep(1)
            
            # Successfully processed brand and its related pages
            print(f"Successfully processed brand {brand_url}")
            driver.quit()
            return brand_parts_data
            
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for brand {brand_url}: {e}")
            if driver:
                driver.quit()
            if attempt < max_retries - 1:
                print("Retrying...")
                time.sleep(5)
            else:
                print(f"Failed to process brand {brand_url} after {max_retries} attempts")
                return brand_parts_data
    
    return brand_parts_data

def get_brand_links(driver, base_url):
    """Get all brand links from the main page"""
    brand_links = []
    if not safe_navigate(driver, base_url):
        print("Failed to navigate to main page")
        return brand_links

    # Wait for the main navigation links
    wait = WebDriverWait(driver, 10)
    try:
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "nf__links")))
        ul_tags = driver.find_elements(By.CLASS_NAME, "nf__links")
        if ul_tags:
            # First ul contains brand links
            li_tags = ul_tags[0].find_elements(By.TAG_NAME, "li")
            print(f"Found {len(li_tags)} brand links")
            
            for li_tag in li_tags:
                try:
                    a_tag = li_tag.find_element(By.TAG_NAME, "a")
                    link_url = safe_get_attribute(a_tag, "href")
                    if link_url and is_valid_url(link_url):
                        brand_links.append(link_url)
                        print(f"Found brand link: {link_url}")
                except Exception as e:
                    print(f"Error processing brand link: {e}")
                    continue
    except Exception as e:
        print(f"Error finding brand links: {e}")
    
    return brand_links

def get_related_links(driver):
    """Get all related part page links from the current page"""
    related_links = []
    try:
        # Find section titles
        section_titles = driver.find_elements(By.CLASS_NAME, "section-title")
        for title in section_titles:
            try:
                title_text = safe_get_text(title)
                if "Related" in title_text and ("Dishwasher Parts" in title_text or "Refrigerator Parts" in title_text):
                    print(f"Found related section: {title_text}")
                    # Find the next ul.nf__links after this title
                    related_ul = title.find_element(By.XPATH, "./following::ul[@class='nf__links'][1]")
                    if related_ul:
                        li_tags = related_ul.find_elements(By.TAG_NAME, "li")
                        print(f"Found {len(li_tags)} related category links")
                        
                        for li_tag in li_tags:
                            try:
                                a_tag = li_tag.find_element(By.TAG_NAME, "a")
                                link_url = safe_get_attribute(a_tag, "href")
                                if link_url and is_valid_url(link_url):
                                    related_links.append(link_url)
                                    print(f"Found related link: {link_url}")
                            except Exception as e:
                                print(f"Error processing related link: {e}")
                                continue
            except Exception as e:
                print(f"Error processing section title: {e}")
                continue
    except Exception as e:
        print(f"Error finding related links: {e}")
    
    return related_links

def scrape_all_parts(base_url):
    """
    Scrape all parts following the correct processing logic with parallel brand processing:
    1. Gather links from all brands at the main page
    2. Process brands in parallel, for each brand:
        a. Process all products from the brand page
        b. Collect all Related part pages
        c. Process all products from each related page sequentially
    
    Args:
        base_url: The base URL to start scraping from
        
    Returns:
        list: List of dictionaries containing part information
    """
    all_parts_data = []
    driver = None
    
    try:
        # Set up initial driver
        print("\nSetting up browser...")
        driver = setup_driver()
        
        # Step 1: Gather all brand links from main page
        print("\nStep 1: Gathering brand links from main page...")
        brand_links = get_brand_links(driver, base_url)
        
        # Clean up initial driver as we'll create new ones for parallel processing
        driver.quit()
        driver = None
        
        if not brand_links:
            print("No brand links found. Exiting.")
            return all_parts_data
        
        # Process brands in parallel
        max_workers = max(1, min(5, len(brand_links)))  # Limit to max 3 workers
        print(f"\nProcessing {len(brand_links)} brands with {max_workers} parallel workers")
        
        completed_brands = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all brand processing tasks
            future_to_url = {
                executor.submit(process_brand_with_retry, url): url 
                for url in brand_links
            }
            
            # Process completed brand tasks
            for future in as_completed(future_to_url):
                brand_url = future_to_url[future]
                try:
                    brand_data = future.result()
                    all_parts_data.extend(brand_data)
                    completed_brands += 1
                    print(f"\nCompleted brand {completed_brands}/{len(brand_links)}: {brand_url}")
                    print(f"Found {len(brand_data)} total products for this brand")
                    print(f"Progress: {completed_brands}/{len(brand_links)} brands processed")
                except Exception as e:
                    print(f"Error processing brand {brand_url}: {e}")
    
    except Exception as e:
        print(f"Error during scraping: {e}")
    
    finally:
        if driver:
            driver.quit()
    
    print(f"\nScraping completed. Total parts found: {len(all_parts_data)}")
    return all_parts_data

def save_to_csv(parts_data, filename):
    """
    Save the parts data to a CSV file.
    
    Args:
        parts_data: List of dictionaries containing part information
        filename: Name of the CSV file to save to
    """
    if not parts_data:
        print("No data to save.")
        return
    
    try:
        # Get the fieldnames from the first dictionary
        fieldnames = parts_data[0].keys()
        
        # Write to CSV file
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(parts_data)
        
        print(f"Successfully saved {len(parts_data)} parts to {filename}")
    
    except Exception as e:
        print(f"Error saving to CSV: {e}")

if __name__ == "__main__":
    # # Base URL for PartSelect dishwasher parts
    # base_url = "https://www.partselect.com/Dishwasher-Parts.htm"
    
    # # Scrape all parts
    # print("Starting dishwasher parts scraping...")
    # parts_data = scrape_all_parts(base_url)
    # print(f"Found {len(parts_data)} dishwasher parts")
    
    # # Save to CSV
    # save_to_csv(parts_data, "dishwasher_parts.csv")
    
    # Scrape refrigerator parts
    base_url = "https://www.partselect.com/Refrigerator-Parts.htm"
    print("\nStarting refrigerator parts scraping...")
    parts_data = scrape_all_parts(base_url)
    print(f"Found {len(parts_data)} refrigerator parts")
    
    # Save to CSV
    save_to_csv(parts_data, "refrigerator_parts.csv")
    
