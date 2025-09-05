import numpy as np
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os
import shutil
import pathlib

"""
Web scraping for Social Security affiliation data
"""

# Target directory
target_dir = "C:/Users/holub/Data/afiliados/src_data"
os.makedirs(target_dir, exist_ok=True)

# Downloads folder
downloads_dir = str(pathlib.Path.home() / "Downloads")

# Set up Firefox options
options = Options()
options.add_argument("--headless")
options.set_preference(
    "browser.helperApps.neverAsk.saveToDisk",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel",
)

# URLs to scrape
urls = {
    "early_data": "https://www.seg-social.es/wps/portal/wss/internet/EstadisticasPresupuestosEstudios/Estadisticas/EST8/EST167/5b11b695-cf1c-4abe-8a12-ecd2d0d15271/2683/2684?changeLanguage=es",
    "late_data": "https://www.seg-social.es/wps/portal/wss/internet/EstadisticasPresupuestosEstudios/Estadisticas/EST8/EST167/5b11b695-cf1c-4abe-8a12-ecd2d0d15271/2683/3460",
}


def scrape_url(url_name, url):
    print(f"\n{'=' * 50}")
    print(f"SCRAPING: {url_name}")
    print(f"{'=' * 50}")

    # Get current files in Downloads before starting
    initial_files = set()
    if os.path.exists(downloads_dir):
        initial_files = set(
            [f for f in os.listdir(downloads_dir) if f.endswith((".xlsx", ".xls"))]
        )

    # Start selenium webdriver
    driver = webdriver.Firefox(options=options)

    try:
        print("Loading page...")
        driver.get(url)

        # Wait for page to load
        wait = WebDriverWait(driver, 20)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(5)

        # Accept cookies if present (from Code 2)
        try:
            cookie_accept = driver.find_element(
                By.XPATH,
                "//button[contains(text(), 'Accept') or contains(text(), 'Aceptar')]",
            )
            cookie_accept.click()
            time.sleep(2)
        except:
            print("No cookie banner found or already accepted")

        # Find download links using multiple methods (combining both approaches)
        download_links = []

        # Method 1 (from Code 1): Direct CSS selector
        links1 = driver.find_elements(By.CSS_SELECTOR, "a[href*='/descarga/']")
        download_links.extend(links1)

        # Method 2 (from Code 2): Multiple CSS selectors
        links2 = driver.find_elements(
            By.CSS_SELECTOR,
            "a[href*='.xlsx'], a[href*='descarga'], a[href*='download']",
        )
        download_links.extend([link for link in links2 if link not in download_links])

        # Method 3 (from Code 2): XPath for XLSX links
        links3 = driver.find_elements(
            By.XPATH, "//a[contains(@href, '.xlsx') or contains(text(), 'XLSX')]"
        )
        download_links.extend([link for link in links3 if link not in download_links])

        # Method 4 (from Code 2): Links in table cells
        links4 = driver.find_elements(
            By.XPATH,
            "//td[contains(text(), 'XLSX')]//a | //td//a[contains(@href, 'xlsx')]",
        )
        download_links.extend([link for link in links4 if link not in download_links])

        # Remove duplicates while preserving order
        unique_links = []
        seen = set()
        for link in download_links:
            href = link.get_attribute("href")
            if href not in seen:
                seen.add(href)
                unique_links.append(link)

        download_links = unique_links
        print(f"Found {len(download_links)} unique download links")

        successful_downloads = 0

        for i, link in enumerate(download_links):
            try:
                # Get link text and context
                link_text = link.text.strip()
                href = link.get_attribute("href")

                # Try multiple methods to get context (combining approaches from both codes)
                context_text = ""

                # Method 1: Try to get the table row context
                try:
                    parent_row = link.find_element(By.XPATH, "./ancestor::tr")
                    context_text = parent_row.text.strip()
                except:
                    # Method 2: Try to get context from parent cell
                    try:
                        parent_cell = link.find_element(By.XPATH, "./ancestor::td")
                        context_text = parent_cell.text.strip()
                    except:
                        context_text = link_text

                print(f"[{i + 1}/{len(download_links)}] Downloading: {context_text}")
                if href:
                    print(f"  Link: {href}")

                # Scroll to link and click
                driver.execute_script(
                    "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});",
                    link,
                )
                time.sleep(1)

                # Try multiple click methods (from both codes)
                try:
                    wait.until(EC.element_to_be_clickable(link))
                    link.click()
                    successful_downloads += 1
                    print("  ✓ Download initiated")
                    time.sleep(
                        3
                    )  # Slightly longer wait between downloads (from Code 2)
                except Exception as click_error:
                    # Try JavaScript click
                    try:
                        driver.execute_script("arguments[0].click();", link)
                        successful_downloads += 1
                        print("  ✓ JavaScript click succeeded")
                        time.sleep(3)
                    except Exception as js_error:
                        print("  ✗ Both click methods failed:")
                        print(f"    - Normal click: {click_error}")
                        print(f"    - JavaScript click: {js_error}")
                        continue

            except Exception as e:
                print(f"  ✗ Error with link {i + 1}: {e}")
                continue

        print(f"\nTotal downloads initiated: {successful_downloads}")
        print("Waiting for downloads to complete...")
        time.sleep(5)

    except Exception as e:
        print(f"Error: {e}")

    finally:
        driver.quit()

    # Return the initial files set for file movement
    return initial_files


def move_new_files(initial_files, url_name):
    print(f"\nMoving files from Downloads to {target_dir} for {url_name}")
    moved_count = 0

    if os.path.exists(downloads_dir):
        # Get current files in Downloads
        current_files = set(
            [f for f in os.listdir(downloads_dir) if f.endswith((".xlsx", ".xls"))]
        )

        # Find new files (files that weren't there before)
        new_files = current_files - initial_files

        print(f"Found {len(new_files)} new Excel files to move")

        for file in new_files:
            try:
                source_path = os.path.join(downloads_dir, file)
                target_path = os.path.join(target_dir, file)

                # Handle duplicate filenames by adding timestamp (from Code 2)
                if os.path.exists(target_path):
                    name, ext = os.path.splitext(file)
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    new_filename = f"{name}_{url_name}_{timestamp}{ext}"
                    target_path = os.path.join(target_dir, new_filename)
                    print(f"  Renaming to avoid duplicate: {new_filename}")
                else:
                    # Add source tag if not a duplicate
                    name, ext = os.path.splitext(file)
                    new_filename = f"{name}_{url_name}{ext}"
                    target_path = os.path.join(target_dir, new_filename)

                shutil.move(source_path, target_path)
                moved_count += 1
                print(f"  Moved: {file} -> {os.path.basename(target_path)}")
            except Exception as e:
                print(f"  Failed to move {file}: {e}")

    print(f"\nMoved {moved_count} files to target directory")
    return moved_count


def analyze_files():
    # Check final results and organize by year
    if os.path.exists(target_dir):
        files = [f for f in os.listdir(target_dir) if f.endswith((".xlsx", ".xls"))]
        print(f"\nFinal count in target directory: {len(files)} Excel files")

        # Group by year and data source for summary
        years = {}
        sources = {}

        for file in files:
            try:
                file_path = os.path.join(target_dir, file)
                file_size = os.path.getsize(file_path)

                # Try to extract year from filename
                year = "Unknown"
                for y in range(2000, datetime.now().year + 1):
                    if str(y) in file:
                        year = str(y)
                        break

                # Determine source
                source = "Unknown"
                if "early_data" in file:
                    source = "Early Data"
                elif "late_data" in file:
                    source = "Municipal Data (2010-2016)"

                # Add to year grouping
                if year not in years:
                    years[year] = []
                years[year].append((file, file_size, source))

                # Add to source grouping
                if source not in sources:
                    sources[source] = []
                sources[source].append((file, file_size, year))

            except Exception as e:
                print(f"  Error processing {file}: {e}")

        # Print summary by year
        print("\n" + "=" * 50)
        print("FILES BY YEAR")
        print("=" * 50)
        for year in sorted(years.keys()):
            print(f"\n{year}:")
            for file, size, source in sorted(years[year]):
                print(f"  - {file} ({size:,} bytes) [{source}]")

        # Print summary by source
        print("\n" + "=" * 50)
        print("FILES BY SOURCE")
        print("=" * 50)
        for source in sorted(sources.keys()):
            print(f"\n{source}:")
            for file, size, year in sorted(sources[source]):
                print(f"  - {file} ({size:,} bytes) [{year}]")


# Main execution
total_moved = 0

# Process each URL
for url_name, url in urls.items():
    initial_files = scrape_url(url_name, url)
    moved = move_new_files(initial_files, url_name)
    total_moved += moved

# Final analysis
print(f"\nTotal files moved across all sources: {total_moved}")
analyze_files()

# Print completion message
print("\n" + "=" * 50)
print("SCRAPING COMPLETED SUCCESSFULLY")
print("=" * 50)
print(f"Data saved to: {target_dir}")
print(f"Total files collected: {total_moved}")
print("=" * 50)
