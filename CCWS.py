import concurrent.futures
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import requests
import csv
import json
import os
import sqlite3

# Flag for debug mode
DEBUG_MODE = False
FRESH_DB = False
classIds = {"Information Technology Specialist I": 7166,
            "Information Technology Specialist II": 7167,
            "Information Technology Technician": 7164,
            "Information Technology Associate": 7165}#, "Information Technology Specialist III": 7168, }
locations = {"Sacramento": 418, "Placer": 382, "Yolo": 660, "El Dorado": 79}

def create_session():
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0'})
    return session

def fetch_html(session, url):
    try:
        response = session.get(url)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Error fetching URL {url}: {e}")
        return None

def search_ca_jobs(class_id, loc_id):
    url = f'https://calcareers.ca.gov/CalHRPublic/Search/JobSearchResults.aspx#classid={class_id}&locid={loc_id}'
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    jobs = []
    try:
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.NAME, 'ctl00$cphMainContent$ddlRowCount')))
        select_element = driver.find_element(By.NAME, 'ctl00$cphMainContent$ddlRowCount')
        Select(select_element).select_by_visible_text('100 Jobs')
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'lead')))
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        jobs = [job.attrs['href'] for job in soup.find_all('a', class_='lead visitedLink')]
    except TimeoutException:
        print(f"No jobs found for class_id={class_id}, loc_id={loc_id}. Moving on...")
    except Exception as e:
        print(f"Error while searching for jobs (class_id={class_id}, loc_id={loc_id}): {e}")
    finally:
        driver.quit()
    return jobs

def get_job_dict():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {(loc, cls): executor.submit(search_ca_jobs, cls_id, loc_id) for loc, loc_id in locations.items() for cls, cls_id in classIds.items()}
        return {loc: {cls: futures[(loc, cls)].result() for cls in classIds} for loc in locations}

def extract():
    job_dict = get_job_dict()
    url_list = [job for city in job_dict.values() for position in city.values() for job in position]
    session = create_session()
    job_details = {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_url = {executor.submit(fetch_html, session, url): url for url in url_list}
        job_details = [future.result() for future in concurrent.futures.as_completed(future_to_url) if future.result()]

    # Save the job details to a JSON file for debugging
    with open('job_data.json', 'w') as f:
        json.dump(job_details, f)

    return job_details

def load_job_data():
    if DEBUG_MODE:
        # Load job data from the JSON file
        if os.path.exists('job_data.json'):
            with open('job_data.json', 'r') as f:
                job_data = json.load(f)
                return job_data
        else:
            print("Debug mode enabled but no job_data.json file found. Exiting.")
            exit(1)
    else:
        return extract()

def transform(jobs):
    def parse_job(data):
        soup = BeautifulSoup(data, 'html.parser')
        return {
            'Classification': ' '.join(soup.find(id="lblPrimaryClassification").get_text().strip().split()[2:]) if soup.find(id="lblPrimaryClassification") else 'Not Specified',
            'Working Title': soup.find(id="lblWorkingTitleHeader").get_text() if soup.find(id="lblWorkingTitleHeader") else 'Not Specified',
            'Department': soup.find(id="lblDepartmentName").get_text().strip() if soup.find(id="lblDepartmentName") else 'Not Specified',
            'Final Filing Date': soup.find(id="lblFinalFilingDate").get_text().strip() if soup.find(id="lblFinalFilingDate") else 'Not Specified',
            'Salary': soup.find(id="lblPrimarySalary").get_text().strip() if soup.find(id="lblPrimarySalary") else 'Not Specified',
            'Location': f"{soup.find(id='rptDropOffAddresses_ctl00_lblCity').get_text().strip()}" if soup.find(id='rptDropOffAddresses_ctl00_lblCity') else 'Not Specified',
            'Job Control Number': soup.find(id="lblDetailsJobControlNumber").get_text().strip() if soup.find(id="lblDetailsJobControlNumber") else 'Not Specified',
            'Job URL': f'https://calcareers.ca.gov/CalHrPublic/Jobs/JobPosting.aspx?JobControlId={soup.find(id="lblDetailsJobControlNumber").get_text().strip()[3:]}',
            'Job Description': soup.find(class_="postingHeader").find_next('span').get_text().strip() if soup.find(class_="postingHeader") and soup.find(class_="postingHeader").find_next('span') else 'Not Specified',
            'Desirable Qualifications': soup.find(id="pnlDesirableQualifications").get_text().strip() if soup.find(id="pnlDesirableQualifications") else 'Not Specified',
            'Special Requirements': soup.find(id="lblSpecialRequirementText").get_text().strip() if soup.find(id="lblSpecialRequirementText") else 'Not Specified'
        }
    return [parse_job(job) for job in jobs]

def load_to_csv(jobs):
    csv_file = 'calcareers_jobs.csv'
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=jobs[0].keys())
        writer.writeheader()
        writer.writerows(jobs)
    print(f"Job data has been written to {csv_file}")

def load_to_sqlite(jobs):
    conn = sqlite3.connect('calcareers_jobs.db')
    cursor = conn.cursor()

    if FRESH_DB:
        cursor.execute("drop table if exists jobs")

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY,
            Classification TEXT,
            Working_Title TEXT DEFAULT 'Not Specified',
            Department TEXT,
            Final_Filing_Date TEXT,
            Salary TEXT,
            Location TEXT,
            Job_Control_Number TEXT UNIQUE,
            Job_URL TEXT,
            Job_Description TEXT,
            Desirable_Qualifications TEXT,
            Special_Requirements TEXT,
            Status TEXT DEFAULT 'none' -- thumbs up: 'up', thumbs down: 'down', no vote: 'none'
        )
    ''')

    for job in jobs:
        try:
            cursor.execute('''
                INSERT INTO jobs (
                    Classification,
                    Working_Title,
                    Department,
                    Final_Filing_Date,
                    Salary,
                    Location,
                    Job_Control_Number,
                    Job_URL,
                    Job_Description,
                    Desirable_Qualifications,
                    Special_Requirements
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (job['Classification'], job['Working Title'], job['Department'], job['Final Filing Date'], job['Salary'], job['Location'], job['Job Control Number'], job['Job URL'], job['Job Description'], job['Desirable Qualifications'], job['Special Requirements']))
        except sqlite3.IntegrityError:
            print(f"Job Control Number {job['Job Control Number']} already exists in the database. Skipping this entry.")

    conn.commit()
    conn.close()
    print("Job data has been written to calcareers_jobs.db")
def load(jobs):
    load_to_csv(jobs)
    load_to_sqlite(jobs)

if __name__ == "__main__":
    print("Scraping data...")
    job_data = load_job_data()
    print("Transforming data...")
    transformed_data = transform(job_data)
    print("Saving transformed data...")
    load(transformed_data)
