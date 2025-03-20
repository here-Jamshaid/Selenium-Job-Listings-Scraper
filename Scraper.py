import os 
import logging
import schedule
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:6999@localhost/jobs_db'  
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# Define Job model
class Job(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    title = db.Column(db.String(100), nullable=False)
    company = db.Column(db.String(100), nullable=False)
    location = db.Column(db.String(100), nullable=False)
    description = db.Column(db.Text, nullable=False)
    Category = db.Column(db.String(100), nullable=False)
    created_at = db.Column(db.DateTime(timezone=True), default=datetime.utcnow)

# Configure logging
logging.basicConfig(filename="scraper.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Configure Selenium WebDriver
options = Options()
options.add_argument("--headless")  
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920,1080")
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

# Function to convert "13h ago" into actual datetime
def parse_time(time_text):
    now = datetime.utcnow()
    if "h ago" in time_text:
        hours = int(time_text.split("h")[0])
        return now - timedelta(hours=hours)
    elif "d ago" in time_text:
        days = int(time_text.split("d")[0])
        return now - timedelta(days=days)
    elif "m ago" in time_text:
        minutes = int(time_text.split("m")[0])
        return now - timedelta(minutes=minutes)
    return now  # Default to now if format is unknown

# Function to extract job descriptions
def get_description(url):
    driver.get(url)
    time.sleep(2) 
    try:
        description_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//p[text()='Job Description']/following-sibling::ul"))
        )
        return description_element.text.strip()
    except:
        return "N/A"

# Function to extract job listings
def get_jobs(url):
    driver.get(url)
    job_list = []
    
    job_cards = driver.find_elements(By.TAG_NAME, "article")
    for job in job_cards:
        try:
            job_title = job.find_element(By.CLASS_NAME, "Job_job-card__position__ic1rc").text.strip()
        except:
            job_title = "N/A"
        
        try:
            job_company = job.find_element(By.CLASS_NAME, "Job_job-card__company__7T9qY").text.strip()
        except:
            job_company = "N/A"
        
        try:
            job_country = job.find_element(By.CLASS_NAME, "Job_job-card__country__GRVhK").text.strip()
        except:
            job_country = "N/A"
        
        try:
            posted_time = job.find_element(By.CLASS_NAME, "Job_job-card__posted-on__NCZaJ").text.strip()
            created_at = parse_time(posted_time)  # Convert relative time
        except:
            created_at = datetime.utcnow()

        try:
            parent_div = job.find_element(By.CLASS_NAME, "Job_job-card__tags__zfriA")  
            job_category = parent_div.find_element(By.CLASS_NAME, "Job_job-card__location__bq7jX").text.strip()
        except:
            job_category = "N/A"
        
        try:
            job_link = job.find_element(By.CLASS_NAME, "Job_job-page-link__a5I5g").get_attribute("href")
        except:
            job_link = "N/A"
        
        job_description = get_description(job_link) if job_link != "N/A" else "N/A"

        job_list.append({
            "title": job_title,
            "company": job_company,
            "location": job_country,
            "description": job_description,
            "category": job_category,
            "created_at": created_at  # Store parsed datetime
        })
    
    return job_list

def scrape_jobs():
    logging.info("Job scraping started.")
    base_url = "https://www.actuarylist.com/"
    
    with app.app_context():  
        for page in range(1, 20):
            url = base_url if page == 1 else f"{base_url}?page={page}"
            jobs = get_jobs(url)
            
            for job in jobs:
                existing_job = Job.query.filter_by(title=job["title"], company=job["company"], location=job["location"]).first()
                if not existing_job:
                    new_job = Job(
                        title=job["title"],
                        company=job["company"],
                        location=job["location"],
                        description=job["description"],
                        Category=job["category"],
                        created_at=job["created_at"]  # Use parsed datetime
                    )
                    db.session.add(new_job)
        
        db.session.commit()
        logging.info(f"Scraping completed and jobs saved to database.")

# Schedule scraper to run daily at 9:00 AM
schedule.every().day.at("09:00").do(scrape_jobs)

if __name__ == "__main__":
    with app.app_context():
        db.create_all() 
    
    scrape_jobs()

    while True:
        schedule.run_pending()
        time.sleep(60)
