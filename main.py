import os
import asyncio
import re
import functions_framework
from google.cloud import bigquery
from wreq import Client, Emulation

# ==========================================
# 1. CONFIGURATION
# ==========================================
ROLES = ["data analyst", "data scientist", "data engineer"]
MAX_JOBS_PER_SEARCH = 150 

PROJECT_ID = "glints-492112" 
BQ_TABLE_ID = f"{PROJECT_ID}.glints_data.job_listings_v3" # Update table name if needed!

# Added createdAt and updatedAt back to the query!
GRAPHQL_QUERY = """query searchJobsV3($data: JobSearchConditionInput!) { 
  searchJobsV3(data: $data) { 
    jobsInPage { 
      id title createdAt updatedAt educationLevel workArrangementOption 
      company { name industry { name } } 
      location { name formattedName latitude longitude parents { name administrativeLevelName level } } 
      hierarchicalJobCategory { name level parents { name level } }
      salaries { salaryType minAmount maxAmount CurrencyCode } 
      skills { skill { name } } 
    } 
    hasMore 
  } 
}"""

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================
def clean_num(value):
    """Converts missing or 'N/A' values to None for BigQuery FLOAT columns."""
    if value == "N/A" or value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

# ==========================================
# 3. CORE SCRAPER
# ==========================================
async def fetch_glints_data():
    client = Client(emulation=Emulation.Chrome134)
    all_jobs = []
    
    cookie_str = os.environ.get("GLINTS_COOKIE", "")
    if not cookie_str:
        print("WARNING: GLINTS_COOKIE environment variable is empty!")

    url = "https://glints.com/api/v2-alc/graphql?op=searchJobsV3"
    headers = {
        "Origin": "https://glints.com",
        "Cookie": cookie_str,
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Referer": "https://glints.com/id/en/opportunities/jobs/explore"
    }

    for role in ROLES:
        print(f"[*] Fetching '{role}'...")
        scraped = 0
        page = 1
        has_more = True

        while has_more and scraped < MAX_JOBS_PER_SEARCH:
            variables = {
                "CountryCode": "ID", "SearchTerm": role, 
                "includeExternalJobs": True, "lastUpdatedAtRange": "PAST_24_HOURS",
                "pageSize": 30, "page": page
            }
            payload = {"operationName": "searchJobsV3", "variables": {"data": variables}, "query": GRAPHQL_QUERY}
            
            response = await client.post(url, json=payload, headers=headers)
            async with response:
                if not response.status.is_success():
                    break
                    
                data = await response.json()
                search_data = data.get("data", {}).get("searchJobsV3", {})
                jobs_in_page = search_data.get("jobsInPage", [])
                
                if not jobs_in_page: break
                
                jobs_to_add = jobs_in_page[:MAX_JOBS_PER_SEARCH - scraped]
                
                for j in jobs_to_add:
                    # URL
                    raw_title = j.get('title', 'job')
                    slug = re.sub(r'[^a-z0-9]+', '-', raw_title.lower()).strip('-')
                    j['job_url'] = f"https://glints.com/id/en/opportunities/jobs/{slug}/{j['id']}"
                    
                    # Industry
                    ind = j.get('company', {}).get('industry')
                    j['industry_name'] = ind.get('name') if ind else None

                    # Category
                    j['cat_lvl_1'], j['cat_lvl_2'], j['cat_lvl_3'] = None, None, None
                    hierarchy = j.get('hierarchicalJobCategory')
                    if hierarchy:
                        j['cat_lvl_3'] = hierarchy.get('name')
                        for parent in hierarchy.get('parents', []):
                            if parent.get('level') == 1: j['cat_lvl_1'] = parent.get('name')
                            elif parent.get('level') == 2: j['cat_lvl_2'] = parent.get('name')

                    # Location
                    loc = j.get('location') or {}
                    j['loc_name'] = loc.get('formattedName') or loc.get('name')
                    j['latitude'] = loc.get('latitude')
                    j['longitude'] = loc.get('longitude')
                    j['city'], j['province'], j['country'] = None, None, None
                    
                    for parent in loc.get('parents', []):
                        admin_level = parent.get('administrativeLevelName', '')
                        lvl = parent.get('level')
                        if admin_level in ['City', 'Kabupaten'] or lvl == 3: j['city'] = parent.get('name')
                        elif admin_level == 'Province' or lvl == 2: j['province'] = parent.get('name')
                        elif admin_level == 'Country' or lvl == 1: j['country'] = parent.get('name')

                    # Salaries
                    j['basic_min'], j['basic_max'], j['bonus_min'], j['bonus_max'] = None, None, None, None
                    j['currency'] = None
                    for sal in (j.get('salaries') or []):
                        j['currency'] = sal.get('CurrencyCode')
                        if sal.get('salaryType') == 'BASIC':
                            j['basic_min'] = sal.get('minAmount')
                            j['basic_max'] = sal.get('maxAmount')
                        elif sal.get('salaryType') == 'BONUS':
                            j['bonus_min'] = sal.get('minAmount')
                            j['bonus_max'] = sal.get('maxAmount')

                    # Skills
                    raw_skills = j.get('skills') or []
                    skill_names = [s.get('skill', {}).get('name') for s in raw_skills if s.get('skill')]
                    j['extracted_skills'] = ", ".join(skill_names) if skill_names else None

                all_jobs.extend(jobs_to_add)
                scraped += len(jobs_to_add)
                has_more = search_data.get("hasMore", False)
                page += 1
            await asyncio.sleep(0.5)

    return all_jobs

# ==========================================
# 4. BIGQUERY INSERT
# ==========================================
def insert_into_bigquery(jobs):
    if not jobs:
        return "No jobs to insert."

    bq_client = bigquery.Client()
    
    rows_to_insert = []
    for job in jobs:
        row = {
            "job_id": job.get("id"),
            "title": job.get("title"),
            "company_name": job.get("company", {}).get("name"),
            "industry": job.get("industry_name"),
            "category_lvl_1": job.get("cat_lvl_1"),
            "category_lvl_2": job.get("cat_lvl_2"),
            "category_lvl_3": job.get("cat_lvl_3"),
            "location_name": job.get("loc_name"),
            "city": job.get("city"),
            "province": job.get("province"),
            "country": job.get("country"),
            "latitude": clean_num(job.get("latitude")),
            "longitude": clean_num(job.get("longitude")),
            "work_arrangement": job.get("workArrangementOption"),
            "education": job.get("educationLevel"),
            "basic_min_salary": clean_num(job.get("basic_min")),
            "basic_max_salary": clean_num(job.get("basic_max")),
            "bonus_min": clean_num(job.get("bonus_min")),
            "bonus_max": clean_num(job.get("bonus_max")),
            "currency": job.get("currency"),
            "skills": job.get("extracted_skills"),
            "job_url": job.get("job_url"),
            "created_at": job.get("createdAt"),  # Added back!
            "updated_at": job.get("updatedAt")   # Added back!
        }
        rows_to_insert.append(row)

    errors = bq_client.insert_rows_json(BQ_TABLE_ID, rows_to_insert)
    if errors:
        raise Exception(f"Encountered errors while inserting rows: {errors}")
    return f"Successfully inserted {len(rows_to_insert)} rows."

# ==========================================
# 5. GOOGLE CLOUD ENTRY POINT
# ==========================================
@functions_framework.http
def run_scraper_cron(request):
    try:
        jobs = asyncio.run(fetch_glints_data())
        result = insert_into_bigquery(jobs)
        print(result)
        return result, 200
    except Exception as e:
        print(f"Error: {str(e)}")
        return f"Error: {str(e)}", 500