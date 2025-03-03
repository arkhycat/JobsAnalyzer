This is a tool I made to make my personal job search easier. \
I am using a modified version of [linkedinscraper](https://github.com/cwwmbm/linkedinscraper) as a starting point for the interface and DB integration and [JobSpy](https://github.com/cullenwatson/JobSpy) for the scraping. \
To run: 
1. Fill in the proxy config
2. Execute all in scrape_jobs.ipynb
3. Run csv_to db.py
4. Run app.py

### TODO:
- Sort the jobs by interesting and the dates
- Add filters for remote/hybrid and countries
- Add custom LLM (probably running GCP/AWS) to
  -   extract requirements
  -   create cover letters
  -   compare with a resume and find things to improve on
