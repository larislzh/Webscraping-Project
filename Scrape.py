#import libraries
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import time

#connect to website and pull data
pd.set_option('display.max_colwidth', None)
url = "https://sg.indeed.com/jobs?q=data%20analyst&sc=0kf%3Ajt(fulltime)%3B&fromage=14&"
page_num = 10
joblist = []
start_time = time.time()
print("Scraping begin")
while True:
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.66 Safari/537.36 Edg/103.0.1264.44'}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "lxml")
    cards = soup.find_all('div', class_='slider_item')
    for card in cards:
        job_title = card.find('a').text
        job_url = 'https://sg.indeed.com' + card.h2.a.get('href')
        company = card.find('span', 'companyName').text.strip()
        job_summary = card.find('div', 'job-snippet').text.strip().replace("\n", " ")
        day_scraped = datetime.today().strftime('%d-%m-%y')
        try:
            job_salary = card.find('div', class_='attribute_snippet').text
            job_salary.split("</svg>")[-1]
        except AttributeError:
            job_salary = ''

        job = {
            'JobTitle': job_title,
            'Company': company,
            'Extracted': day_scraped,
            'Salary': job_salary,
            'Summary': job_summary,
            'JobUrl': job_url
        }
        joblist.append(job)
    #check if next page exists, if yes. move to next page, else break from loop
    #create a timer and tracker so as to know if the code bugs, which part did it stop at
    if soup.find("a", {"aria-label": "Next"}) != None:
        url = "https://sg.indeed.com/jobs?q=data%20analyst&sc=0kf%3Ajt(fulltime)%3B&fromage=14&start={}".format(page_num)
        elapsed_time = time.time() - start_time
        print(time.strftime("%H:%M:%S since start", time.gmtime(elapsed_time)))
        print(f"Page {round(page_num / 10)} extracted")
        page_num += 10
        time.sleep(10)
    else:
        break

#create csv to store 1st part of data
df = pd.DataFrame(joblist)
df = df.astype('string')
print(f"Total jobs scraped: {len(joblist)}")
print("Part1 Clear")
df.to_csv('level1Scrape.csv')

#initiate second part to run through each individual job url
quali = []
n = 0
save_interval = 50
while n < len(df['JobUrl']):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.66 Safari/537.36 Edg/103.0.1264.44'}
    for urls in df['JobUrl']:
        print("Scraping for row #%d" % n)
        response2 = requests.get(urls)
        time.sleep(10)
        soup2 = BeautifulSoup(response2.text, "lxml")
        first = soup2.find('div', 'jobsearch-jobDescriptionText')

        try:
            job_description = first.find_all('ul')
            job_quali = ''
            for parts in job_description:
                job_quali += str(parts.get_text()).strip().replace("\n", " ")
            quali.append(job_quali)
        except TypeError:
            print("No data found")

        if n % save_interval == 0:
            df1 = pd.DataFrame(quali, columns=['Qualifications'])
            df1 = df1.astype('string')
            df1.to_csv("Savepoint.csv")
            print("Savings at rows #%d. . ." % n)

        elapsed_time = time.time() - start_time
        print(time.strftime("%H:%M:%S since start", time.gmtime(elapsed_time)))
        n += 1

df1 = pd.DataFrame(quali, columns=['Qualifications'])
df1 = df1.astype('string')
df1.to_csv("Savedf1.csv")
print("Part2 Clear")

#saving to csv for further analysis
df_final = pd.concat([df, df1], axis=1)
df_final = df_final.astype('string')
df_final.to_csv('level2Scrape.csv')

print("Scraping done")
end_time = time.time() - start_time
print(time.strftime("%H:%M:%S to complete", time.gmtime(end_time)))
