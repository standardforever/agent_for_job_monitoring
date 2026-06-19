from __future__ import annotations

import os
from pathlib import Path

CDP_URL = "http://localhost:9222"
TEST_URLS = [

    # "https://kinvet.careers.adp.com/vacancies",
    # "https://careers.lahproperty.co.uk/",
    # "https://www.lancingcollege.co.uk/lancing-college/about-college/vacancies/",
    # "https://latymer.ciphr-irecruit.com/applicants/vacancy",

    # "https://globalcareers.travelopia.com/Brand/LeBoat/All%20Roles",
    # "https://apply.workable.com/lenstore-dot-c-o-dot-u-k/",
    # "https://equiniti.wd3.myworkdayjobs.com/Opportunities?q=lenvi",
    # "https://ce0218li.webitrent.com/ce0218li_webrecruitment/wrd/run/ETREC179GF.open?WVID=506283GkCP",
    # "https://careers.publicisgroupe.com/leoburnettuk/jobs",
    # "https://jobs.compass-group.co.uk/levy",
    # "https://www.leyf.org.uk/careers/",
    # "https://www.lgo.org.uk/jobs/vacancies",
    # "https://careers.liaise.com/vacancies",
    # "https://www.jobs.libertatemhealthcare.co.uk/vacancies/vacancy-search-results.aspx",
    # "https://careers.libertywines.co.uk/vacancies",
    # "https://www.eteach.com/careers/lifemultiacademytrust-org/",
    # "https://lmp-group.co.uk/vacancies/",
    # "https://locality.org.uk/vacancies?vacancy_query=&vacancy_category=&vacancy_order=postDate+DESC&vacancy_page=0",
    # "https://careers.itw.com/us/en/loma",
    # "https://londongolf.co.uk/careers/",
    "https://careers.londonsport.org/vacancies"
]

# TEST_URLS = [
#     # "https://jobs.dayforcehcm.com/en-GB/kpdn/ClientCareersSiteIndeedExcluded",
#     # "https://careers.lbsbm.co.uk/vacancies",
#     # "https://careers.bouygues-construction.com/go/Bouygues-UK-EN/9560601/",
#     # "https://www.ferrovial.com/en/careers/opportunities/",
#     # "https://careers.macegroup.com/current-vacancies",
#     # "https://acadia.com/en-us/careers/job-board",
#     # "https://careers.abbvie.com/en/jobs",
#     # "https://careers.admabiologics.com/search/?q=&locationsearch=&",
#     # "https://agilent.wd5.myworkdayjobs.com/Agilent_Careers",
#     # "https://jobs.dayforcehcm.com/en-GB/kpdn/ClientCareersSiteIndeedExcluded",
#     # "https://www.snagajob.com/search?q=restaurant&radius=20&query_trigger=def_static",
#     # "https://jobgether.com/remote-jobs/united-states", # watch for exceed max token
#     # "https://www.jobsite.co.uk/jobs/admin",
#     # "https://agilent.wd5.myworkdayjobs.com/Agilent_Careers",
#     # "https://another-site.com/careers",
#     # "https://jooble.org/jobs/Denver%2C-CO"
#     # "https://apprenticeships.knightstrainingacademy.com/category/job/"
#     # "https://sthelenscollege1.talosats-careers.com/vacancies"
#     # "https://apply.workable.com/koothjobs/"
#     # "https://www.kyra.com/careers"
#     # "https://www.lblaw.co.uk/about-us/careers/job-search/"
#     # "https://remoteok.com/"
#     # "https://www.mcsgroup.jobs/jobs/"
#     "https://weworkremotely.com/"
# ]

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5-nano")
MAX_PAGINATION_TEST_PAGES = 3
MAX_REPAIR_ATTEMPTS = 3
OUTPUT_DIR = Path("pagination_output")
