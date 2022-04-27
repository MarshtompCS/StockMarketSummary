from process_posts import parse_post, filter_this_post, save_post
from bs4 import BeautifulSoup
import pickle
import os
import time
from tqdm import tqdm

RAW_POSTS_PAGE_DIR = "./posts_raw_data_page"
POSTS_DIR = "./posts_data"
BASE_URL = "https://www.55188.com/"

previous_raw_data = []
files = os.listdir(RAW_POSTS_PAGE_DIR)
for file in files:
    raw_data = pickle.load(open(os.path.join(RAW_POSTS_PAGE_DIR, file), 'rb'))
    if raw_data in previous_raw_data:
        continue
    else:
        previous_raw_data.append(raw_data)

    soup = BeautifulSoup(raw_data, "lxml")

    post_list = soup.select("#threadlisttableid > article")
    parsed_posts = []
    post_htmls = []
    tmp_bar = tqdm(total=(len(post_list)), leave=True)
    for post in post_list:
        tmp_bar.update(1)
        post_time = post.find("div", "cell2").find("time").text
        hour = int(post_time[-5:-3])
        if hour < 14:
            continue
            # too early
        post_cell = post.find("div", "cell1").find("a", "subject")
        post_url = BASE_URL + post_cell["href"]
        post_title = post_cell["title"]
        if not filter_this_post(post_title):
            time.sleep(1)
            parsed_post, post_html = parse_post(post_url)

            year_month_day = parsed_post["time"].strftime("%Y-%m-%d")
            parsed_posts.append(parsed_post)
            post_htmls.append(post_html)

    for j in range(len(parsed_posts)):
        save_post(parsed_posts[j], post_htmls[j])
