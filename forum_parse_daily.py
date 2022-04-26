import json
import unicodedata
from typing import Dict
import bs4
from bs4 import BeautifulSoup
import requests
import re
import pickle
import os
import datetime
import time
from tqdm import tqdm

# 爬取帖子来源：理想论坛 > 实战交流 > 看盘
# 帖子将作为点评文本生成模型的粗粒度预训练语料

BASE_URL = "https://www.55188.com/"
POSTS_DIR = "./posts_data"
RAW_POSTS_DIR = "./posts_raw_data"
MAX_POST_PER_DAY = 100
MAX_PAGE_PER_DAY = 50


def create_dir_env():
    if not os.path.exists(POSTS_DIR):
        os.mkdir(POSTS_DIR)
    if not os.path.exists(RAW_POSTS_DIR):
        os.mkdir(RAW_POSTS_DIR)


def create_test_html_for_debug():
    if not os.path.exists("./test_html"):
        os.mkdir("./test_html")
        test_post_url = "https://www.55188.com/thread-10022357-1-1.html"
        html = requests.get(test_post_url)
        test_post_html = html.text
        pickle.dump(test_post_html, open("./test_html/post.html", 'wb'))
        test_post_url = "https://www.55188.com/forum-8-t2.html"  # Firs page of forum
        # https://www.55188.com/forum-8-t2-2.html # Second page's URL
        html = requests.get(test_post_url)
        test_post_html = html.text
        pickle.dump(test_post_html, open("./test_html/post_list.html", 'wb'))


def filter_this_post(post_title):
    # filter the post according to title
    # other rules ???
    title_key_words = ["复盘", "日记", "回顾", "记录", "日记"]
    for key_word in title_key_words:
        if key_word in post_title:
            return False

    return True


def save_post(post_info: Dict, raw_data, save_raw_data=True):
    save_name = f'{post_info["time"].strftime("%Y-%m-%d")}_{post_info["title"][:10]}_{post_info["id"]}'
    save_path = os.path.join(POSTS_DIR, save_name + ".json")
    save_content = {
        "id": post_info["id"],
        "title": post_info["title"],
        "time": post_info["time"].strftime("%Y-%m-%d"),
        "url": post_info["url"],
        "text_post": post_info["text_post"]
    }
    json.dump(save_content, open(save_path, "w", encoding="utf-8"), ensure_ascii=False, indent=4)

    if save_raw_data:
        raw_data_path = os.path.join(RAW_POSTS_DIR, save_name + ".pkl")
        pickle.dump(raw_data, open(raw_data_path, 'wb'))


def parse_post_list(require_day, post_list_url, previous_ids, total_posts, post_per_day):
    if post_list_url is None:
        post_list_html = pickle.load(open("./test_html/post_list.html", 'rb'))  # For debug
    else:
        post_list_html = requests.get(post_list_url)
        post_list_html = post_list_html.text
    soup = BeautifulSoup(post_list_html, "lxml")

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
            parsed_post = parse_post(post_url)
            if parsed_post['id'] in previous_ids:
                continue
                # skip the saved ids
            year_month_day = parsed_post["time"].strftime("%Y-%m-%d")
            if year_month_day != require_day:
                continue
            else:
                if year_month_day not in post_per_day.keys():
                    post_per_day[year_month_day] = 1
                    total_posts += 1
                else:
                    if post_per_day[year_month_day] >= MAX_POST_PER_DAY:
                        break
                        # save max num
                    else:
                        post_per_day[year_month_day] += 1
                        total_posts += 1
            # save_post(parsed_post, post_list_html)
            parsed_posts.append(parsed_post)
            post_htmls.append(post_list_html)
    return parsed_posts, post_htmls, total_posts, post_per_day


def parse_post(post_url=None) -> Dict:
    if post_url is None:
        post_html = pickle.load(open("./test_html/post.html", 'rb'))  # For debug
    else:
        post_html = requests.get(post_url)
        post_html = post_html.text
    soup = BeautifulSoup(post_html, "lxml")

    # Select post-list, where the first post is the main post, and the rest posts are replies.
    # The main post contains `header` node, but others do not contain `header`.
    posts = soup.select("#postlist > div")
    main_post = posts[0]
    post_id = main_post["id"]
    post_id = post_id.split('_')[-1]
    post_title = main_post.header.h2.text.strip()
    post_title = post_title.replace('/', '')
    post_time_text = main_post.find("span", "z ml_10").text
    post_time_text = post_time_text.replace("发表于", "").strip()
    post_time = datetime.datetime.strptime(post_time_text, "%Y-%m-%d %H:%M")

    post_nodes = main_post.find("div", "nei")
    text_post = build_text_post_from_nodes(post_nodes)

    return {"id": post_id, "title": post_title, "time": post_time,
            "text_post": text_post, "post_nodes": post_nodes, "url": post_url}


def build_text_post_from_nodes(post_nodes):
    text_post = ""
    for node in post_nodes:
        assert type(node) in [bs4.element.Tag, bs4.element.NavigableString, bs4.element.Comment]
        if type(node) == bs4.element.Tag:
            if node.name == "br":
                if len(text_post) > 0 and not unicodedata.category(text_post[-1]).startswith("P"):
                    # replace \n by 。 if the previous one is not a punctuation
                    text_post += "。"
        elif type(node) == bs4.element.Comment:
            continue
        else:
            node = node.strip().replace(" ", "")  # remove blanks
            text_post += node

    # Clean text_post by following rules:
    # 1. one char repeats more than 6 times will be truncated.
    text_post = re.sub(r"(.)(\1){6,}", r"\1\1\1\1\1\1", text_post, re.M)
    # 2. other rules ???

    return text_post


def main():
    # 1. get the id of previous saved posts
    # 2. set several ``parse_list_url`` and parse posts
    # 3. check the parsed posts whether had saved previous
    # 4. save the new parsed posts
    total_posts = 0
    post_per_day = {}
    previous_files = os.listdir(POSTS_DIR)
    previous_ids = []
    for file in previous_files:
        id = file.split('_')[-1].split('.')[0]
        previous_ids.append(id)
        year_month_day = file.split('_')[0]
        if year_month_day not in post_per_day.keys():
            post_per_day[year_month_day] = 1
            total_posts += 1
        else:
            if post_per_day[year_month_day] >= MAX_POST_PER_DAY:
                continue
            else:
                post_per_day[year_month_day] += 1
                total_posts += 1
    require_day = datetime.datetime.now().strftime('%Y-%m-%d')

    pbar = tqdm(total=MAX_PAGE_PER_DAY)
    parse_list_url = BASE_URL + 'forum-8-t2.html'
    parsed_posts, post_htmls, total_posts, post_per_day = parse_post_list(require_day=require_day,
                                                                          post_list_url=parse_list_url,
                                                                          previous_ids=previous_ids,
                                                                          total_posts=total_posts,
                                                                          post_per_day=post_per_day)
    pbar.set_description('TOTAL %d' % total_posts)
    pbar.update(1)
    for j in range(len(parsed_posts)):
        save_post(parsed_posts[j], post_htmls[j])
    print(require_day, ":", post_per_day[require_day])
    for i in range(2, MAX_PAGE_PER_DAY):
        parse_list_url = BASE_URL + 'forum-8-t2-' + str(i) + '.html'
        parsed_posts, post_htmls, total_posts, post_per_day = parse_post_list(require_day=require_day,
                                                                              post_list_url=parse_list_url,
                                                                              previous_ids=previous_ids,
                                                                              total_posts=total_posts,
                                                                              post_per_day=post_per_day)
        for j in range(len(parsed_posts)):
            save_post(parsed_posts[j], post_htmls[j])
        pbar.set_description('TOTAL %d' % total_posts)
        pbar.update(1)
        print(require_day, ":", post_per_day[require_day])
    print(require_day,":", post_per_day[require_day])

    pass


if __name__ == '__main__':
    # create_dir_env()
    # create_test_html_for_debug()
    # parse_post()
    # parse_post_list()
    main()
