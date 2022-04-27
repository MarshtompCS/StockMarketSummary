from process_posts import parse_post_list, save_post
import os
import time
from tqdm import tqdm
from datetime import timedelta, datetime

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


def main():
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

    yesterday = datetime.today() + timedelta(-2)
    yesterday_format = yesterday.strftime('%Y-%m-%d')
    pbar = tqdm(total=MAX_PAGE_PER_DAY)
    parse_list_url = BASE_URL + 'forum-8-t2.html'
    parsed_posts, post_htmls, total_posts, post_per_day = parse_post_list(require_day=yesterday_format,
                                                                          post_list_url=parse_list_url,
                                                                          previous_ids=previous_ids,
                                                                          total_posts=total_posts,
                                                                          post_per_day=post_per_day)
    pbar.set_description('TOTAL %d' % total_posts)
    pbar.update(1)
    for j in range(len(parsed_posts)):
        save_post(parsed_posts[j], post_htmls[j])
    print(yesterday_format, ":", post_per_day[yesterday_format])
    for i in range(2, MAX_PAGE_PER_DAY):
        parse_list_url = BASE_URL + 'forum-8-t2-' + str(i) + '.html'
        parsed_posts, post_htmls, total_posts, post_per_day = parse_post_list(require_day=yesterday_format,
                                                                              post_list_url=parse_list_url,
                                                                              previous_ids=previous_ids,
                                                                              total_posts=total_posts,
                                                                              post_per_day=post_per_day)
        for j in range(len(parsed_posts)):
            save_post(parsed_posts[j], post_htmls[j])
        pbar.set_description('TOTAL %d' % total_posts)
        pbar.update(1)
        print(yesterday_format, ":", post_per_day[yesterday_format])
        if post_per_day[yesterday_format] >= MAX_POST_PER_DAY:
            break
    print(yesterday_format, ":", post_per_day[yesterday_format])

    pass


if __name__ == '__main__':
    main()
