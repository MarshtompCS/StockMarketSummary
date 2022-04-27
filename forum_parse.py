from process_posts import parse_post_list, save_post
import os
from tqdm import tqdm

# 爬取帖子来源：理想论坛 > 实战交流 > 看盘
# 帖子将作为点评文本生成模型的粗粒度预训练语料

BASE_URL = "https://www.55188.com/"
POSTS_DIR = "./data/posts_data"
RAW_POSTS_DIR = "./data/posts_raw_data"
MAX_POST_PER_DAY = 100


def create_dir_env():
    if not os.path.exists(POSTS_DIR):
        os.mkdir(POSTS_DIR)
    if not os.path.exists(RAW_POSTS_DIR):
        os.mkdir(RAW_POSTS_DIR)


def main():
    # 1. get the id of previous saved posts
    # 2. set several ``parse_list_url`` and parse posts
    # 3. check the parsed posts whether had saved previous
    total_posts = 0
    post_per_day = {}
    # previous_files = os.listdir(POSTS_DIR)
    # previous_ids = []
    # for file in previous_files:
    #     id = file.split('_')[-1].split('.')[0]
    #     previous_ids.append(id)
    #     year_month_day = file.split('_')[0]
    #     if year_month_day not in POST_PER_DAY_DICT.keys():
    #         POST_PER_DAY_DICT[year_month_day] = 1
    #         TOTAL_NUM += 1
    #     else:
    #         if POST_PER_DAY_DICT[year_month_day] >= MAX_POST_PER_DAY:
    #             continue
    #         else:
    #             POST_PER_DAY_DICT[year_month_day] += 1
    #             TOTAL_NUM += 1

    pbar = tqdm(total=1557)
    parse_list_url = BASE_URL + 'forum-8-t2.html'
    parsed_posts, post_htmls, total_posts, post_per_day = parse_post_list(require_day=None,
                                                                          post_list_url=parse_list_url,
                                                                          previous_ids=[],
                                                                          total_posts=total_posts,
                                                                          post_per_day=post_per_day)
    pbar.set_description('TOTAL %d' % total_posts)
    pbar.update(1)
    for j in range(len(parsed_posts)):
        save_post(parsed_posts[j], post_htmls[j])
    pbar.update(1)

    for i in range(2, 1558):
        parse_list_url = BASE_URL + 'forum-8-t2-' + str(i) + '.html'
        parsed_posts, post_htmls, total_posts, post_per_day = parse_post_list(require_day=None,
                                                                              post_list_url=parse_list_url,
                                                                              previous_ids=[],
                                                                              total_posts=total_posts,
                                                                              post_per_day=post_per_day)
        pbar.update(1)
        for j in range(len(parsed_posts)):
            save_post(parsed_posts[j], post_htmls[j])
        pbar.set_description('TOTAL %d' % total_posts)
        pbar.update(1)
    print(post_per_day)
    # it is better to ensure the collected posts equally distributed by day.
    pass


if __name__ == '__main__':
    main()
