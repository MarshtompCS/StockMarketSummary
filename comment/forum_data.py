import json
import pickle
import os
from tqdm import tqdm
import logging
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter
import matplotlib as mpl

# snssns.set()
from comment.tushare_utils import get_stock_basic, get_industry_basic

if not os.path.exists("./data"):
    os.chdir("../")

POSTS_DIR = "./data/posts_data"
RAW_POST_DIR = "./data/posts_raw_data"

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
    level=logging.INFO,
)

mpl.rcParams["axes.unicode_minus"] = False
plt.style.use('seaborn-paper')
mpl.rcParams["font.sans-serif"] = ["Calibri"]
plt.grid(True)


def draw_1dim_distribution(save_name):
    x = pickle.load(open(f"./pictures/{save_name}.pkl", 'rb'))
    y = []
    for i in range(len(x)):
        y.append(0)
    # plt.scatter(x, y, edgecolors='red')
    # plt.xlim(0, 1)
    # plt.ylim(-1, 1)
    sns.displot(x)  # 左图
    # sns.distplot(x, hist=False)  # 中图
    plt.savefig(f'./pictures/{save_name}.pdf')


def draw_entity_line(save_name):
    # stock_entity_num = pickle.load(open("./pictures/stock_entity_num_split500.pkl", 'rb'))
    # industry_entity_num = pickle.load(open("./pictures/industry_entity_num.pkl", 'rb'))
    target_entity_num = pickle.load(open(f"./pictures/{save_name}.pkl", 'rb'))
    a = Counter(target_entity_num)
    s = sorted(a.items(), key=lambda x: x[0])
    cur_nums = 0
    x = []
    y = []
    for entity_num, post_num in s:
        x.append(entity_num)
        cur_nums += post_num
        y.append(cur_nums)
    y = [i / y[-1] for i in y]
    y = [i for i in y if i < 0.993]
    x = x[:len(y)]
    plt.plot(x, y, marker="o", label=f"accumulative {save_name}")
    plt.savefig(f"./pictures/accumulative {save_name}.pdf")


def draw_text_length(save_name="text_post_lengths"):
    industry_entity_num = pickle.load(open(f"./pictures/{save_name}.pkl", 'rb'))
    a = Counter(industry_entity_num)
    s = sorted(a.items(), key=lambda x: x[0])
    cur_nums = 0
    x = []
    y = []
    for entity_num, post_num in s:
        x.append(entity_num)
        cur_nums += post_num
        y.append(cur_nums)
    y = [i / y[-1] for i in y]
    plt.plot(x, y, marker="o", label="accumulative text_post_lengths")
    plt.savefig(f"./pictures/{save_name}.pdf")


def extract_marked_entities(post, stock_list, industry_list):
    stock_entity = []
    industry_entity = []
    for i in stock_list:
        if i in post:
            stock_entity.append(i)
    for i in industry_list:
        if i in post:
            industry_entity.append(i)

    return {
        "stock_entity": stock_entity,
        "stock_entity_num": len(stock_entity),
        "industry_entity": industry_entity,
        "industry_entity_num": len(industry_entity),
    }


def chinese_percentage(text) -> float:
    # 汉字unicode:
    # \u4e00 ~ \u9fa5
    chinese_num = len([i for i in text if u'\u4e00' <= i <= u'\u9fa5'])
    percent = chinese_num / len(text)
    return percent


def ana1():
    #     pickle.dump(stock_entity_num, open("stock_entity_num.pkl", 'wb'))
    #     pickle.dump(industry_entity_num, open("industry_entity_num.pkl", 'wb'))
    stock_basic = get_stock_basic()
    industry_basic = get_industry_basic()
    stock_list = set(stock_basic.name)
    industry_list = set(industry_basic.name)
    posts_data = []
    previous_files = os.listdir(POSTS_DIR)
    cnt = 0
    cpercent = []
    stock_entity_num = []
    industry_entity_num = []
    text_lengths = []

    max_length = 500

    for filename in tqdm(previous_files):
        filepath = os.path.join(POSTS_DIR, filename)
        cur_post = json.load(open(filepath, 'r', encoding='utf-8'))
        # cur_post = pickle.load(open(filepath, 'rb'))

        # if len(cur_post["text_post"]) < 64 or len(cur_post["text_post"]) > 2048:
        #     continue
        #
        # if int(cur_post["time"][:4]) <= 2019:
        #     continue
        #
        # cur_post["text_post"] = cur_post["text_post"][:max_length]
        #
        # text_lengths.append(len(cur_post["text_post"]))
        #
        # # add entities
        # res = extract_marked_entities(cur_post["text_post"], stock_list, industry_list)
        # stock_entity_num.append(len(res['stock_entity']))
        # industry_entity_num.append(len(res['industry_entity']))
        #
        # cur_post["stock_entity"] = res["stock_entity"]
        # cur_post["industry_entity"] = res["industry_entity"]
        #
        # cur_post["stock_entity_num"] = len(res["stock_entity"])
        # cur_post["industry_entity_num"] = len(res["industry_entity"])
        #
        # # add chinese percentage
        # cp = chinese_percentage(cur_post["text_post"])
        # cpercent.append(cp)
        # cur_post["chinese_char_percentage"] = cp

        posts_data.append(cur_post)

    print(cnt)
    print(f"posts num: {len(posts_data)}")
    # pickle.dump(text_lengths, open("./pictures/text_post_lengths.pkl", "wb"))
    # draw_text_length()

    # pickle.dump(cpercent, open("./pictures/chinese_char_percent_split500.pkl", 'wb'))
    # draw_1dim_distribution("chinese_char_percent_split500")
    # draw_1dim_distribution(stock_entity_num, "stock_entity_num")
    # draw_1dim_distribution(industry_entity_num, "industry_entity_num")
    # pickle.dump(stock_entity_num, open("./pictures/stock_entity_num_split500.pkl", 'wb'))
    # pickle.dump(industry_entity_num, open("./pictures/industry_entity_num_split500.pkl", 'wb'))
    #
    # draw_entity_line("stock_entity_num_split500")
    # draw_entity_line("industry_entity_num_split500")

    pickle.dump(posts_data, open("./0512_post_data_split500.pkl", 'wb'))


def draw():
    # draw_entity_line("stock_entity_num_split500")

    target_entity_num = pickle.load(open(f"./pictures/stock_entity_num_split500.pkl", 'rb'))
    a = Counter(target_entity_num)
    s = sorted(a.items(), key=lambda x: x[0])
    cur_nums = 0
    x = []
    y = []
    for entity_num, post_num in s:
        x.append(entity_num)
        cur_nums += post_num
        y.append(cur_nums)
    y = [i / y[-1] for i in y]
    y = [i for i in y if i < 0.993]
    x = x[:len(y)]
    len_y = len(y)

    plt.plot(x, y, marker="o", label=f"accumulative stock entity")

    target_entity_num = pickle.load(open(f"./pictures/industry_entity_num_split500.pkl", 'rb'))
    a = Counter(target_entity_num)
    s = sorted(a.items(), key=lambda x: x[0])
    cur_nums = 0
    x = []
    y = []
    for entity_num, post_num in s:
        x.append(entity_num)
        cur_nums += post_num
        y.append(cur_nums)
    y = [i / y[-1] for i in y]
    y = y[:len_y]
    x = x[:len_y]

    plt.plot(x, y, marker="o", label=f"accumulative industry entity")

    plt.legend(['accumulative stock entity', 'accumulative industry entity'])

    plt.savefig(f"./pictures/accumulative stock and industry split500.pdf")

    # draw_entity_line("industry_entity_num_split500")


def process1():
    stock_basic = get_stock_basic()
    industry_basic = get_industry_basic()
    stock_list = set(stock_basic.name)
    industry_list = set(industry_basic.name)
    # if not os.path.exists("./tmp/all_processed_data.pkl"):
    #     posts_data = []
    #     previous_files = os.listdir(POSTS_DIR)
    #     for filename in tqdm(previous_files):
    #         filepath = os.path.join(POSTS_DIR, filename)
    #         cur_post = json.load(open(filepath, 'r', encoding='utf-8'))
    #         # cur_post = pickle.load(open(filepath, 'rb'))
    #
    #         if len(cur_post["text_post"]) == 0:
    #             continue
    #
    #         res = extract_marked_entities(cur_post["text_post"], stock_list, industry_list)
    #         # len(res['stock_entity'])
    #         # len(res['industry_entity'])
    #         cur_post.update(res)
    #         cp = chinese_percentage(cur_post["text_post"])
    #         cur_post["chinese_char_percentage"] = cp
    #         posts_data.append(cur_post)
    #     pickle.dump(posts_data, open("./tmp/all_processed_data.pkl", 'wb'))
    # else:
    #     posts_data = pickle.load(open("./tmp/all_processed_data.pkl", 'rb'))
    posts_data = pickle.load(open("./0512_post_data_split500.pkl", 'rb'))

    filtered_post = []
    ccp_fil_n = 0
    sen_fil_n = 0
    ien_fil_n = 0
    for post in posts_data:
        stock_entity_num = post["stock_entity_num"]
        industry_entity_num = post["industry_entity_num"]
        chinese_char_percentage = post["chinese_char_percentage"]

        flag = True
        if chinese_char_percentage < 0.72:
            ccp_fil_n += 1
            flag = False

        if stock_entity_num > 27:
            sen_fil_n += 1
            flag = False

        if industry_entity_num > 15:
            ien_fil_n += 1
            flag = False

        if flag:
            filtered_post.append(post)

    print(f"after filtering num: {len(filtered_post)}, save percent: {len(filtered_post) / len(posts_data):.5f} \n"
          f"ccp_fil_n: {ccp_fil_n}, {ccp_fil_n / len(posts_data) :.5f} \n"
          f"sen_fil_n: {sen_fil_n}, {sen_fil_n / len(posts_data) :.5f} \n"
          f"ien_fil_n: {ien_fil_n}, {ien_fil_n / len(posts_data) :.5f} \n")

    # pickle.dump(filtered_post, open("./0505_filtered_posts.pkl", 'wb'))
    pickle.dump(filtered_post, open("./0512_filtered_posts.pkl", 'wb'))


def process2():
    posts_data = pickle.load(open("./0505_filtered_posts.pkl", 'rb'))
    text_lengths = []
    for cur_post in posts_data:
        if len(cur_post["text_post"]) <= 1022:
            text_lengths.append(len(cur_post["text_post"]))
        # print("--" * 100)
        # print(i)
        # print("--" * 100)

    # len_name = "text_post_lengths_0505_filtered"
    # pickle.dump(text_lengths, open(f"./pictures/{len_name}.pkl", "wb"))
    # draw_text_length(len_name)
    len_name = "text_post_lengths_0505_filtered_split"
    pickle.dump(text_lengths, open(f"./pictures/{len_name}.pkl", "wb"))
    draw_text_length(len_name)


if __name__ == '__main__':
    ana1()
    process1()

    # process2()
    # draw()
