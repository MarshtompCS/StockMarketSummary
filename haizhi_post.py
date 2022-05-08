import time
from datetime import datetime
from datetime import timedelta
import pymysql
import pandas as pd
import numpy as np


def db_connection(host="haizhiouter.mysql.rds.aliyuncs.com", user="haizhi_root", passwd="Hai965310", db="ht"):
    db_conn = pymysql.connect(host=host, user=user, passwd=passwd, db=db, use_unicode=True, charset="utf8")
    return db_conn


def get_post():
    db = db_connection(db="ht")
    select_fields = ["Post_title", "Post_text", "Post_time", "Author_name", "Author_id", "Post_id", "Post_type"]
    sql_get_all_post = f"SELECT {','.join(select_fields)} FROM post_main_table WHERE Section_id=6"
    cursor = db.cursor()
    id2post = dict()
    cursor.execute(sql_get_all_post)
    for data in cursor:
        dict_data = {filed_name: data[filed_id] for filed_id, filed_name in enumerate(select_fields)}
        assert dict_data["Post_id"] not in id2post.keys()
        id2post[dict_data["Post_id"]] = dict_data
    db.close()
    return id2post


def get_follow_post(id2post: dict):
    db = db_connection(db="ht")
    select_fields = ["Post_id", "Follow_id", "Author_id", "Follow_text", "Follow_time", "Follow_type"]
    sql_get_all_follow_post = f'SELECT {",".join(select_fields)} FROM post_follow_table WHERE Follow_type="follow"'
    cursor = db.cursor()
    id2follow = dict()
    cursor.execute(sql_get_all_follow_post)
    for data in cursor:
        dict_data = {filed_name: data[filed_id] for filed_id, filed_name in enumerate(select_fields)}
        assert dict_data["Follow_id"] not in id2follow.keys()
        if dict_data["Post_id"] in id2post:
            id2follow[dict_data["Follow_id"]] = dict_data

    db.close()
    return id2follow


def main():
    id2post = get_post()
    id2follow = get_follow_post(id2post)

    # 用户名一致的跟帖视为日记贴

    print("")

    # for key in id2post:



if __name__ == '__main__':
    main()
