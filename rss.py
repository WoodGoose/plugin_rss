# encoding:utf-8

import plugins
from bridge.context import ContextType
from bridge.reply import Reply, ReplyType
from channel.chat_message import ChatMessage
from common.log import logger
from plugins import *
from config import conf

import json
import requests
import time
import threading
from datetime import datetime
from queue import Queue
import sqlite3
from collections import defaultdict


DB_FILE = "rss_items.db"

def build_db_path():
    curdir = os.path.dirname(__file__)
    db_path = os.path.join(curdir, DB_FILE)
    return db_path

def init_db():
    conn = sqlite3.connect(build_db_path())
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        rss_key TEXT,
        title TEXT,
        link TEXT UNIQUE,
        description TEXT,
        pub_date TEXT,
        insert_time TEXT
    )
    """)
    conn.commit()

def get_new_items(items):
    conn = sqlite3.connect(build_db_path())
    cursor = conn.cursor()
    links = [item['link'] for item in items]
    cursor.execute(
        "SELECT link FROM items WHERE link IN ({})".format(",".join("?" for _ in links)),
        links
    )
    existing_links = {row[0] for row in cursor.fetchall()}
    conn.close()
    return [item for item in items if item['link'] not in existing_links]

def save_item_to_db(rss_key, item):
    conn = sqlite3.connect(build_db_path())
    cursor = conn.cursor()
    try:
        cursor.execute("""
        INSERT INTO items (rss_key, title, link, description, pub_date, insert_time)
        VALUES (?, ?, ?, ?, ?, ?)
        """, (
            rss_key,
            item['title'],
            item['link'],
            item['description'],
            item['pub_date'],
            datetime.utcnow().isoformat()
        ))
        conn.commit()
    except sqlite3.IntegrityError:
        pass  # ignore duplicate item
    conn.close()


url_stats = defaultdict(lambda: {'success': 0, 'failure': 0})
def fetch_rss(urls):
    global url_stats
    def success_rate(url):
        stats = url_stats[url]
        total = stats['success'] + stats['failure']
        return stats['success'] / total if total > 0 else 0
    
    sorted_urls = sorted(urls, key=success_rate, reverse=True)

    for url in sorted_urls:
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            url_stats[url]['success'] += 1
            return response.text
        except requests.RequestException as e:
            url_stats[url]['failure'] += 1
            print(f"Failed to fetch RSS from {url}: {e}")
    return None


def parse_rss(rss_content):
    import xml.etree.ElementTree as ET
    root = ET.fromstring(rss_content)
    channel = root.find("channel")
    items = []
    for item in channel.findall("item"):
        title = item.find("title").text
        link = item.find("link").text
        description = item.find("description").text
        pub_date = item.find("pubDate").text
        items.append({
            "title": title,
            "link": link,
            "description": description,
            "pub_date": pub_date
        })
    return items


@plugins.register(
    name="rss",
    desire_priority=-1,
    hidden=True,
    desc="A simple plugin that subscribe rss",
    version="0.1",
    author="fred",
)
class Rss(Plugin):
    def __init__(self):
        super().__init__()
        self.TAG = "[RSS]"

        try:
            self.config = super().load_config()
            if not self.config:
                self.config = self._load_config_template()
            
            self.threads = []
            init_db()

            self.start_rss_workers()

            logger.info(f"[{self.TAG}] inited")
        except Exception as e:
            logger.error(f"{self.TAG}init error: {e}")
            raise f"[{self.TAG}] init failed, ignore "
        
        self.channel = None
        self.channel_type = conf().get("channel_type", "wx")
        if self.channel_type == "wx":
            try:
                from lib import itchat
                self.channel = itchat
            except Exception as e:
                logger.error(f"itchat not installed: {e}")
        else:
            logger.error(f"unsupport channel_type: {self.channel_type}")


    def start_rss_workers(self):
        for entry in self.config:
            rss_catalog = entry['catalog']
            rss_key = f"{rss_catalog}_{entry['key']}"
            duration = entry['duration_in_minutes']
            urls = entry['url']
            receiver_name = entry['receiver_name']
            group_name = entry['group_name']
            thread = threading.Thread(
                target=self.rss_worker,
                args=(rss_key, rss_catalog, urls, duration, receiver_name, group_name),
                daemon=True
            )
            thread.start()
            self.threads.append(thread)

        # todo: handle this
        # for thread in threads:
        #     thread.join()

    def rss_worker(self, rss_key, rss_catalog, urls, duration, receiver_name, group_name):
        while True:
            time.sleep(duration * 60)
            logger.info(f"{self.TAG}fetching RSS for {rss_key}...")
            rss_content = fetch_rss(urls)
            if rss_content:
                items = parse_rss(rss_content)
                new_items = get_new_items(items)
                new_items = self.filter_old_items(new_items)
                for item in reversed(new_items[-3:]): # no more than N new items every period
                    if self.handle_item(rss_catalog, item, receiver_name, group_name):
                        save_item_to_db(rss_key, item)
            else:
                logger.warning(f"{self.TAG}fetch RSS fail for {rss_catalog}/{rss_key}")
    

    # keep only the items from the last 3 days
    def filter_old_items(self, items):
        from datetime import datetime, timedelta

        time_format = "%a, %d %b %Y %H:%M:%S GMT"

        current_time = datetime.utcnow()

        three_days_ago = current_time - timedelta(days=3)

        filtered_items = [
            item for item in items
            if datetime.strptime(item["pub_date"], time_format) >= three_days_ago
        ]
        return filtered_items

    # todo: ensure send succcess
    def handle_item(self, rss_catalog, item, receiver_names, group_names):
        content = self.format_item(rss_catalog, item)
        for group_name in group_names:
            self.send_msg_to_group(content, group_name)
        for receiver_name in receiver_names:
            self.send_msg_to_friend(content, receiver_name)
        return True
    
    def format_item(self, rss_catalog, item):
        import re
        text = ""
        if rss_catalog in ("zhihu", ):
            title = item['title']
            text += title + "\n" + "-" * 30 + "\n"
        text += self.format_html_text(item['description'])

        pub_date = self.convert_to_east_eight_time(item['pub_date'])
        link = item['link']

        return f"{text}\n\n{pub_date}\n{link}"

    def format_html_text(self, text):
        import re

        # <img alt="[good]" src="https://h5.sinaimg.cn/m/emoticon/icon/others/h_good-0c51afc69c.png" style="width:1em; height:1em;" referrerpolicy="no-referrer">
        text = re.sub(r'<img [^>]*alt="([^"]*)"[^>]*>', r"\1", text)
        # <img style="" src="https://tvax2.sinaimg.cn/large/008AGV4Yly1hvw0lmxzvoj31401o0e82.jpg" referrerpolicy="no-referrer">
        text = re.sub(
            r'<\s*img\s+style=".*"\s*src=".+" referrerpolicy="no-referrer">',
            "[图片]",
            text,
        )

        # <a href="https://video.weibo.com/show?fid=1034:5103567598452769" data-hide="">小米社区的微博视频</a>
        # <a href="https://m.weibo.cn/search?containerid=231522type%3D1%26t%3D10%26q%3D%23k80%23" data-hide=""><span class="surl-text">#k80#</span></a>
        text = re.sub(r"<a [^>]*>(.+?)</a>", r"<\1>", text)

        # <span class="url-icon">[作揖]</span>
        # <span class="surl-text">小米社区的微博视频</span>
        text = re.sub(r"<span [^>]*>(.+?)</span>", r"\1", text)

        # <div style="clear: both"></div>
        text = re.sub(r"<div [^>]*>(.*?)</div>", r"\1", text)

        # <video controls="controls" poster="https://tvax1.sinaimg.cn/orj480/9c9b24f5ly1hvv5ri55etj21hc0u00ul.jpg" style="width: 100%"><source src="https://f.video.weibocdn.com/o0/D2SO3GsAlx08jN130MwM01041200r8670E010.mp4?label=mp4_720p&amp;template=1280x720.25.0&amp;ori=0&amp;ps=1Cx9YB1mmR49jS&amp;Expires=1732349608&amp;ssig=1yBb1QUH1I&amp;KID=unistore,video"><source src="https://f.video.weibocdn.com/o0/2V29C0v0lx08jN12EjtK01041200et0y0E010.mp4?label=mp4_hd&amp;template=852x480.25.0&amp;ori=0&amp;ps=1Cx9YB1mmR49jS&amp;Expires=1732349608&amp;ssig=f8U9ILZkZX&amp;KID=unistore,video"><source src="https://f.video.weibocdn.com/o0/JgZhVujslx08jN12RPRe010412009nXm0E010.mp4?label=mp4_ld&amp;template=640x360.25.0&amp;ori=0&amp;ps=1Cx9YB1mmR49jS&amp;Expires=1732349608&amp;ssig=7b9GZSv54O&amp;KID=unistore,video"><p>视频无法显示，请前往<微博视频>观看。</p></video>
        text = re.sub(r"<source [^>]*>", "", text)
        # <video controls="controls" poster="https://tvax1.sinaimg.cn/orj480/9c9b24f5ly1hvv5ri55etj21hc0u00ul.jpg" style="width: 100%"><p>视频无法显示，请前往<微博视频>观看。</p></video>
        text = re.sub(r"<video [^>]*>(.+?)</video>", r"\1", text)

        # <br clear="both">
        # <br>
        text = re.sub(r"<br [^>]*>", "\n", text)
        text = re.sub(r"<br>", "\n", text)

        # <p>视频无法显示，请前往<微博视频>观看。</p>
        text = re.sub(r"<p>(.*?)</p>", r"\1\n", text)
        # <p data-pid="rIQ5HAkA">老而不死是为贼</p >
        text = re.sub(r"<p [^>]*>(.*?)</p>", r"\1\n", text)

        # <sup data-text="在宇宙尺度下，奇数原子序数的元素丰度比偶数的要少，所以下列是以偶数原子序数原子为主" data-url="" data-draft-node="inline" data-draft-type="reference" data-numero="1">[1]</sup>
        text = re.sub(r"<sup [^>]*>(.*?)</sup>", r"\1", text)

        # <b>我原本以为保守派已经天下无敌，没想到还有高手，极端保守派！</b>
        text = re.sub(r"<b>(.*?)</b>", r"**\1**", text)
        text = re.sub(r"<strong>(.*?)</strong>", r"**\1**", text)

        # <figure data-size="normal"></figure>
        text = text.replace(r'<figure data-size="normal"></figure>', "[图片]")

        # <h2>前情回顾：恒星的核聚变</h2>
        text = re.sub(r"<h1>(.*?)</h1>", r"# \1\n", text)
        text = re.sub(r"<h2>(.*?)</h2>", r"## \1\n", text)
        text = re.sub(r"<h3>(.*?)</h3>", r"### \1\n", text)

        def format_blockquote(match):
            content = match.group(1)
            formatted_lines = [
                "> " + line.strip() for line in content.splitlines() if line.strip()
            ]
            return "\n" + "\n".join(formatted_lines)

        text = re.sub(
            r"<blockquote[^>]*>(.*?)</blockquote>", format_blockquote, text, flags=re.DOTALL
        )

        text = text.replace("&nbsp;", " ")

        return text

    def convert_to_east_eight_time(self, gmt_time_str):
        from datetime import datetime, timedelta

        gmt_time = datetime.strptime(gmt_time_str, "%a, %d %b %Y %H:%M:%S GMT")
        
        east_eight_time = gmt_time + timedelta(hours=8)
        
        return east_eight_time.strftime("%Y-%m-%d %H:%M:%S")

    def send_msg_to_group(self, content, group_name):
        chatrooms = self.channel.search_chatrooms(name=group_name)
        if not chatrooms:
            logger.error(f"{self.TAG}not found group：{group_name}")
            return False
        else:
            # todo: handle duplicate group name
            chatroom = chatrooms[0]
            # todo: ensure send success
            self.channel.send(content, chatroom.UserName)
            return True
    
    def send_msg_to_friend(self, content, receiver_name):
        friends = self.channel.search_friends(remarkName=receiver_name)
        if not friends:
            friends = self.channel.search_friends(name=receiver_name)
        if not friends:
            logger.error(f"{self.TAG}not found friend: {receiver_name}")
            return False
        else:
            # todo: handle duplicate name
            friend = friends[0]
            self.channel.send(content, friend.UserName)
            return True


    def get_help_text(self, **kwargs):
        help_text = "订阅RSS消息到微信群或好友。\n"
        return help_text

    def _load_config_template(self):
        logger.debug("No config.json, use config.json.template")
        try:
            plugin_config_path = os.path.join(self.path, "config.json.template")
            if os.path.exists(plugin_config_path):
                with open(plugin_config_path, "r", encoding="utf-8") as f:
                    plugin_conf = json.load(f)
                    return plugin_conf
        except Exception as e:
            logger.exception(e)