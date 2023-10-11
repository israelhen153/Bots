import json
from argparse import ArgumentParser
from instaloader import Instaloader
from pandas import DataFrame
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Queue, cpu_count

loader = Instaloader()
GOOD_HASHTAGS = Queue()
BAD_HASHTAGS = Queue()


class InstegramScraper:
    def __init__(self):
        self.hashes_pool = Queue()
        self.read_hashtags_file()

    def get_hashtags_posts(self):
        while self.hashes_pool.qsize() > 0:
            hashtag = self.hashes_pool.get()
            print(hashtag)
            try:
                posts = loader.get_hashtag_posts(hashtag[1])
                for index, post in zip(range(1), posts):
                    data = {
                        'Date': [post.date.strftime("%Y-%m-%d %H:%M:%S")],
                        'shortFormat': [f"https://www.instagram.com/p/{post.shortcode}"],
                        'Video URL': [post.video_url] if post.is_video else None
                    }
                    print(data)
                    if hashtag[0] == "good":
                        GOOD_HASHTAGS.put(data)
                    else:
                        BAD_HASHTAGS.put(data)

                    if index == 0:
                        break
            except Exception as e:
                pass

    def read_hashtags_file(self):
        for hashtag_type in ["bad", "good"]:
            with open(f"{hashtag_type}_hashtags.txt") as f:
                hashtags = f.read().splitlines()
                [self.hashes_pool.put([hashtag_type, hashtag_line]) for hashtag_line in hashtags]

    def get_hashtags_posts_parallel(self):
        with ThreadPoolExecutor(max_workers=cpu_count()) as executor:
            executor.submit(self.get_hashtags_posts)

        good_hashtags = bad_hashtags = []
        # Process the data
        # turn the queues to json files
        while GOOD_HASHTAGS.qsize() > 0:
            data = GOOD_HASHTAGS.get()
            good_hashtags.append(data)

        with open("instaegram_hashtags_good.json", "w") as f:
            json.dump(good_hashtags, f)

        while BAD_HASHTAGS.qsize() > 0:
            data = BAD_HASHTAGS.get()
            bad_hashtags.append(data)

        with open("instaegram_hashtags_bad.json", "w") as f:
            json.dump(bad_hashtags, f)

column_names = ['accessibility_caption', 'caption', 'caption_hashtags', 'caption_mentions', 'comments', 'date', 'date_local', 'date_utc', 'from_iphone_struct', 'from_mediaid', 'from_shortcode', 'get_comments', 'get_is_videos', 'get_likes', 'get_sidecar_nodes', 'is_pinned', 'is_sponsored', 'is_video', 'likes', 'location', 'mediacount', 'mediaid', 'mediaid_to_shortcode', 'owner_id', 'owner_profile', 'owner_username', 'pcaption', 'profile', 'shortcode', 'shortcode_to_mediaid', 'sponsor_users', 'supported_graphql_types', 'tagged_users', 'title', 'typename', 'url', 'video_duration', 'video_url', 'video_view_count', 'viewer_has_liked']


if __name__ == "__main__":
    InstegramScraper().get_hashtags_posts_parallel()
