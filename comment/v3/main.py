# main.py
from crawl_comments import crawl_comments_for_post
if __name__ == "__main__":
    page_url = "https://www.facebook.com/tranbobg79/posts/pfbid0bcx41ydRjYi55EmGTjYUnnvckzxqAvmmpsBFQdxAVdKiyrvLgh87tyAyjPGk87KUl?rdid=x4mrINHWSbMQi2Mq#"
    COOKIES_PATH = r"E:\NCS\fb-selenium\database\facebookaccount\authen_tranhoangdinhnam\cookies.json"


    rows = crawl_comments_for_post(
        page_url=page_url,
        cookies_path=COOKIES_PATH,
        max_rounds=10000,
        sleep_between_rounds=1.5,
        headless=False,
        out_path="comments.ndjson",  # ví dụ
    )