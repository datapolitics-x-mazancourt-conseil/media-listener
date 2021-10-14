# %% Init
import requests
from urllib.parse import urlparse
import os
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from tweet import TweetMedia

from dotenv import load_dotenv
load_dotenv()
    
#%% Fonctions utiles
def bearer_oauth(r):
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r

def connect_to_endpoint(url, params):
    response = requests.get(url, auth=bearer_oauth, params=params)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def is_url_media_domain(url, media):
    url_domain = urlparse(url).netloc.strip("www.").lower()
    media_domain = urlparse(media["website"]).netloc.strip("www.").lower()
    return (url_domain == media_domain)

def get_full_urls(item):
    if "entities" in item and "urls" in item["entities"]:
        urls = []
        for url in item["entities"]["urls"]:
            if "unwound_url" in url:
                urls.append(url["unwound_url"])
            elif "expanded_url" in url:
                urls.append(url["expanded_url"])
        if len(urls) > 0:
            return urls
    return None

#%% Initialisation
with open("medias.json") as f:
    medias = json.load(f)

bearer_token = os.environ.get("BEARER_TOKEN_TESTS")
url_twitter = "https://api.twitter.com/2/tweets/search/recent"
output_file = r"data/tweets_medias.csv"

keys = [
    "username","author_id","conversation_id","created_at","hashtags",
    "mentions","like_count","quote_count","reply_count", "retweet_count",
    "retweet","reply","quote","reply_settings", "source","text"
]

counter = 0

#%% Extraction des tweets
logging.info("Début d'exécution")

for media in medias:
    logging.info("Requesting data for twitter account : {}".format(media["twitter"]))

    time.sleep(1) # Sécurité pour la rate limit de 2 query/s

    query_string = '(from:{media_username})'.format(media_username=media["twitter"])

    # on recherche les tweets des dernières 1h05 (soit 1h + petite sécurité)
    # date must be YYYY-MM-DDTHH:mm:ssZ (ISO 8601/RFC 3339)

    start_time = (datetime.now(timezone.utc) - timedelta(hours=1, minutes=5)).isoformat()
    query_params = {
        'query': query_string,
        'start_time': start_time,
        'max_results': 100,
        'tweet.fields': 'attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,reply_settings,source,text,withheld',
    }
    
    # pour chaque média, on fait la requête à l'API Twitter
    json_response = connect_to_endpoint(url_twitter, query_params)

    # on mape les données pour sortir un format à plat
    if('data' in json_response):
        tweets = list()
        for item in json_response["data"]:

            current_tweet = TweetMedia()
            current_tweet.id = item["id"]
            current_tweet.username = media["twitter"]
            current_tweet.author_id = item["author_id"]
            current_tweet.conversation_id = item["conversation_id"]
            current_tweet.published = item["created_at"]
            current_tweet.hashtags = [hashtag["tag"] for hashtag in item["entities"]["hashtags"]] if ("entities" in item and "hashtags" in item["entities"]) else None
            current_tweet.mentions = [mention["username"] for mention in item["entities"]["mentions"]] if ("entities" in item and "mentions" in item["entities"]) else None
            current_tweet.like_count = item["public_metrics"]["like_count"]
            current_tweet.quote_count = item["public_metrics"]["quote_count"]
            current_tweet.reply_count = item["public_metrics"]["reply_count"]
            current_tweet.retweet_count = item["public_metrics"]["retweet_count"]
            current_tweet.retweet = (True if any(ref_tweet["type"]=="retweeted" for ref_tweet in item["referenced_tweets"]) else False) if 'referenced_tweets' in item else False
            current_tweet.reply = (True if any(ref_tweet["type"]=="replied" for ref_tweet in item["referenced_tweets"]) else False) if 'referenced_tweets' in item else False
            current_tweet.quote = (True if any(ref_tweet["type"]=="quoted" for ref_tweet in item["referenced_tweets"]) else False) if 'referenced_tweets' in item else False
            current_tweet.reply_settings = item["reply_settings"]
            current_tweet.source = item["source"]
            current_tweet.full_text = item["text"]
            url_list = get_full_urls(item)
            current_tweet.article_links = [url for url in url_list if is_url_media_domain(url, media)] if url_list else None

            #current_tweet.save()
            tweets.append(current_tweet.to_dict())
            counter += 1
    else:
        logging.info("Aucun tweet pour le média {}".format(media["twitter"]))

    with open("data/test.json", "w") as fp:
        json.dump(tweets, fp)

logging.info("Fin d'exécution")

print(counter)
# %%
