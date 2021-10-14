from elasticsearch_dsl import Document, Date, Nested, Boolean, analyzer, InnerDoc, Completion, Keyword, Text, Integer, connections
from datetime import datetime

class TweetMedia(Document):
    id = Text()
    username = Text()
    author_id = Text()
    conversation_id = Text()
    published = Date()
    hashtags = Keyword()
    mentions = Keyword()
    like_count = Integer()
    quote_count = Integer()
    reply_count = Integer()
    retweet_count = Integer()
    retweet = Boolean()
    reply = Boolean()
    quote = Boolean()
    reply_settings = Text()
    source = Text()
    full_text = Text()
    media_type = Text()
    article_links = Text()

    class Index:
        name = 'tweet-media'
        settings = {
          "number_of_shards": 2,
        }

    def save(self, ** kwargs):
        self.meta.id = self.id
        return super(TweetMedia, self).save(** kwargs)