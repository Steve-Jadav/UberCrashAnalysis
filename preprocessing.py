from crawler import Twitter
import pandas as pd
import string
import re


class Preprocessing:

    def __init__(self, file_path):
        self.uber = pd.read_csv(file_path, low_memory=False)


    def text_cleaning(self, text):

        '''
        Tweet cleaner. Returns the raw text only format of the tweet.
        Removes punctuations and urls.
        '''

        if text is None:
            return None

        # Remove the urls from the tweet, if present
        url_pattern = r'((http|ftp|https):\/\/)?[\w\-_]+(\.[\w\-_]+)+([\w\-\.,@?^=%&amp;:/~\+#]*[\w\-\@?^=%&amp;/~\+#])?'
        text = re.sub(url_pattern, '', text)

        # Remove the punctuations from the tweet. Convert the tweet into all lower case characters
        text = ''.join([char for char in text.strip() if char not in string.punctuation]).lower()
        text = text.replace('\n', ' ')

        return text


    def preprocessing_pipeline(self)->pd.DataFrame:

        '''
        Returns a processed DataFrame.
        '''

        twitter = Twitter()

        # Keep the useful columns
        self.uber = self.uber[['tweet_id', 'tweet_user_id', 'tweet_user_lang',
                 'tweet_user_location', 'tweet_user_screen_name', 'tweet_user_friends_count',
                 'tweet_user_followers_count', 'tweet_user_verified', 'tweet_created_at',
                 'tweet_text', 'tweet_truncated', 'tweet_retweeted',
                 'tweet_retweet_count', 'tweet_user_statuses_count', 'tweet_lang']]

        self.uber = self.uber[self.uber['tweet_lang'] == 'en']
        self.uber.drop_duplicates(keep='first', inplace=True)
        self.uber.reset_index(inplace=True)
        self.uber.drop(labels='index', axis=1, inplace=True)
        self.uber['tweet_user_url'] = self.uber['tweet_user_screen_name'].apply(lambda x: twitter.construct_user_url(x))
        self.uber['tweet_url'] = self.uber.apply(lambda x: twitter.construct_tweet_url(x['tweet_id'], x['tweet_user_screen_name']), axis=1)

        print ('Rehydrating tweets')
        self.uber['tweet_text'] = self.uber['tweet_id'].apply(twitter.get_tweet)
        self.uber['tweet_text'] = self.uber['tweet_text'].apply(self.text_cleaning)


        return self.uber



if __name__ == '__main__':

    # Uber7 is remaining
    print ('Reading data...')
    p = Preprocessing('data/testuberbig7.csv')
    data = p.preprocessing_pipeline()
    print ('Writing data to csv')
    data.to_csv(path_or_buf='rehydrated_data/uber7.csv')
    print ('Task completed')
