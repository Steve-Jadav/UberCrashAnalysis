import tweepy
from config import APP_KEY, APP_SECRET
import time
import pandas as pd

class Twitter:

    def __init__(self):

        print ('Initializing twitter object.')
        self.auth = tweepy.AppAuthHandler(APP_KEY, APP_SECRET)
        self.api = tweepy.API(self.auth)
        self.counter = 0
        self.reset_time = 0

    def crawl(self, query, count):

        """
        Get the 'count' most recent tweets based on the search-query.
        However, this method only returns the tweets from the past week. It
        does not go back in time.
        """

        dataframe = pd.DataFrame()

        for tweet in self.api.search(q=query, tweet_mode='extended', lang='en', count=count):

            temp = pd.DataFrame()

            temp['tweet_id'] = [tweet.id]
            temp['tweet_user_id'] = [tweet.user.id]
            temp['tweet_user_lang'] = [tweet.user.lang]
            temp['tweet_user_location'] = [tweet.user.location]
            temp['tweet_user_screen_name'] = [tweet.user.screen_name]
            temp[ 'tweet_user_friends_count'] = [tweet.user.friends_count]
            temp['tweet_user_followers_count'] = [tweet.user.followers_count]
            temp['tweet_user_verified'] = [tweet.user.verified]
            temp['tweet_created_at'] = [tweet.created_at]
            temp['tweet_truncated'] = [tweet.truncated]
            temp['tweet_retweeted'] = [tweet.retweeted_status.retweeted]
            temp['tweet_retweet_count'] = [tweet.retweeted_status.retweet_count]
            temp['tweet_user_statuses_count'] = [tweet.user.statuses_count]
            temp['tweet_lang'] = [tweet.lang]


            # If it's a Retweet
            try:
                print (tweet.retweeted_status.full_text)
                temp['tweet_text'] = [tweet.retweeted_status.full_text]

            # Not a Retweet
            except AttributeError:
                print (tweet.full_text)
                temp['tweet_text'] = [tweet.full_text]

            except tweepy.TweepError:
                print ('Account might have been deleted.')


            pd.concat([dataframe, temp])

        return dataframe


    def get_tweet(self, tweet_id):

        '''
        Returns a tweet with the specified tweet id.
        This api method only allows upto 900 tweets per window where each window
        has a length of 15 minutes. It is also unable to retrieve tweets of users
        who have denied permission to do so.
        '''

        self.counter += 1

        # Start of a window. Window size = 15 minutes.
        # Get the time remaining for the current window to end.
        if self.counter == 1:
            self.reset_time = self.api.rate_limit_status()['resources']['lists']['/lists/statuses']['reset']

        print ('Processed {0} tweets'.format(self.counter))
        
        # If it's a retweet
        try:
            tweet = self.api.get_status(tweet_id, tweet_mode='extended')
            return tweet.retweeted_status.full_text

        # If not a retweet
        except AttributeError:
            return tweet.full_text


        except tweepy.RateLimitError:
            print ('Current rate-limit reached. Sleeping until the start of the next window...')
            remaining_time = self.reset_time - time.time()
            time.sleep(remaining_time + 3)

            # Update reset time
            self.reset_time = self.api.rate_limit_status()['resources']['lists']['/lists/statuses']['reset']

        # If the user has not authorized anybody to access tweets publicly.
        except tweepy.TweepError as err:
            print ('Not authorized. Private account.')
            return None



    def get_tweet_from_user(self, user_id):

        '''
        Returns the most recent status of the specified user_id.
        '''

        # If it's a Retweet
        try:
            status = self.api.get_status(user_id, tweet_mode='extended')
            print (status.retweeted_status.full_text)

        # Not a Retweet
        except AttributeError:
            print (status.full_text)
            print (status.created_at)

        except tweepy.RateLimitError:
            print ('Maximum limit reached in current window. Please wait for the next window.')

        except tweepy.TweepError:
            print ('No status found with that ID. Account might have been deleted.')


    def fetch_timeline(self, user_id, number_of_tweets: int = 20):

        '''
        Returns the 20 most recent statuses (not more than that) posted from the specified user.
        '''

        count = 0

        try:
            tweets = self.api.user_timeline(user_id, tweet_mode='extended', count=number_of_tweets, page=1)
            for tweet in tweets:

                print (tweet.full_text)
                print (tweet.created_at)
                print ()
                count += 1
                '''
                if tweet.created_at.year == 2018 and 'uber' in tweet.full_text.lower():
                    print (tweet.created_at)
                    temp.append(tweet.full_text)
                '''

        except tweepy.TweepError as err:
            print (err)

        print ('COUNT: ', count)


    def tweet_lookup(self, tweet_id: list):

        '''
        Returns full Tweet objects for up to 100 tweets per request, specified by the tweet_id parameter.
        Returns None if the users have protected their tweets.
        '''

        status = self.api.statuses_lookup(tweet_id)

        if len(status) == 0:
            print ('Protected account')
            return None

        return status


    def construct_user_url(self, user_screen_name):

        '''
        Returns a constructed user url.
        '''
        return f'https://twitter.com/{user_screen_name}'


    def  construct_tweet_url(self, tweet_id, user_screen_name):

        '''
        Returns a constructed tweet url.
        '''

        return f'https://twitter.com/{user_screen_name}/status/{tweet_id}'
