from dotenv import load_dotenv
import sys
import os
import praw

def main():
    reddit = authenticate_read_only()

def authenticate_read_only():
    load_dotenv()

    reddit = praw.Reddit(
        client_id = os.getenv('REDDIT_CLIENT_ID'),
        client_secret = os.getenv('REDDIT_SECRET'),
        user_agent = 'Test Script (by /u/cilk02)',
    )

    return reddit

if __name__ == '__main__':
    sys.exit(main())