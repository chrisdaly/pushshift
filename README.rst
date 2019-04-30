Pushshift
--------
Interacts with the Pushshift Reddit API to retrieve submission and comment data.

Basic usage::
    start = datetime.datetime(2018, 1, 11)
    end = datetime.datetime(2018, 1, 13)
    subreddit = "spinalmuscularatrophy"

    params = {
        'after': str(PushshiftClient.datetime_to_epoch(start)),
        'before': str(PushshiftClient.datetime_to_epoch(end)),
        'subreddit': subreddit
    }

    pushshift_api = PushshiftClient()
    data = pushshift_api.get_all_content(include_context=True, **params)
    df = PushshiftClient.data_to_dataframe(data)