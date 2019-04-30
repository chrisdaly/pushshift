import json
import requests
import math
import datetime
import time
import pandas as pd
from tqdm import tqdm_notebook as tqdm


class PushshiftClient(object):
    """Interacts with the Pushshift Reddit API to retrieve submission and
    comment data.

    Documentation:
        https://github.com/pushshift/api

     Args:
        max_retries (int): Number of times to attempt a call before giving up.
        size (int): Size of data to pull down at once.

    Todo:
        option to dump data for api response
    """

    def __init__(self, max_retries=10, size=500):
        self.max_retries = max_retries
        self.size = size
        self.url_base = 'https://api.pushshift.io/reddit/search/{}'
        self.endpoints = {'comment': 'link_id', 'submission': 'ids'}
        self._reset_params()

    def _reset_params(self):
        self.params = {'size': self.size, 'before': None, 'sort': 'desc'}

    def get_all_content(self, check_count=True, include_context=False, **params):
        data = {}

        for endpoint in self.endpoints.keys():
            if check_count:
                count = self.count(endpoint, **params)
                num_pulls = math.ceil(count / 500)
            else:
                num_pulls = None
            data_ = self.search(endpoint, num_pulls=num_pulls, **params)
            data[endpoint] = data_

        # Include every comment/submission that relates to the pulled content but doesn't fall into the date range.
        if include_context:
            threads_ids_of_comments = set([x.get('link_id').split('_')[1] for x in data['comment']])
            threads_ids = set([x.get('id') for x in data['submission']])
            threads_to_pull = list(threads_ids_of_comments.difference(threads_ids))

            for id_ in threads_to_pull:
                for endpoint, id_field in self.endpoints.items():
                    params = {id_field: id_}
                    data_ = self.search(endpoint, **params)
                    data[endpoint].extend(data_)

        return data

    def search(self, endpoint, num_pulls=None, dir_=None, **kwargs):
        self._reset_params()
        # if 'link_id' in kwargs:
        #     dir_ = '{}/{}'.format(dir_, kwargs['link_id'])
        #     if not os.path.exists(dir_):
        #         os.makedirs(dir_)
        return self._get_paged_data(endpoint, num_pulls, dir_, **kwargs)

    def count(self, endpoint, **kwargs):
        self._reset_params()
        return self._get_total_count(endpoint, **kwargs)

    def _get_total_count(self, endpoint, **kwargs):
        try:
            kwargs['aggs'] = 'created_utc'
            kwargs['frequency'] = '10000d'  # Set frequency to a very high number to get overall count.
            kwargs['size'] = 0
            count = self._get_data_from_endpoint(endpoint, **kwargs)
            return count.get('aggs').get(kwargs['aggs'])[0].get('doc_count')
        except:
            return 0

    def _get_paged_data(self, endpoint, num_pulls=None, dir_=None, **kwargs):
        i = 0
        data_all = []

        if num_pulls:
            pbar = tqdm(total=num_pulls)

        while True:
            if num_pulls:
                pbar.update(1)

            data = self._get_data_from_endpoint(endpoint, **kwargs).get('data')
            data_all.extend(data)
#             if dir_:
#                 self.dump_data(endpoint, dir_, data, i)

            if len(data) != self.size:
                if num_pulls:
                    pbar.close()
                break

            else:
                before = min(x['created_utc'] for x in data)
                kwargs.update({'before': before})  # move to self.params
                i += 1

        return data_all

    def _get_data_from_endpoint(self, endpoint, **kwargs):
        url = self.url_base.format(endpoint)
        self.params.update(kwargs)
        i, success = 0, False
        while (not success) and (i < self.max_retries):
            time.sleep(1)
            response = requests.get(url, params=self.params)
            print(response.url)
            success = response.status_code == 200
            i += 1

        return response.json()

    def dump_data(self, endpoint, dir_, data, i):
        """ Dumps each pull into a folder.
        """
        flle_path = '{}/{} - {}.txt'.format(dir_, endpoint, i)
        print(flle_path)
        with open(flle_path, 'w') as outfile:
            json.dump(data, outfile)

    @staticmethod
    def make_full_link(row):
        full_link = 'https://new.reddit.com/comments/{}/_/{}'.format(row.link_id.split('t3_')[1], row.id)
        return full_link

    @staticmethod
    def epoch_to_datetime(epoch):
        return datetime.datetime.fromtimestamp(epoch)

    @staticmethod
    def datetime_to_epoch(datetime_):
        epoch_micro = datetime_.timestamp()
        epoch = str(epoch_micro).split('.')[0]
        epoch = int(epoch)
        return epoch

    @staticmethod
    def format_threads_df(df_threads):
        df_threads['type'] = "submission"
        if 'permalink' in df_threads.columns:
            df_threads["thread_id"] = df_threads["permalink"].apply(lambda x: str(x).split('/comments/')[1].split('/')[0])
            df_threads['full_link'] = df_threads['permalink'].apply(lambda x: "http://www.reddit.com" + x)
        df_threads.rename(columns={'url': 'url_external'}, inplace=True)
        columns_template = ['created_utc', 'updated_utc', 'author', 'title', 'subreddit', 'full_link', 'thread_id', 'type', 'url_external']
        df_threads = df_threads[[col for col in columns_template if col in df_threads.columns]]

        return df_threads

    @staticmethod
    def format_comments_df(df_comments):
        df_comments['type'] = "comment"
        df_comments["thread_id"] = df_comments["link_id"].apply(lambda x: x.split('_')[1])
        df_comments['full_link'] = df_comments.apply(PushshiftClient.make_full_link, axis=1)
        columns_template = ['created_utc', 'updated_utc', 'author', 'body', 'subreddit', 'id', 'link_id', 'type', 'thread_id', 'full_link']
        df_comments = df_comments[[col for col in columns_template if col in df_comments.columns]]

        return df_comments

    @staticmethod
    def format_df_combined(df):
        df['created_utc'] = df['updated_utc'].fillna(df['created_utc'])
        df['created_utc'] = df['created_utc'].apply(PushshiftClient.epoch_to_datetime)
        df.sort_values(['thread_id', 'type', 'created_utc'], ascending=[True, False, True], inplace=True)
        columns_template = ['thread_id',  'type',  'created_utc', 'author', 'title', 'subreddit', 'full_link', 'body', 'url_external']
        df = df[[col for col in columns_template if col in df.columns]]
        return df

    @staticmethod
    def data_to_dataframe(data):
        df_threads = pd.DataFrame(data['submission'])
        df_comments = pd.DataFrame(data['comment'])

        if not df_threads.empty:
            df_threads = PushshiftClient.format_threads_df(df_threads)

        if not df_comments.empty:
            df_comments = PushshiftClient.format_comments_df(df_comments)

        df = pd.concat([df_threads, df_comments], sort=True)
        df = PushshiftClient.format_df_combined(df)
        return df


if __name__ == '__main__':
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
    print(df.shape)
    print(df.head(3))

    file_name = '{}.xlsx'.format(params.get('subreddit') or params.get('author'))
    writer = pd.ExcelWriter(file_name, options={'strings_to_urls': False})
    df.to_excel(writer, index=False)
    writer.save()
    print('{} - {}'.format(file_name, df.shape))

    with open('./{}.json'.format(params.get('subreddit') or params.get('author')), 'w') as f:
        json.dump(data, f)
