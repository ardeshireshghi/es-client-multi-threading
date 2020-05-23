import json
import re
from urllib2 import unquote
from time import time, sleep
import requests
from threading import Thread
from Queue import Queue
import sys


class MultiThreadESSearchClient():
    def __init__(self, host, query_template, search_uris,
                 concurrency, threads_per_second, requests_queue=None):

        self.host = host
        self.query_template = query_template
        self.requests_queue = Queue(maxsize=concurrency) if not isinstance(
            requests_queue, Queue) else requests_queue
        self.sess = self.create_request_session()
        self.search_uris = search_uris
        self.threads_per_second = threads_per_second
        self.requests_count = 0
        self.initial_requests_start_time = time()
        self.start_threads(concurrency)

    def start_threads(self, concurrency):
        # Define threads
        for i in range(concurrency):
            t = Thread(target=self.do_work)
            t.daemon = True
            t.start()

    def fire_requests(self):
        for uri in self.search_uris:
            es_query_keyword = self.get_query_word_from_url(uri)
            request_params = dict(
                search_uri=uri.replace('\r\n', ''),
                query_keyword=es_query_keyword
            )

            self.requests_queue.put(request_params)

        # Block until all items in the queue have been handled
        self.requests_queue.join()

        # Output total execution time after all requests are fired
        total_time_elapsed = time() - self.initial_requests_start_time
        print("\nTotal request time in seconds: %s" % total_time_elapsed)

    def do_work(self):
        while True:
            # Initial start time
            if not hasattr(self, 'start_time'):
                self.start_time = time()

            self.elapsed_time = time() - self.start_time

            # If we have passed 1 seconds or had more than threads_per_seconds requestswithin a second
            if self.elapsed_time >= 1 or self.requests_count > self.threads_per_second:
                self.start_time = time()
                self.requests_count = 0

                # If had more requests than threads_per_seconds then pause for the diff time
                if self.elapsed_time < 1:
                    sleep(1 - self.elapsed_time)

            request_params = self.requests_queue.get()
            response = self.perform_es_request(**request_params)
            self.output_results(response, **request_params)
            self.requests_queue.task_done()
            self.requests_count += 1

    def perform_es_request(self, query_keyword, search_uri):
        params = dict(
            url=self.host + '/_search',
            headers={"content-type": 'application/json'},
            data=self.query_template.replace("{{queryKeyword}}", query_keyword)
        )

        start_time = time()
        response = self.do_request(**params)
        end_time = time()
        response.elapsed_time = (end_time - start_time) * 1000
        return response

    def do_request(self, **kwargs):
        try:
            response = self.sess.post(
                timeout=60,
                **kwargs
            )
            return response
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            print e.message
            raise e

    def output_results(self, response, query_keyword, search_uri):
        response_to_json = response.json()
        print('%s,%s,%s,%s' % (
            response_to_json['took'], response.elapsed_time, search_uri, query_keyword))

    def create_request_session(self):
        sess = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=500,
            pool_maxsize=100,
            max_retries=3
        )
        sess.mount('http://', adapter)
        return sess

    def get_query_word_from_url(self, url):
        url = unquote(url).decode('utf-8')
        return re.search('q=([^\r]+)&', url).group(1).split('&')[0]
