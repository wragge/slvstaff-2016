---
layout: slide
title: ""
---

{% highlight python %}
class SearchHarvester():
    """
    Harvest the details of 'Closed' files from RecordSearch.
    Saves to MongoDB.
    harvester = SearchHarvester(harvest='2015-12-31')
    harvester.start_harvest()
    """
    def __init__(self, harvest, **kwargs):
        self.total_pages = None
        self.pages_complete = 0
        self.client = RSSearchClient()
        self.prepare_harvest(access='Closed')
        db = self.get_db()
        self.items = db.items
        self.series = db.series
        self.harvests = db.harvests
        self.set_harvest(harvest)

    def set_harvest(self, harvest):
        # self.harvests.create_index('harvest_date', unique=True)
        dates = harvest.split('-')
        harvest_date = datetime(int(dates[0]), int(dates[1]), int(dates[2]), 0, 0, 0)
        try:
            self.harvests.insert({'harvest_date': harvest_date})
        except DuplicateKeyError:
            pass
        self.harvest_date = harvest_date

    def get_db(self):
        dbclient = MongoClient(MONGOLAB_URL)
        db = dbclient.get_default_database()
        return db

    def get_total(self):
        return self.client.total_results

    def prepare_harvest(self, **kwargs):
        self.client.search(**kwargs)
        total_results = self.client.total_results
        print '{} items'.format(total_results)
        self.total_pages = (int(total_results) / self.client.results_per_page) + 1
        print '{} pages'.format(self.total_pages)

    def start_harvest(self, page=None):
        item_client = RSItemClient()
        series_client = RSSeriesClient()
        # Refresh series with each harvest
        # self.series.remove({})
        # self.items.remove({})
        if not page:
            page = self.pages_complete + 1
        else:
            self.pages_complete = page - 1
        while self.pages_complete < self.total_pages:
            response = self.client.search(access='Closed', page=page, sort='9')
            for result in response['results']:
                item = item_client.get_summary(entity_id=result['identifier'])
                item['_id'] = item['identifier']
                item['random_id'] = [random.random(), 0]
                # Normalise reasons
                item['reasons'] = []
                # item['year'] = item['contents_dates']['end_date']['date'].year
                for reason in item['access_reason']:
                    matched = False
                    for exception, pattern in EXCEPTIONS:
                        if re.match(pattern, reason['reason']):
                            item['reasons'].append(exception)
                            matched = True
                    if not matched:
                        item['reasons'].append(reason['reason'])
                # Get series and agency info
                print item['series']
                series = self.series.find_one({'identifier': item['series']})
                if not series:
                    try:
                        series = series_client.get_summary(entity_id=item['series'])
                        agencies = series_client.get_controlling_agencies(entity_id=item['series'])
                        series['controlling_agencies'] = agencies
                        self.series.insert(series)
                    except UsageError:
                        series = None
                if series:
                    item['series_title'] = series['title']
                    if series['controlling_agencies']:
                        item['agencies'] = []
                        for agency in series['controlling_agencies']:
                            if not agency['end_date']:
                                item['agencies'].append(agency)
                old_item = self.items.find_one_and_replace({'_id': item['identifier']}, item, upsert=True)
                if old_item and ('harvests' in old_item) and (self.harvest_date not in old_item['harvests']):
                    harvests = old_item['harvests'].append(self.harvest_date)
                else:
                    harvests = [self.harvest_date]
                self.items.update_one({'_id': item['identifier']}, {'$set': {'harvests': harvests}})
                print item['identifier']
            self.pages_complete += 1
            page += 1
            print '{} pages complete'.format(self.pages_complete)
            time.sleep(1)
{% endhighlight %}

###### [https://github.com/wragge/closed_access](https://github.com/wragge/closed_access){: .external }