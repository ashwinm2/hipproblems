from flask import Flask, jsonify
import requests
import threading
from Queue import Queue
import logging

app = Flask(__name__)

# This list could be maintained separately or can be populated by passing the request parameters.
repositories = [ 'united', 'expedia', 'orbitz', 'priceline', 'travelocity']

''' Enables queue elements to be accessed using index, ensuring concurrency conflicts do not occur.
'''
class IndexableQueue(Queue):
  def __getitem__(self, index):
    with self.mutex:
      return self.queue[index]

''' For each site, separate threads are spawned to get the data.
'''
class scraperThread (threading.Thread):
    def __init__(self, name):
        threading.Thread.__init__(self)
        self.name = name

    def run(self):
        url = 'http://localhost:9000/scrapers/' + self.name
        try:
            response = requests.get(url, headers={'Content-Type': 'application/json'})
            # Check if the api responded successfully and more checks can be added as need be.
            if response.status_code != 200:
                logging.warning("The site " + self.name + "did not respond succeessfully")
            else:
                content = response.json()
                temp = content['results']
                total.put(temp)
        except:
            logging.error("The service is not up and running.")


# Queue used to store data obtained from the api calls.
total = IndexableQueue()

''' Endpoint for the search request '/flights/search'.
'''
@app.route('/flights/search', methods=['GET'])
def search():
    for siteName in repositories:
        t = scraperThread(siteName)
        t.start()

    content = []
    while (not total.empty()):
        content.extend(total.get())

    # Sorting the whole list by agony.
    newlist = sorted(content, key=lambda k: k['agony'])
    content = {"results":newlist}
    return jsonify(content)

''' Starting the flask application. '''
if __name__ == '__main__':
    app.run(port = 8000,debug=True)
