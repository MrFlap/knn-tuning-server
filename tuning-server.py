from flask import Flask, request, send_file
import subprocess
import os
import opensearchpy as opensearch
import json

app = Flask(__name__)

CONFIG = json.load(open('config.json'))

class OSCluster:
    def __init__(self, url):
        self.url = url
        self.client = opensearch.OpenSearch(url)
        self.index_built = False

    def create_command(self):
        if self.index_built == False:
            return [
                'opensearch-benchmark',
                'run',
                f'--workload={CONFIG["workload"]}',
                '--test-mode',
                '--results-format=csv',
                '--kill-running-processes',
                '--results-file=results.csv'
            ]
        else:
            return []

    def run_osb(self):
        result = subprocess.run(self.create_command())

CLUSTER = OSCluster(CONFIG["cluster-url"])

@app.route('/run_osb', methods=['POST'])
def run_osb():
    print("Running OSB...")
    result = CLUSTER.run_osb()
    print("Success")
    print(result)
    # Send the result file back
    return send_file('results.csv')

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000)