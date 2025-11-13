from flask import Flask, request, send_file
import subprocess
import os
import opensearchpy as opensearch
import json
import tempfile

app = Flask(__name__)

CONFIG = json.load(open('config.json'))

def generate_workload_params(params_file, extra_params=None):
    if extra_params is None or len(extra_params) == 0:
        return params_file
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
        params = json.load(open(params_file))
        for k, v in extra_params.items():
            params[k] = v
        f.write(json.dumps(params))
        f.flush()
        os.fsync(f.fileno())
        print(f.name)
        return f.name

class OSCluster:
    def __init__(self, url):
        self.url = url
        self.client = opensearch.OpenSearch(url)
        self.index_built = False

    def create_command(self):
        # cmd = [
        #     'opensearch-benchmark',
        #     'run',
        #     f'--workload={CONFIG["workload"]}',
        #     '--test-mode',
        #     '--results-format=csv',
        #     '--kill-running-processes',
        #     '--results-file=results.csv'
        # ]
        params_file = generate_workload_params(CONFIG["workload-params"], CONFIG["extra-workload-params"])
        params = open(params_file).read()
        cmd = [
            'opensearch-benchmark',
            'run',
            f'--target-hosts={CONFIG["cluster-url"]}',
            f'--workload={CONFIG["workload"]}',
            f'--workload-params={params}',
            '--pipeline=benchmark-only',
            '--test-procedure=no-train-test',
            '--kill-running-processes',
            '--results-file=results.csv'
        ]
        if "auth" in CONFIG:
            cmd.append(
                f'--client-options=use_ssl:true,verify_certs:true,basic_auth_user:{CONFIG["auth"]["user"]},basic_auth_password:{CONFIG["auth"]["password"]},timeout:300')
        if "extra_args" in CONFIG:
            cmd.extend(CONFIG["extra_args"])
        return cmd

    def run_osb(self):
        if os.path.exists('results.csv'):
            os.remove('results.csv')
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

@app.route('/send_opensearch', methods=['POST'])
def send_opensearch():
    # Get the file from the request
    file = request.files['file']
    # Save the file
    file.save('kNN.tar.gz')

    os.remove('kNN') if os.path.exists('kNN') else None
    # Untar the file
    subprocess.run(['tar', '-xzf', 'kNN.tar.gz'])
    subprocess.run(['rm', 'kNN.tar.gz'])
    subprocess.run(['./gradlew', 'run'], cwd='kNN')
    # Send the success response
    return 'Success'

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000)