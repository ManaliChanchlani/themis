import requests
import json
import csv
import pandas as pd

BASE_URL = "https://gateway.watsonplatform.net/retrieve-and-rank/api/"

def hello(USERNAME, PASSWORD, URL):
    return "hello"

def convert_corpus_to_json(CORPUS_FILE):
    df = pd.read_csv(CORPUS_FILE)
    df = df[['Answer', 'Answer Id']]
    # f = open('corpus.json', 'w')
    f = open('metlife/corpus_trial.json', 'w')
    df.to_json(f, orient = 'records')


    with open('metlife/corpus_trial.json', 'r') as f:
        data = json.load(f)
    a = []
    for row in data:
        temp = {"doc" : row}
        a.append(("add", temp))

    out = '{%s}' % ',\n'.join(['"{}": {}'.format(action, json.dumps(dictionary)) for action, dictionary in a])
    with open ('metlife/corpus.json', 'w') as f:
        f.write(out)


def create_cluster(BASE_URL,USERNAME, PASSWORD):
    cred = (USERNAME, PASSWORD)
    resp = requests.post(BASE_URL+"v1/solr_clusters", auth = cred)
    return resp.text


def check_cluster_status(BASE_URL,USERNAME, PASSWORD, CLUSTER_ID):
    cred = (USERNAME, PASSWORD)
    resp = requests.get(BASE_URL+"v1/solr_clusters/"+CLUSTER_ID, auth = cred )
    return resp.text


def upload_schema(BASE_URL, USERNAME, PASSWORD, CLUSTER_ID, ZIP_FILE):
    cred = (USERNAME, PASSWORD)
    headers = {
    'Content-Type': 'application/zip',
    }
    data = open(ZIP_FILE)
    resp = requests.post(BASE_URL+'v1/solr_clusters/'+CLUSTER_ID+'/config/example_config', headers=headers, data=data, auth = cred)
    return resp.text


def associate_config(BASE_URL,USERNAME, PASSWORD, CLUSTER_ID):
    cred = (USERNAME, PASSWORD)
    data = {"action" : "CREATE",
            "name":"example_collection",
            "collection.configName" : "example_config"}
    resp = requests.post(BASE_URL+'v1/solr_clusters/'+CLUSTER_ID+'/solr/admin/collections', data=data, auth=cred)
    return resp.text


def upload_corpus(BASE_URL, USERNAME, PASSWORD, CLUSTER_ID, CORPUS_FILE):
    cred = (USERNAME, PASSWORD)
    headers = {
    'Content-Type': 'application/json',
    }
    data = open(CORPUS_FILE)
    resp = requests.post(BASE_URL+'v1/solr_clusters/'+CLUSTER_ID+'/solr/example_collection/update', headers=headers, data=data, auth = cred)
    return resp.text


def upload_test_corpus(BASE_URL,USERNAME, PASSWORD, CLUSTER_ID):
    cred = (USERNAME, PASSWORD)
    resp = requests.get(BASE_URL+'v1/solr_clusters/'+CLUSTER_ID+'/solr/example_collection/select?q=*:*&fl=*&df=Answer', auth = cred)
    return resp.text


def create_truth(TRUTH_FILE):
    df = pd.read_csv(TRUTH_FILE)
    df = df[['Question', 'Answer Id']]
    df['Question'] = df['Question'].str.replace(":", "")
    print(df.keys())
    df['Relevance'] = 4
    print(df.keys())
    df.to_csv('metlife/rnr_truthincorpus.csv', index = False, header = False)


def check_ranker_status(BASE_URL,USERNAME, PASSWORD, RANKER_ID):
    cred = (USERNAME, PASSWORD)
    resp = requests.get(BASE_URL+'v1/rankers/'+RANKER_ID, auth = cred)
    return resp.text


def query_ranker(BASE_URL,USERNAME, PASSWORD, CLUSTER_ID, RANKER_ID, QUERY):
    print QUERY
    cred = (USERNAME, PASSWORD)
    resp = requests.get(BASE_URL+'v1/solr_clusters/'+CLUSTER_ID+'/solr/example_collection/fcselect?ranker_id='+RANKER_ID+'&q='+QUERY+'&wt=json', auth=cred)
    return resp.text

def query_untrained_ranker(BASE_URL, USERNAME, PASSWORD, CLUSTER_ID, QUERY):
    cred = (USERNAME, PASSWORD)
    resp = requests.get(BASE_URL+'v1/solr_clusters/'+CLUSTER_ID+'/solr/example_collection/fcselect?q='+QUERY+'&wt=json', auth=cred)
    return resp.text

def query_trained_rnr(BASE_URL,USERNAME, PASSWORD, CLUSTER_ID, RANKER_ID, QUESTION_FILE):
    answers = []
    with open(QUESTION_FILE, 'r') as f:
        input_reader = csv.DictReader( f, delimiter=',' )
        rows = [r for r in input_reader]
    print "number of sample questions: " ,len(rows)
    for row in rows:
        query = row['Question'].replace("#", "").replace(":","")
        resp = query_ranker(BASE_URL, USERNAME,PASSWORD,CLUSTER_ID,RANKER_ID, query)
        # if resp.status == 200:
        try:
            res = json.loads(resp)
        except:
            print resp.status_code, resp.text
            answers.append([query,0,"Query Error"])
            continue

        if res['response']['docs']:
            answers.append([query,res['response']['docs'][0]['score'],res['response']['docs'][0]['Answer'][0]])
        else:
            answers.append([query, 0, "No docs returned from RnR"])

    with open('deakin/answers.trained.rnr.csv', 'w') as f:
        output_writer = csv.writer(f)
        output_writer.writerow(['Question', 'Confidence', 'Answer'])
        for r in answers:
            output_writer.writerow((r))

def query_untrained_rnr(BASE_URL,USERNAME, PASSWORD, CLUSTER_ID, QUESTION_FILE):
    answers = []
    with open(QUESTION_FILE, 'r') as f:
        input_reader = csv.DictReader( f, delimiter=',' )
        rows = [r for r in input_reader]
    print "number of sample questions: " ,len(rows)
    for row in rows:
        query = row['Question'].replace("#", "")
        resp = query_untrained_ranker(BASE_URL, USERNAME,PASSWORD,CLUSTER_ID, query)
        # if resp.status_code == 200:
        try:
            res = json.loads(resp)
        except:
            print "inside error!!!!!!!"
            # print resp.status_code, resp.text
            answers.append([query,0,"Query Error"])
            continue

        if res['response']['docs']:
            answers.append([query,res['response']['docs'][0]['score'],res['response']['docs'][0]['Answer'][0]])
        else:
            answers.append([query, 0, "No docs returned from RnR"])

    with open('metlife/answers.untrained.rnr.csv', 'w') as f:
        output_writer = csv.writer(f)
        output_writer.writerow(['Question', 'Confidence', 'Answer'])
        for r in answers:
            output_writer.writerow((r))