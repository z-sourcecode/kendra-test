# Set up python env to run function locally
# python3 -m venv .venv
# . .venv/bin/activate
# python3 -m pip install -r requirements.txt
# echo python3 kendra-query-manager.py --a send-simple-query -s "text" -r $AWS_DEFAULT_REGION -p $AWS_PROFILE
# aws sts get-caller-identity
# python3 kendra-query-manager.py --a send-simple-query -s "text" -r $AWS_DEFAULT_REGION -p $AWS_PROFILE
from aws_lambda_powertools.logging.logger import Logger, set_package_logger
from aws_lambda_powertools.logging.formatter import LambdaPowertoolsFormatter
import json
import pandas as pd
import boto3
import argparse
from aws_lambda_powertools import Logger
import requests

TEST_DATASET = []
OPERATION_MODE = ""
EVE_ENDPOINT = ""
SESSION = None
REGION = None

class MyFormatter(LambdaPowertoolsFormatter):
    def __init__(self):
        super().__init__(json_serializer=self.json_serializer_function)

    @staticmethod
    def json_serializer_function(obj):
        return json.dumps(obj, default=str, separators=(",", ":"), indent=2)


logger = Logger(service="KendraQueriesManager", logger_formatter=MyFormatter())

# INDEX 1
KENDRA_INDEX_ID = "85420848-ba02-4d0c-931f-dcb254074734"


# INDEX 2
# KENDRA_INDEX_ID = "85420848-ba02-4d0c-931f-dcb254074734"

def choices_descriptions():
    return """
    Choices supports the following:
       send-simple-query         - Send a simple query to kendra search 
    """


def is_good_response(response):
    """Helper method to check if boto3 call was a success."""
    code = response["ResponseMetadata"]['HTTPStatusCode']
    # get response code 201 on EFS creation
    return 200 <= code < 300


def send_simple_query_kendra(query_text=None):
    kendra_client = SESSION.client('kendra', region_name=REGION)
    response = kendra_client.query(
        IndexId=KENDRA_INDEX_ID,
        QueryText=query_text,
        # 'DOCUMENT'|'QUESTION_ANSWER'|'ANSWER',
        QueryResultTypeFilter='DOCUMENT',
        PageSize=10,
        # You can use any field that has the Sortable flag set to true.
        # Reserve Kendra Key: _category,  _created_at, _last_updated_at, _version, _view_count
        # Custom Key: published
        SortingConfiguration={
            'DocumentAttributeKey': '_last_updated_at',
            'SortOrder': 'ASC'
        },
        SpellCorrectionConfiguration={
            'IncludeQuerySpellCheckSuggestions': False
        }
    )

    is_good_response(response)
    logger.info(response)
    return response


def run_test_case(filename=None):
    utterances = pd.read_csv(filename, sep=':', header=None, )
    test_case = {}

    for row in utterances.itertuples(index=False):
        persona = row[1]
        question = row[2].strip()
        test_case["persona"] = persona
        test_case["question"] = question

        if OPERATION_MODE == "EVE":
            test_case["intent"] = get_eve_intent(question)
            test_case["eve_results"] = get_eve_results(question)
            test_case["kendra_results"] = get_kendra_results(question, test_case["intent"])
        else:
            test_case["intent"] = ""
            test_case["eve_results"] = ""
            test_case["kendra_results"] = get_kendra_results(question)

        TEST_DATASET.append(test_case)      # save the test case in the final list

    # now push the list to a bucket
    write_results(TEST_DATASET)


def get_eve_intent(question):
    r = requests.get(EVE_ENDPOINT + "?utterance=" + question)
    print(r.text)
    return r.text


def get_eve_results(question):
    r = requests.get(EVE_ENDPOINT + "?utterance=" + question)
    print(r.text)
    return r.text


def get_kendra_results(question, intent=""):
    if intent == "":
        results = send_simple_query_kendra(query_text=question)
        return parse_kendra_results(results)
    # else:
        # results = send_intent_query_kendra(query_text=question, intent)
        # return parse_kendra_results(results)


def parse_kendra_results(raw_response):
    obj = json.loads(raw_response)
    counter = 1
    results = []
    for result in obj["ResultItems"]:
        if counter==3:
            return results
        else:
            results.append(result)
            counter = counter + 1
    print(result)
    return results


def write_results(dataset):
    # first create csv, then upload to s3
    s3_client = SESSION.client('s3', region_name=REGION)
    df = pd.DataFrame(dataset)
    csv_data = df.to_csv(index=False)

    # Structure: persona, question, eve_result1,eve_result2,eve_result3, kendra_result1, kendra_result2, kendra_result3
    # structure remains the same, even if some items has no value - we put in empty text
def main():
    parser = argparse.ArgumentParser(description='Kendra Query Manager',
                                     formatter_class=argparse.RawTextHelpFormatter, fromfile_prefix_chars=None,
                                     argument_default=None,
                                     conflict_handler='error', add_help=True, epilog=choices_descriptions())

    parser.add_argument('-r', '--region', help='target aws account region', default='us-east-1')
    parser.add_argument('-o', '--operation', help='DIRECT/EVE', default='DIRECT')
    parser.add_argument('-ep', '--endpoint', help="Eve endpoint of eve", default='None')
    parser.add_argument('-p', '--profile', help='account profile to assume credentials', required=True)
    parser.add_argument('-s', '--search', required=False, help='specify text to search')
    parser.add_argument('-f', '--file', required=False, help='specify file path of type csv')
    parser.add_argument('-a', '--action', required=True, choices=['send-simple-query', 'run-test-case'],
                        help='actions to perform')

    args = parser.parse_args()
    logger.info(args)
    OPERATION_MODE = args.operation
    EVE_ENDPOINT = args.endpoint
    REGION = args.region

    # Get temporary credential from current session to execute api calls against target account
    SESSION = boto3.Session(profile_name=args.profile, region_name=REGION)
    sts_client = SESSION.client('sts', region_name=REGION)
    logger.info(sts_client.get_caller_identity())



    if args.action == 'send-simple-query':
        if not args.search:
            logger.error("missing required option: args.search")
        else:
            send_simple_query_kendra(args.search)
    elif args.action == 'run-test-case':
        if not args.file:
            logger.error("missing required option: args.file")
        else:
            run_test_case(args.file)


if __name__ == "__main__":
    main()


#python kendra-query-manager.py --a run-test-case -f input.csv -r us-east-1 -p gfs-acceleration-team+kendra