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
import boto3
import argparse
from aws_lambda_powertools import Logger


class MyFormatter(LambdaPowertoolsFormatter):
    def __init__(self):
        super().__init__(json_serializer=self.json_serializer_function)

    @staticmethod
    def json_serializer_function(obj):
        return json.dumps(obj, default=str, separators=(",", ":"))


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


def send_simple_query(kendra_client=None, query_text=None):
    response = kendra_client.query(
        IndexId=KENDRA_INDEX_ID,
        QueryText=query_text,
        # 'DOCUMENT'|'QUESTION_ANSWER'|'ANSWER',
        QueryResultTypeFilter='DOCUMENT',
        PageSize=10,
        # You can use any field that has the Sortable flag set to true.
        # _category,  _created_at, _last_updated_at, _version, _view_count
        SortingConfiguration={
            'DocumentAttributeKey': '_last_updated_at',
            'SortOrder': 'ASC'
        },
        SpellCorrectionConfiguration={
            'IncludeQuerySpellCheckSuggestions': False
        }
    )

    is_good_response(response)
    # logger.info(response)
    json_formatted_str = json.dumps(response, indent=2)
    # # print(json_formatted_str)
    print(json_formatted_str)


def main():
    parser = argparse.ArgumentParser(description='Kendra Query Manager',
                                     formatter_class=argparse.RawTextHelpFormatter, fromfile_prefix_chars=None,
                                     argument_default=None,
                                     conflict_handler='error', add_help=True, epilog=choices_descriptions())

    parser.add_argument('-r', '--region', help='target aws account region', default='us-east-1')
    parser.add_argument('-p', '--profile', help='account profile to assume credentials', required=True)
    parser.add_argument('-s', '--search', required=True, help='specify text to search')
    parser.add_argument('-a', '--action', required=True, choices=['send-simple-query'],
                        help='actions to perform')

    args = parser.parse_args()
    logger.info(args)

    # Get temporary credential from current session to execute api calls against target account

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    sts_client = session.client('sts', region_name=args.region)
    logger.info(sts_client.get_caller_identity())

    kendra_client = session.client('kendra', region_name=args.region)
    if args.action == 'send-simple-query':
        send_simple_query(kendra_client, args.search)


if __name__ == "__main__":
    main()
