from cmath import log
import boto3
import json
import argparse
from os import listdir, getcwd, path, makedirs, remove
from os.path import isfile, join
from aws_lambda_powertools import Logger
from multiprocessing.pool import ThreadPool 
from functools import partial

# python data-loader.py --action prepare
# python data-loader.py --action clean
# python data-loader.py --action send --target aaa
# python data-loader.py --action upload --target kendrastack-kendrapocbucket5a96cdc2-hgvl4rwmo8x6 --region us-east-1 --profile gfs-acceleration-team+kendra
logger = Logger(service="DataLoader")


def main():
    parser = argparse.ArgumentParser(description='Kendra Query Manager',
                                     formatter_class=argparse.RawTextHelpFormatter, fromfile_prefix_chars=None,
                                     argument_default=None,
                                     conflict_handler='error', add_help=True, epilog=choices_descriptions())
    parser.add_argument('-a', '--action', required=True, choices=['prepare', 'upload', 'clean'],
                        help='actions to perform')
    parser.add_argument('-t', '--target', help='S3 bucket')
    parser.add_argument('-p', '--profile', help='AWS Profile to use')
    parser.add_argument('-r', '--region', help='target aws account region', default='us-east-1')

    args = parser.parse_args()
    logger.info(args)
    if args.action == 'prepare':
        prepare()
    elif args.action == "upload":
        upload("content", args.target, args.region, args.profile)
    elif args.action == "clean":
        clean("content")


def prepare():
    source_dir = "raw"
    destination_dir = "content"
    isExist = path.exists(path.join(getcwd(), destination_dir))
    if not isExist:
        makedirs(path.join(getcwd(), destination_dir))

    source_path = getcwd() + "/" + source_dir
    data_files = [f for f in listdir(source_path) if isfile(join(source_path , f))]
    logger.info("Found {0} files to process".format(len(data_files)))
    counter = 1
    for i in data_files:
        process_file(i, source_dir, destination_dir)
        counter = counter + 1
        if counter % 100==0:
            logger.info("processed {0} files.".format(counter))
    

def process_file(file, source_dir, output_dir):
    content = ""
    metadata = {}
    f = open(path.join(source_dir, file))
    data = json.load(f)
    
    content = data["text"]

    metadata["Title"] = data["title"]
    metadata["ContentType"] =  "JSON"
    metadata["DocumentId"] = data["uuid"]
   
    metadata["Attributes"] = {
        "_category": "",  # TODO: Add integration with comprehend
        "_created_at": data["published"],
        "_last_updated_at": "ISO 8601 encoded string",
        "_source_uri": data["url"],
        "_version": "file version",
        "_view_count": 0,
        "published": data["published"],
        "organizations": data["organizations"],
        "author": data["author"],
        "entities": data["entities"],
        "url": data["url"],
        "locations": data["locations"],
        "language": data["language"],
        "persons": data["persons"],
        "external_links": data["external_links"],
        "crawled": data["crawled"],
        "highlightTitle": data["highlightTitle"],
        "highlightText": data["highlightText"]

    }
    filename=path.basename(f.name).split('.')[0] + ".txt"    # override extention for text files
    write_to_disk(content,filename , "content")
    write_to_disk(metadata, filename + ".metadata.json", "content", "JSON")


def write_to_disk(content, filename, directory, content_type="TEXT"):
    if content_type=="TEXT":
        # logger.info(content)
        
        with open(path.join(directory, filename), "w") as outfile:
            outfile.write(content)
    elif content_type=="JSON":
        json_object = json.dumps(content)
        # logger.info(json_object)
        with open(path.join(directory, filename), "w") as outfile:
            outfile.write(json_object)


def upload(source_directory, bucket, region, profile):
    session = boto3.Session(profile_name=profile, region_name=region) 
    s3 = session.client('s3') 
    source_path = path.join(getcwd(), source_directory)
    data_files = [f for f in listdir(source_path) if isfile(join(source_path , f))]
    logger.info("Found {0} files to upload".format(len(data_files)))
    func = partial(upload_file, s3, bucket, source_directory)
    pool = ThreadPool(processes=10) 
    pool.map(func, data_files) 


def clean(directory):
    source_path = path.join(getcwd(), directory)
    data_files = [f for f in listdir(source_path) if isfile(join(source_path , f))]
    logger.info("Found {0} files to clean".format(len(data_files)))
    counter = 1
    for i in data_files:
        remove(path.join(source_path,i))        
        counter = counter + 1
        if counter % 100==0:
            logger.info("Removed {0} files.".format(counter))


def upload_file(s3_session,bucket,source_directory, myfile):
    source_path = path.join(getcwd(), source_directory, myfile)
    # logger.info(source_path)
    s3_session.upload_file(source_path, bucket, myfile) 


def choices_descriptions():
    return """
    Choices supports the following:
       prepare         - Prepare the data for content and metadata files
       upload            - send the files to S3 bucket 
       clean           - clean content and metadata directories
    """


if __name__ == "__main__":
    main()

