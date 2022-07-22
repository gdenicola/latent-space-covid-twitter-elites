# Adapted from https://github.com/thepanacealab/SMMT/blob/master/data_acquisition/get_metadata.py.
# Updated to Twitter API v2 and tailored to our needs.

import csv
import json
import math
import os
import time
import zipfile

import pandas as pd
import tweepy


def is_retweet(entry):
    return "retweeted_status" in entry.keys()


def get_source(entry):
    if "<" in entry["source"]:
        return entry["source"].split(">")[1].split("<")[0]
    else:
        return entry["source"]


def hydrate(*, bearer_token: str, inputfile: str, outputfile: str, idcolumn="tweet_id"):
    client = tweepy.Client(
        bearer_token=bearer_token, wait_on_rate_limit=True, return_type=dict
    )
    output_file = outputfile

    output_file_noformat = output_file.split(".", maxsplit=1)[0]
    print("-------------------------------------")
    print(f"Output File: {output_file}")
    print("-------------------------------------")

    output_file_short = f"{output_file_noformat}_short.json"
    compression = zipfile.ZIP_DEFLATED
    ids = []

    inputfile_df = pd.read_csv(inputfile, sep="\t")
    print("Tab separated input file, using \\t delimiter")

    if isinstance(inputfile_df, pd.DataFrame):
        inputfile_df = inputfile_df.set_index(idcolumn)

        ids = list(inputfile_df.index)

        print(f"total ids: {len(ids)}")

        start = 0
        end = 100
        limit = len(ids)
        i = int(math.ceil(float(limit) / 100))

        last_tweet = None
        if os.path.isfile(outputfile) and os.path.getsize(outputfile) > 0:
            with open(output_file, "rb") as f:
                # may be a large file, seeking without iterating
                f.seek(-2, os.SEEK_END)
                while f.read(1) != b"\n":
                    f.seek(-2, os.SEEK_CUR)
                last_line = f.readline().decode()
            last_tweet = json.loads(last_line)
            start = ids.index(int(last_tweet["id"]))
            end = start + 100
            i = int(math.ceil(float(limit - start) / 100))
        try:
            with open(output_file, "a") as outfile:
                for _ in range(i):
                    print("currently getting {} - {}".format(start, end))
                    time.sleep(6)  # needed to prevent hitting API rate limit
                    id_batch = ids[start:end]
                    start += 100
                    end += 100
                    backOffCounter = 1
                    while True:
                        try:
                            tweets = client.get_tweets(
                                id_batch,
                                tweet_fields=[
                                    "author_id",
                                    "created_at",
                                    "public_metrics",
                                    "context_annotations",
                                    "conversation_id",
                                    "entities",
                                    "reply_settings",
                                    "in_reply_to_user_id",
                                    "source",
                                    "possibly_sensitive",
                                ],
                            )
                            break
                        except tweepy.TweepyException as ex:
                            print(f"Caught the TweepyException:\n {ex}")
                            time.sleep(
                                30 * backOffCounter
                            )  # sleep a bit to see if connection Error is resolved before retrying
                            backOffCounter += 1  # increase backoff
                            if backOffCounter == 3:  # try 2 times
                                break
                            continue

                    if "data" in tweets.keys():
                        for tweet in tweets["data"]:
                            json.dump(tweet, outfile)
                            outfile.write("\n")
        except BaseException as ex:
            print(ex)
            print("exception: continuing to zip the file")

        print("creating zipped master json file")
        zf = zipfile.ZipFile("{}.zip".format(output_file_noformat), mode="w")
        zf.write(output_file, compress_type=compression)
        zf.close()

        print("creating minimized json master file")
        with open(output_file_short, "w") as outfile:
            with open(output_file) as json_data:
                for tweet in json_data:
                    data = json.loads(tweet)
                    text = data["text"]
                    like_count = data["public_metrics"]["like_count"]
                    retweet_count = data["public_metrics"]["retweet_count"]
                    reply_count = data["public_metrics"]["reply_count"]
                    quote_count = data["public_metrics"]["quote_count"]
                    in_reply_to_user_id = ""
                    context_annotations = ""
                    entities = ""
                    conversation_id = ""

                    if "in_reply_to_user_id" in data.keys():
                        in_reply_to_user_id = data["in_reply_to_user_id"]
                    if "context_annotations" in data.keys():
                        context_annotations = data["context_annotations"]
                    if "entities" in data.keys():
                        entities = data["entities"]
                    if "conversation_id" in data.keys():
                        conversation_id = data["conversation_id"]

                    t = {
                        "created_at": data["created_at"],
                        "text": text,
                        "author_id": data["author_id"],
                        "in_reply_to_user_id": in_reply_to_user_id,
                        "like_count": like_count,
                        "retweet_count": retweet_count,
                        "quote_count": quote_count,
                        "reply_count": reply_count,
                        "context_annotations": context_annotations,
                        "conversation_id": conversation_id,
                        "entities": entities,
                        "source": data["source"],
                        "id": data["id"],
                        "reply_settings": data["reply_settings"],
                        "possibly_sensitive": data["possibly_sensitive"],
                    }
                    json.dump(t, outfile)
                    outfile.write("\n")

        f = csv.writer(open("{}.csv".format(output_file_noformat), "w"))
        print("creating CSV version of minimized json master file")
        fields = [
            "created_at",
            "text",
            "author_id",
            "in_reply_to_user_id",
            "like_count",
            "retweet_count",
            "quote_count",
            "reply_count",
            "context_annotations",
            "conversation_id",
            "entities",
            "source",
            "id",
            "reply_settings",
            "possibly_sensitive",
        ]
        f.writerow(fields)
        with open(output_file_short) as master_file:
            for tweet in master_file:
                data = json.loads(tweet)
                f.writerow(
                    [
                        data["created_at"],
                        data["text"].encode("utf-8"),
                        data["author_id"],
                        data["in_reply_to_user_id"],
                        data["like_count"],
                        data["retweet_count"],
                        data["quote_count"],
                        data["reply_count"],
                        data["context_annotations"],
                        data["conversation_id"],
                        data["entities"],
                        data["source"],
                        data["id"],
                        data["reply_settings"],
                        data["possibly_sensitive"],
                    ]
                )

