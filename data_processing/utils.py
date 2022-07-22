import glob

import pandas as pd
import tweepy


def build_df(path, extension):
    all_files = glob.glob(f"{path}/*.{extension}")

    df_list = []
    lengths = []

    for filename in all_files:
        if extension == "tsv":
            df = pd.read_csv(filename, sep="\t")
        else:
            df = pd.read_csv(filename)
        lengths.append(len(df))
        df_list.append(df)

    df = pd.concat(df_list)
    assert sum(lengths) == len(df)

    return df


def get_users(user_ids, bearer_token):
    client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)
    users = []
    ids = [user_ids[i : i + 100] for i in range(0, len(user_ids), 100)]

    for i in ids:
        u = client.get_users(
            ids=i,
            user_fields=[
                "public_metrics",
                "created_at",
                "verified",
                "description",
                "location",
            ],
        )
        data = [
            {
                "id": user["id"],
                "name": user["name"],
                "username": user["username"],
                "created_at": user["created_at"],
                "verified": user["verified"],
                "followers_count": user["public_metrics"]["followers_count"],
                "following_count": user["public_metrics"]["following_count"],
                "tweet_count": user["public_metrics"]["tweet_count"],
                "location": "" if "location" not in user.keys() else user["location"],
            }
            for user in u.data
        ]
        users.extend(data)

    return users
