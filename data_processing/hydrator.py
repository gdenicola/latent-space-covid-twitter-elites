import argparse
from calendar import monthrange
import csv
import gzip
import linecache
import os
from pathlib import Path
import shutil
from shutil import copyfile

import wget

from get_metadata import hydrate


class ascii:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


parser = argparse.ArgumentParser(description="Hydrator Script")
parser.add_argument(
    "-f",
    "--file",
    metavar="",
    type=str,
    default="",
    help="tsv file containing tweet IDs to be hydrated",
)
parser.add_argument(
    "-m",
    "--month",
    metavar="",
    type=int,
    required=True,
    help="month (number) for which the data should be hydrated",
)
parser.add_argument(
    "-y",
    "--year",
    metavar="",
    type=int,
    required=True,
    help="year (number) for which the data should be hydrated",
)
parser.add_argument(
    "-c",
    "--clean",
    action="store_true",
    help="whether or not to hydrate clean data (without retweets)",
)
parser.add_argument(
    "-l",
    "--lang",
    metavar="",
    type=str,
    default="de",
    help="langauge used to filter Tweets",
)
parser.add_argument(
    "-t",
    "--token",
    metavar="",
    type=str,
    required=True,
    help="Twitter API Bearer Token",
)

args = parser.parse_args()

file = args.file
month = args.month
year = args.year
clean = args.clean
language = args.lang
bearer_token = args.token

if file:
    file_noformat = file.split(".", maxsplit=1)[0]
    hydrate(
        bearer_token=bearer_token,
        inputfile=file,
        outputfile=f"data/clean/{language}/hydrated/hydrated_{file_noformat}",
    )

else:

    days_in_month = monthrange(year, month)[1]

    Path(f"data/clean/{language}").mkdir(parents=True, exist_ok=True)
    Path(f"dataset/full/{language}").mkdir(parents=True, exist_ok=True)

    for day in range(1, days_in_month + 1):

        url = ""
        file_path = ""
        filtered_file_path = ""
        hydrated_file_path = ""
        if clean:
            url = f"https://github.com/thepanacealab/covid19_twitter/blob/master/dailies/2021-{month:02d}-{day:02d}/2021-{month:02d}-{day:02d}_clean-dataset.tsv.gz?raw=true"
            file_path = f"data/clean/dataset-{month:02d}-{day:02d}.tsv"
            filtered_file_path = (
                f"data/clean/{language}/dataset-filtered-{month:02d}-{day:02d}.tsv"
            )
            hydrated_file_path = (
                f"data/clean/{language}/hydrated/hydrated-tweets-{month:02d}-{day:02d}"
            )
        else:
            url = f"https://github.com/thepanacealab/covid19_twitter/blob/master/dailies/2021-{month:02d}-{day:02d}/2021-{month:02d}-{day:02d}-dataset.tsv.gz?raw=true"
            file_path = f"data/full/dataset-{month:02d}-{day:02d}.tsv"
            filtered_file_path = (
                f"data/full/{language}/dataset-filtered-{month:02d}-{day:02d}.tsv"
            )
            hydrated_file_path = (
                f"data/full/{language}/hydrated/hydrated-tweets-{month:02d}-{day:02d}"
            )

        print(f"{ascii.OKCYAN}{url}{ascii.ENDC}")
        print(f"{ascii.OKCYAN}{file_path}{ascii.ENDC}")
        print(f"{ascii.OKCYAN}{filtered_file_path}{ascii.ENDC}")
        print(f"{ascii.OKCYAN}{hydrated_file_path}{ascii.ENDC}")

        # Downloads the dataset (compressed in a GZ format)
        #!wget dataset_URL -O clean-dataset.tsv.gz
        wget.download(url, out=f"{file_path}.gz")

        # Unzips the dataset and gets the TSV dataset
        with gzip.open(f"{file_path}.gz", "rb") as f_in:
            with open(file_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        # Deletes the compressed GZ file
        os.unlink(f"{file_path}.gz")

        filtered_language = language

        # If no language specified, it will get all records from the dataset
        if filtered_language == "":
            copyfile(
                file_path,
                filtered_file_path,
            )
        # If language specified, it will create another tsv file with the filtered records
        else:
            filtered_tw = list()
            current_line = 1
            with open(file_path) as tsvfile:
                tsvreader = csv.reader(tsvfile, delimiter="\t")
                if current_line == 1:
                    filtered_tw.append(
                        linecache.getline(
                            file_path,
                            current_line,
                        )
                    )

                for line in tsvreader:
                    if line[3] == filtered_language:
                        filtered_tw.append(
                            linecache.getline(
                                file_path,
                                current_line,
                            )
                        )
                    current_line += 1

            print(
                f"\n\n{ascii.BOLD}Showing first 5 tweets from the filtered dataset{ascii.ENDC}\n\n"
            )
            print(filtered_tw[1 : (6 if len(filtered_tw) > 6 else len(filtered_tw))])

            with open(filtered_file_path, "w") as f_output:
                for item in filtered_tw:
                    f_output.write(item)

        hydrate(
            bearer_token=bearer_token,
            inputfile=filtered_file_path,
            outputfile=hydrated_file_path,
        )
