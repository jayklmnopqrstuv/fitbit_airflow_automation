import pandas as pd

import logging

logging.basicConfig(level=logging.DEBUG)

def main(date, src_files, dest_files):
    logging.info(f'Reading input data from {src_files}')
    for i in src_files:
        egv_df = pd.read_csv(i)

    logging.info('Done!')

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",
            help="Date to calculate glycemic features for (format YYYY-MM-DD)")
    parser.add_argument("--source_files",
            nargs="*",
            help="Path to source data file")
    parser.add_argument("--dest_files",
            nargs="*",
            help="Path to destination data files")
    args = parser.parse_args()
    main(args.date, args.source_files, args.dest_files)
