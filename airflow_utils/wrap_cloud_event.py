import logging
import uuid
import json
from datetime import datetime

logging.basicConfig(level=logging.INFO)

def main(ce_source, ce_type, ce_project, src_file, dest_file):

    timestamp = datetime.utcnow().isoformat()
    logging.info(f'Reading input data from {src_file}')
    with open(src_file, 'r') as src, open(dest_file, 'w') as dest:
        for line in src:
            cloud_event = {
                    "datacontenttype": "application/json",
                    "id": str(uuid.uuid4()),
                    "source": ce_source,
                    "type": ce_type,
                    "time": datetime.utcnow().isoformat(),
                    "referenceprojectid": ce_project,
                    "data": json.loads(line),
            }
            dest.write(json.dumps(cloud_event))
            dest.write('\n')

    logging.info('Done!')

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("source",
            help="Source of the cloud event")
    parser.add_argument("type",
            help="Type of the cloud event")
    parser.add_argument("project",
            help="Project of the cloud event")
    parser.add_argument("src_file",
            help="Path to source data file")
    parser.add_argument("dest_file",
            help="Path to destination data file")
    args = parser.parse_args()
    main(args.source, args.type, args.project, args.src_file, args.dest_file)
