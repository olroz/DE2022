"""
Convert to avro and save to the stg dir.
"""

import json
import os
from fastavro import writer


def avro_to_disk(raw_dir, stg_dir):
    """
    Convert json from raw_dir to avro and save to the stg_dir dir.
    """

    if raw_dir and stg_dir:
        files = os.listdir(raw_dir)

        for file in files:
            jsonfilename = os.path.join(raw_dir, file)

            if os.path.isfile(jsonfilename) and file.endswith(".json"):
                print(jsonfilename)

                avrofilename = os.path.join(stg_dir, os.path.splitext(file)[0] + ".avro")
                #shutil.rmtree(stg_dir) dont need it since replace the result file
                os.makedirs(os.path.dirname(avrofilename), exist_ok=True)

                f = open(jsonfilename, "r")
                json_objects = json.load(f)

                schema = {
                    'name': 'Sales',
                    'namespace': 'de2022',
                    'type': 'record',
                    'fields': [
                        {'name': 'client', 'type': 'string'},
                        {'name': 'purchase_date', 'type': 'string'},
                        {'name': 'product', 'type': 'string'},
                        {'name': 'price', 'type': 'int'},
                    ],
                }

                with open(avrofilename, 'wb') as out:
                    writer(out, schema, json_objects)

                f.close()
                out.close()
                return "ok"

    return "unknown"
