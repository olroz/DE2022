"""
Layer of persistence. Save content to the raw dir.
"""
import json
import os


def save_to_disk(json_content, path):
    """
    Save json_content to the path.
    """

    if json_content and path:
        filename = path + ".json"
        #shutil.rmtree(path) dont need it since replace the result file
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        with open(filename, 'w') as json_file:
            json.dump(json_content, json_file,
                            indent=4,
                            separators=(',',': '))

        json_file.close()
        return "ok"

    return "unknown"
