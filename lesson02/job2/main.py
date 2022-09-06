"""
This file contains the controller that accepts command via HTTP
and trigger business logic layer
"""

from flask import Flask, request
from flask import typing as flask_typing
import converter


app = Flask(__name__)


@app.route('/', methods=['POST'])
def main() -> flask_typing.ResponseReturnValue:
    """
    Controller that accepts command via HTTP and
    trigger business logic layer

    Proposed POST body in JSON:
    {
        "raw_dir": "/path/to/my_dir/raw/sales/2022-08-09",
        "stg_dir: "/path/to/my_dir/stg/sales/2022-08-09"
    }
    """
    input_data: dict = request.json
    msg = ''
    code = 0

    if "raw_dir" in input_data and "stg_dir" in input_data:

        converted = converter.avro_to_disk(input_data['raw_dir'], input_data['stg_dir'])

        if converted == 'ok':
            msg = "Data converted and saved successfully"
            code = 201

    else:
        msg = "Check that all parameters are set"
        code = 400

    return {
            "message": msg,
            }, code


if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=8082)
