"""
This file contains the controller that accepts command via HTTP
and trigger business logic layer
"""

import os
import sys
from flask import Flask, request
from flask import typing as flask_typing
import api
import storage


AUTH_TOKEN = os.environ.get("API_AUTH_TOKEN")

if not AUTH_TOKEN:
    print("AUTH_TOKEN environment variable must be set")
    sys.exit()


app = Flask(__name__)


@app.route('/', methods=['POST'])
def main() -> flask_typing.ResponseReturnValue:
    """
    Controller that accepts command via HTTP and
    trigger business logic layer

    Proposed POST body in JSON:
    {
      "date: "2022-08-09",
      "raw_dir": "/path/to/my_dir/raw/sales/2022-08-09"
    }
    """
    input_data: dict = request.json
    msg = ''
    code = 0

    if "date" in input_data and "raw_dir" in input_data:
        #print(input_data, input_data['date'])

        mylist = api.get_sales(input_data['date'])
        #print(mylist[0])
        #print(mylist[1])
        code = int(mylist[1])

        stored = storage.save_to_disk(
                    mylist[0]['data'],
                    input_data['raw_dir']+"/sales_"+input_data['date']
        )

        if mylist[1] == 200 and stored == 'ok':
            msg = "Data retrieved from API and saved successfully"
            code = 201
        elif not mylist:
            msg = "Nothing found for the date"
            code = 201

    else:
        msg = "Check that all parameters are set"
        code = 400

    return {
            "message": msg,
            }, code

if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=8081)
