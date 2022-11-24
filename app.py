# """
# Main application file:
# a simple Flask web application.  This application contains one endpoint that reverses and returns the requested URI
# """

# from flask import Flask
# app = Flask(__name__)

# @app.route('/<random_string>')
# def returnBackwardsString(random_string):
#     """Reverse and return the provided URI"""
#     return "".join(reversed(random_string))

# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port=8080)



"""
Main application file
This is to break the unit test
"""
from flask import Flask
app = Flask(__name__)

@app.route('/<random_string>')
def returnBackwardsString(random_string):
    """Reverse and return the provided URI"""
    return "Breaking the unit test"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)