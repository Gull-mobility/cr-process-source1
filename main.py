from flask import Flask
from markupsafe import escape

#Import to show date
from datetime import datetime

#Import all process
from process import execution

app = Flask(__name__)

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"

@app.route("/process/<name>")
def param(name):

    #Execute process
    execution(name)

    #Print time
    #TODO: Change timezone
    date = datetime.now()
    
    return f"Task donde for id, {escape(name)} at {date}!"

@app.route("/hello/<name>")
def hello(name):
    execution(escape(name))
    return f"Hello, {escape(name)}!"

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))