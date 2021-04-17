import pandas as pd, json, io
from flask import Flask, render_template, Response, jsonify

app = Flask(__name__)

@app.route('/')
@app.route('/home')
def home():
    return render_template('home.html')


@app.route('/json')
def json_so():
    df = pd.read_csv(io.StringIO("""recipe_id|recipe_name|ingredients2
       240488  |  Pork Loin  |   [{"title":"basmati rice","quantity":"1 cup"}, {"title":"mint  leaves","quantity":"1/2teaspoon"}, {"title":"salt","quantity":"as required"}]
       218939  |   Foolproof Rosemary   |    [{"title":"Meat","quantity":"1 kg"}, {"title":"Water","quantity":"1 Glass"}, {"title":"Mirch powder","quantity":"5 -10 Teaspoon"}]
     """), sep="|")
    # key step convert string to actual JSON so when given back as response it's not a string
    df["ingredients2"] = df["ingredients2"].apply(lambda x: json.loads(x))
    # just use jsonify() it's simpler
    return jsonify(df[['recipe_id', 'recipe_name', 'ingredients2']].to_dict(orient="records"))


if __name__ == '__main__':
    app.run(debug=True, port=3000)