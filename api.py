from flask import Flask, request
from flask_restful import Resource, Api
from db.queries import get_all_full_rows
from db.schemas import AugmentedBillSchema


app = Flask(__name__)
api = Api(app)

class LegislationList(Resource):
    def get(self):
        rows = get_all_full_rows()
        # Only return bill data as first step
        rows_json = [AugmentedBillSchema().dump(r) for r in rows]
        # TODO: Update Access-Control-Allow-Origin once we've set up
        # the urls correctly.
        return rows_json, 200, {'Access-Control-Allow-Origin': '*'}

api.add_resource(LegislationList, '/legislation-tracker')

if __name__ == '__main__':
    app.run(debug=True)
