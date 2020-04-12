import pickle
import pandas as pd
import numpy as np
from flask import Flask
from flask import request
app = Flask(__name__,static_url_path='', 
            static_folder='web/static',
            template_folder='web/templates')

@app.route('/simulate')
def get_recalculated():
	county = request.args.get('County', default = 'Sacramento', type = str)
	state = request.args.get('State', default = 'California', type = str)
	year = request.args.get('years', default =2025 , type = int)
	column = request.args.get('Column', default = 'NO', type = str)
	percent = request.args.get('percent', default =5 , type = int)
	with open('model.pickle', 'rb') as handle:
		model = pickle.load(handle)
		#read the forecasted csv data for county and state [forecasted_april11_new_x_values.csv]
		#recalculate the forecasted values as new X until 2025
		# put the new X through model and find the new Y's
		# return the new Ys until 2025
	return str(percent)



if __name__ == '__main__':
    app.run()

