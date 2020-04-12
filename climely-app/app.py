import pickle
import pandas as pd
import numpy as np
import json
from flask import Flask
from flask import request
app = Flask(__name__,static_url_path='', 
            static_folder='web/static',
            template_folder='web/templates')

@app.route('/simulate')
def get_recalculated():
	county = request.args.get('County', default = 'Maricopa', type = str)
	state = request.args.get('State', default = 'California', type = str)
	year = request.args.get('years', default =2025 , type = int)
	column = request.args.get('Column', default = 'NO', type = str)
	percent = request.args.get('percent', default =50 , type = int)
	with open('./web/static/model.pkl', 'rb') as handle:
		model = pickle.load(handle)
		sim_df = pd.read_csv('./web/static/Simulated_X_Y.csv', index_col='year')
		cty_df = sim_df[sim_df.State_Name == state]
		cty_df = cty_df[cty_df.County_Name == county]
		cty_df = cty_df.sort_values(by='year')
		if len(cty_df) == 0:
			return "INVALID"
		cty_df = cty_df.iloc[:, 2:-1]
		# 10000(1−𝑟)10=8700.
		right  = cty_df.loc[year][column] * ( 1 - (percent/100.0))
		left = cty_df.loc[year][column]
		k = (left - right) / (year-2020)
		i = 1
		for index, row in cty_df.iterrows():
		    print(row[column] - (k * (i + 1)))
		    cty_df.loc[index][column] = row[column] - (k * (i + 1))
		    i += 1
		cty_df['Disasters_By_Count'] = model.predict(cty_df)
	return cty_df.to_json(orient='index')



if __name__ == '__main__':
    app.run()

