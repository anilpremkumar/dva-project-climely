conda create -n dva-server python=3.7
conda activate dva-server
python -m pip install Flask
python -m pip freeze > requirements.txt
python app.py
http://localhost:5000/
