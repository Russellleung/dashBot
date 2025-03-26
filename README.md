# dashBot


upload a csv file into Elasticsearch, get AI to create queries for you

create .env file

index_name = "ben10"
projectsDir = "projects"

path = ${projectsDir}/${index_name}

csv_file_path = ${path}/"ben10.csv"


elasticsearch_username="taken from local elastic"
elasticsearch_password="taken from local elastic"


API_KEY = "taken from openrouter"
API_URL = "https://openrouter.ai/api/v1/chat/completions"

steps 3-4 are optional. Only if you need special mappings
If you need to delete an index. Just run elasticsearchActions.py

steps to run this
1. create a new project under the projects folder. The name of the folder should be the same as your index name
2. add the csv file you want inside there
3. create a .py file as see in the australia folder and create a child of the Mappy class. The name variable should be the same as your index name
4. add a new elif statement in the MappyFactory class
5. complete the .env file. You can leave projectsDir as is. Change index_name, csv_file_path, the elasticsearch variables, ans the open_router api key.
6. go to the repository in command line
7. run "python3.10 -m venv venv"
8. run "source venv/bin/activate"
9. run "pip install -r requirements.txt"
10. run uploadToElasticSearch.py
11. run "streamlit run dashBot.py"