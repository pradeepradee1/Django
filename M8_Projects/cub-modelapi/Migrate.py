git clone https://github.com/zucisystems-dev/cub-modelapi.git
git checkout athens
sudo apt-get install python3-dev default-libmysqlclient-dev build-essential
sudo apt install redis-server
vim Pipfile -> change python version from 3.7 to version that you have ex:3.8
vim .env -> change the mysql host username password port that you have
pipenv install
pipenv shell
python manage.py makemigrations modeltrain
python manage.py migrate
python manage.py runserver
Open new ternimal and run the following command
pipenv run celery -A model worker --pool=solo -l info