FROM python:3.8-alpine
WORKDIR /usr/src/app

ADD . .

RUN pip install --trusted-host pypi.python.org --no-cache-dir --upgrade -r requirements.txt


CMD ["python3", "main.py"]