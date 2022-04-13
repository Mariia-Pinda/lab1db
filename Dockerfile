FROM python:3.7

WORKDIR /docker

COPY requirements.txt /docker
RUN pip install -r requirements.txt

COPY . /docker

CMD [ "python3", "main.py" ]
