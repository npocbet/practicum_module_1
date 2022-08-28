FROM python:3
EXPOSE 8000
COPY . /app/
WORKDIR /app
RUN pip install --upgrade pip \
     && pip install -r requirements.txt
CMD ["python", "manage.py", "runserver"]
