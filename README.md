### replicated_log

- master and secondary apps are in main.py in each respective folder
- each app also contains models.py module where all the replication, communication logic is implemented 
- master app has html input client implemented. After container start it is accessible at http://127.0.0.1:8080/chat 
- to retrieve recorded messages please use "curl localhost:{container PORT}" 
- the app is built via docker-compose with an option of secondary scaling to any number of instances ("docker-compose up --scale secondary={number_of_instances}")


