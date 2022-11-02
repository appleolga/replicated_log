### replicated_log

- master and secondary apps are in main.py in each respective folder
- each app also contains models.py module where all the replication, communication logic is implemented 
- master app has html input client implemented. Get method to retrieve log messages is also implemented on the master client. After container start it is accessible on localhost:8080
- to retrieve recorded messages on a secondary please use "curl localhost:{container PORT}" 
- the app is built via docker-compose with an option of secondary scaling to any number of instances ("docker-compose up --scale secondary={number_of_instances}")


p.s. Thank you for this task :) I knew almost nothing about server-clients, asyncronous programming or Docker. Now, after a lot of struggling,  I know much more 
