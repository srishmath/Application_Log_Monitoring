to run:

inside /docker - 
docker-compose up --build


in another terminal - /workload_simulator -
python3 simulate_requests.py


in another terminal - /kafka -
python3 producer.py

in another terminal - /KafkaConsumer -
python3 consumer.py

in another terminal - /grafana
curl -X POST   -H "Content-Type: application/json"   -u "admin:94sm@123"   --data-binary @logs-dashboard.json    http://localhost:3000/api/dashboards/db
