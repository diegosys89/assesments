# assesments
How to send a requet api for insert data example

curl -X POST -H "Content-Type: application/json" -d '[{"id": 1234, "job": "test_job"},{"id":9999,"job":"tes2"}]' "http://localhost:4000/insert_data?table_name=jobs"
