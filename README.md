# assesments
How to send a requet api for insert data example

`curl -X POST -H "Content-Type: application/json" -d '[{"id": 1234, "job": "test_job"},{"id":9999,"job":"tes2"}]' "http://localhost:5001/insert_data?table_name=jobs"`

feature create backup
`python table_backup.py`

feature restore tables
`python restore_tables.py`

All data tables are stored in a json config file config_data.json