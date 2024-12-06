# LSPT-Indexing Service

## What's next? How do I run this project?

you need to creat a '.env' file in root becuase github will Ignore this file, you will not get this file when you clone. 

Then you can copy code from 'envtemple.txt' to '.env'.  Please replace <db_password> with the password for the yuxlin21 database user. 

Then run `pip install fastapi pymongo uvicorn python-dotenv`

Then run `uvicorn app.main:app --reload`

The API will be available at:
http://127.0.0.1:8000
