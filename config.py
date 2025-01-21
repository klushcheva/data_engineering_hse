from scripts.env import DB, HOST, USER, PASSWORD, PORT


class Config:
    remote = {
        'database': DB,
        'host': HOST,
        'user': USER,
        'password': PASSWORD,
        'port': PORT
    }
