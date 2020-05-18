#!/usr/bin/env python3

import sqlite3

class DataBase:
    
    def __init__(self, name):
        self._name=name
        try:
            self._conn = sqlite3.connect(self._name)
        except Error as e:
            print(e)
        self._cur=self._conn.cursor()

    def __del__(self):
        self._cur.close()
        self._conn.close()

    @property
    def cur(self):
        return self._cur

    @property
    def conn(self):
        return self._conn


        