from collections.abc import Callable, Iterable, Mapping
import threading
import time
from socket import *
import os
from typing import Any

HOST = '127.0.0.1'
PORT = 8765
BUFSIZE = 8192
ADDR = (HOST, PORT)


class client:
    def __init__(self):
        self.client = socket(AF_INET, SOCK_STREAM)
        self.client.connect(ADDR)

    def run(self):
        for i in range(1, 51):
            for j in range(1,11):
                data = f"select COUNT(ol_o_id) as cnt_{i}_{j} from order_line where ol_w_id={i} and ol_d_id={j};"
                self.client.send(bytes(data, 'utf-8'))
                self.client.recv(BUFSIZE)
                data = f"select SUM(o_ol_cnt) as sum_ol_cnt from orders where o_w_id={i} and o_d_id={j};"
                self.client.send(bytes(data, 'utf-8'))
                self.client.recv(BUFSIZE)
                data = f"select d_next_o_id from district where d_w_id={i} and d_id={j};;"
                self.client.send(bytes(data, 'utf-8'))
                self.client.recv(BUFSIZE)
                data = f"select MAX(o_id) as max_o_id from orders where o_w_id={i} and o_d_id={j};"
                self.client.send(bytes(data, 'utf-8'))
                self.client.recv(BUFSIZE)
                data = f"select MAX(no_o_id) as max_no_o_id from new_orders where no_w_id={i} and no_d_id={j};"
                self.client.send(bytes(data, 'utf-8'))
                self.client.recv(BUFSIZE)
                data = f"select COUNT(no_o_id) as count_no_o_id from new_orders where no_w_id={i} and no_d_id={j};"
                self.client.send(bytes(data, 'utf-8'))
                self.client.recv(BUFSIZE)
                data = f"select MIN(no_o_id) as min_no_o_id from new_orders where no_w_id={i} and no_d_id={j};"
                self.client.send(bytes(data, 'utf-8'))
                self.client.recv(BUFSIZE)
                data = f"select MAX(no_o_id) as max_no_o_id from new_orders where no_w_id={i} and no_d_id={j};"
                self.client.send(bytes(data, 'utf-8'))
                self.client.recv(BUFSIZE)


main = client()
main.run()
