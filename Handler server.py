import pickle
import queue
import select
import socket
import threading
import time

import cv2

# Data sent by masterVM
# ["op","channel","image","value","height","width"]
# Data received from vms
# ["op","channel","processedimage"]
VMS_IP_Port = {"vm1": ["0.0.0.0", 5030],
               "vm2": ["0.0.0.0", 5031]}


def MasterTask(name, r, g, b, value, height=0, width=0):
    result_queue = queue.Queue(3)
    if name == "intensity-grey":
        processed = vm2.process(name, "ig", r, value, height, width)
        return processed
    if name == "threshold":
        processed = vm1.process(name, "th", r, value, height, width)
        return processed
    else:
        vm1.start_process(name, "r", r, value, height, width)
        vm2.start_process(name, "b", b, value, height, width)
        time.sleep(0.01)
        vm1_receive_thread = threading.Thread(
            target=vm1.receive_process, args=(result_queue,))
        vm1_receive_thread.start()
        vm2_receive_thread = threading.Thread(
            target=vm2.receive_process, args=(result_queue,))
        vm2_receive_thread.start()
        while not result_queue.full():
            continue
        adjusted_R = adjusted_G = adjusted_B = None
        for i in range(3):
            item = result_queue.get()
            if item[1] == "r":
                adjusted_R = item[2]
            elif item[1] == "b":
                adjusted_B = item[2]

        processed = cv2.merge((adjusted_R, adjusted_G, adjusted_B))
    return processed


class VM:
    def __init__(self, ip, port) -> None:
        self.ip = ip
        self.port = port
        self.vmSocket = None

    def connect(self):
        try:
            print(f"Connecting to {self.ip}:{self.port}")
            self.vmSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.vmSocket.connect((self.ip, self.port))
            print(f"Connected to {self.ip}:{self.port}")
        except Exception as e:
            print(f"Error connecting to {self.ip}:{self.port}: {e}")

    def process(self, op, channel, image, value, height, width):
        try:
            print("start sending")
            self.send_data([op, channel, image, value, height, width])
            print("start receive process")
            receivedData = self.receive_data()
            operation = receivedData[0]
            ch = receivedData[1]
            processedImageChannel = receivedData[2]
            return processedImageChannel

        except Exception as e:
            print(f"Error in process: {e}")

    def start_process(self, op, channel, image, value, height, width):
        try:
            print("start sending")
            self.send_data([op, channel, image, value, height, width])

        except Exception as e:
            print(f"Error in start_process: {e}")

    def receive_process(self, result_queue):
        try:
            print("start receive process")
            receivedData = self.receive_data()
            operation = receivedData[0]
            ch = receivedData[1]
            processedImageChannel = receivedData[2]

            result_queue.put([operation, ch, processedImageChannel])

        except Exception as e:
            print(f"Error in receive_process: {e}")

    def closeVM(self):
        try:
            self.send_data(["EXIT"])
            receivedData = self.receive_data()
            self.vmSocket.close()
        except Exception as e:
            print(f"Error in closeVM: {e}")

    def receive_data(self):
        try:
            timeout = 5
            data = b""
            ready_to_read, _, _ = select.select([self.vmSocket], [], [])
            if ready_to_read:
                print(f"start receiving data")
                self.vmSocket.settimeout(timeout)
                while True:
                    try:
                        chunk = self.vmSocket.recv(4096)
                        if not chunk:
                            break
                        data += chunk
                    except socket.timeout:
                        break

            array = pickle.loads(data)
            print("returning received array")
            return array
        except Exception as e:
            print(f"Error in receive_data: {e}")

    def send_data(self, data):
        try:
            serialized_data = pickle.dumps(data)
            self.vmSocket.sendall(serialized_data)
        except Exception as e:
            print(f"Error in send_data: {e}")


vm1 = VM(VMS_IP_Port["vm1"][0], VMS_IP_Port["vm1"][1])
vm2 = VM(VMS_IP_Port["vm2"][0], VMS_IP_Port["vm2"][1])

# Connect VMs
vm1.connect()
vm2.connect()
