import pickle
import select
import socket
import threading
import image_processing_helper as img_helper
import pyopencl as cl

IP_ADDRESS = "0.0.0.0"
PORT = 5030

class VMHandler(threading.Thread):
    def __init__(self, ip, port, socket) -> None:
        super(VMHandler, self).__init__()
        self.ip = ip
        self.port = port
        self.socket = socket
        self.socket.setblocking(0)
        self.gpu_context = alt_lib.create_context()
        self.gpu_queue = alt_lib.create_queue(self.gpu_context)
        self.kernels = {
            'brighten': """
                __kernel void brighten(__global float* V) {
                    int i = get_global_id(0);
                    if (V[i] + 60.0f <= 255.0f)
                        V[i] = V[i] + 60.0f;
                    else
                        V[i] = 255.0f;
                }
            """,
            'darken': """
                __kernel void darken(__global float* V) {
                    int i = get_global_id(0);
                    if (V[i] - 60.0f >= 0.0f)
                        V[i] = V[i] - 60.0f;
                    else
                        V[i] = 0.0f;
                }
            """,
            'threshold': """
                __kernel void threshold(__global float* V) { 
                    int i = get_global_id(0);
                    V[i] = V[i] >= 127.0f ? 255.0f : 0.0f;
                }
            """,
            'greyscale': """
                __kernel void greyscale(__global float* channel, __global float* result) {
                    int i = get_global_id(0);
                    result[i] = channel[i];
                }
            """
        }
        
    def run(self) -> None:
        while True:
            try:
                received_data = self.receive_data()
                if len(received_data) == 0:
                    continue
                print(f"Received: {received_data[0]} : {received_data[1]} : {received_data[3]} : ")
                operation = received_data[0]
                channel = received_data[1]
                image_channel = received_data[2]
                value = received_data[3]
                height = received_data[4]
                width = received_data[5]
                processed = self.process_image(self.gpu_context, self.gpu_queue, operation, image_channel, value, height, width)
                print(f"Sending response {operation} ")
                self.send_data([operation, channel, processed])
            except OSError as o_err:
                print("OSError: {0}".format(o_err))
            except Exception as e:
                print("Exception: {0}".format(e))
                break
                
    def receive_data(self):
        timeout = 5
        data = b""
        ready_to_read, _, _ = select.select([self.socket], [], [])
        if ready_to_read:
            print(f"Start receiving data")
            self.socket.settimeout(timeout)
            while True:
                try:
                    chunk = self.socket.recv(4096)
                    if not chunk:
                        break
                    data += chunk
                except socket.timeout:
                    break
        array = pickle.loads(data)
        print("Returning received array")
        return array

    def send_data(self, data):
        serialized_data = pickle.dumps(data)
        print("Sending serialized data")
        self.socket.sendall(serialized_data)
    
    def process_image(self, ctx, queue, op, img, value, height=0, width=0):
        print(f"Starting processing {op}")
        if op == 'brighten':
            processed_channel = gpu_helper.apply_brightness(ctx, queue, img, value, self.kernels['brighten'], self.kernels['darken'])
            return processed_channel
        elif op == "greyscale":
            processed_channel = img_helper.convert_to_greyscale(ctx, queue, img, height, width, self.kernels['greyscale'])
            return processed_channel
        elif op == "threshold":
            processed_channel = img_helper.apply_threshold(ctx, queue, img, self.kernels['threshold'], height, width)
            return processed_channel

tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcp_socket.bind((IP_ADDRESS, PORT))
print(f"First virtual machine Start listening on IP:{IP_ADDRESS}, PORT:{PORT}")
tcp_socket.listen(1)

while tcp_socket:
    if not tcp_socket:
        continue
    readable, _, _ = select.select([tcp_socket], [], [])
    server_socket, server_address = tcp_socket.accept()
    new_thread = VMHandler(server_address[0], server_address[1], server_socket)
    print(f"Starting Thread with:{server_address[0]} : {server_address[1]} ")
    new_thread.start()
    break