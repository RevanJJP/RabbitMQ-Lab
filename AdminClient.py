import threading

import pika
import time
import SystemNamespace as SN


class AdminClient(object):
    def __init__(self, admin_name):
        self.__communication_lock = threading.RLock()

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=SN.Host))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange=SN.Exchanges.Main, exchange_type='topic')

        self.key = SN.key(SN.Key.Direct, admin_name)
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.queue = result.method.queue

        self.__bind_admin_queues()
        self.channel.basic_consume(queue=self.queue, on_message_callback=self.receive_messages_basic, auto_ack=True)

    def __bind_admin_queues(self):
        self.channel.queue_bind(exchange=SN.Exchanges.Main, queue=self.queue, routing_key='#')

    def receive_messages_basic(self, ch, method, props, body):
        self.__communication_lock.acquire()

        if props.reply_to != self.key:
            print(props.reply_to.__str__() + " TO " + method.routing_key.__str__() + ": \"" + body.decode() + "\"")

        self.__communication_lock.release()

    def send_message_to_group(self, msg, group):
        self.__communication_lock.acquire()
        self.channel.basic_publish(
            exchange=SN.Exchanges.Main,
            routing_key=SN.key(group),
            properties=pika.BasicProperties(
                reply_to=self.key,
            ),
            body=msg
        )
        self.__communication_lock.release()

    def __consume_from_queue(self, timeout=1):
        self.__communication_lock.acquire()
        self.connection.process_data_events(timeout)
        self.__communication_lock.release()
        time.sleep(0.5)

    def consume(self):
        while True:
            self.__consume_from_queue()


def interface(admin):
    print("Message sending mode:\n\tE -> everyone\n\tA -> agencies\n\tC -> carriers")

    while True:
        message = input()
        _input = input("Input mode: ")
        if _input == "E":
            admin.send_message_to_group(message, SN.Key.Agencies)
            admin.send_message_to_group(message, SN.Key.Carriers)
        elif _input == "A":
            admin.send_message_to_group(message, SN.Key.Agencies)
        elif _input == "C":
            admin.send_message_to_group(message, SN.Key.Carriers)
        else:
            print("Couldn't recognized input mode")


def main():
    name = input("Input your admin's name: ")
    admin = AdminClient(name)
    consume_thread = threading.Thread(target=admin.consume)
    consume_thread.start()
    interface(admin)


if __name__ == "__main__":
    main()
