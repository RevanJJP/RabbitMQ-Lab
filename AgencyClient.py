import asyncio
import threading

import pika, uuid, time
import SystemNamespace as SN


class AgencyClient(object):
    def __init__(self, agency_name):
        self.__communication_lock = threading.RLock()
        self.orders = []
        self.completed_orders = []
        self.order_counter = 0

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=SN.Host))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange=SN.Exchanges.Main, exchange_type='topic')

        self.key = SN.key(SN.Key.Direct, agency_name)
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.queue = result.method.queue
        self.__bind_agency_queues()

        self.corr_id = 0
        self.awaited_response = None
        self.awaited_responder = None

        self.channel.basic_consume(queue=self.queue, on_message_callback=self.__on_response, auto_ack=True)

    def __bind_agency_queues(self):
        self.channel.queue_bind(exchange=SN.Exchanges.Main, queue=self.queue, routing_key=SN.key(SN.Key.Agencies))
        self.channel.queue_bind(exchange=SN.Exchanges.Main, queue=self.queue, routing_key=self.key)

    def __on_response(self, ch, method, props, body):
        msg = SN.msg_decode(body.decode())

        if msg[0] == SN.Response.ServiceCompleted:
            order_no = int(msg[1])
            self.__handle_order_completion(order_no)
        elif msg[0] == SN.Response.ServiceUnavailable:
            print(SN.title(props.reply_to) + ":" + msg[1].__str__() + " is unavailable")
        elif msg[0] == SN.Response.ServiceAvailable:
            print(SN.title(props.reply_to) + ": " + msg[1].__str__() + " is available")
            if self.corr_id != 0 and self.corr_id == props.correlation_id:
                self.awaited_response = body.decode()
                self.awaited_responder = props.reply_to
        else:
            print("Message\t" + SN.title(props.reply_to) + ": " + body.decode())

    def request_service(self, service_type):
        self.__set_corr_id()

        msg = SN.msg([SN.Request.ServiceAvailability, service_type])

        self.__communication_lock.acquire()
        self.channel.basic_publish(
            exchange=SN.Exchanges.Main,
            routing_key=SN.key(SN.Key.Carriers),
            properties=pika.BasicProperties(
                reply_to=self.key,
                correlation_id=self.corr_id
                ),
            body=msg
        )
        self.__communication_lock.release()
        self.__await_service_availability(service_type)
        self.__reset_corr_id()

    def __await_service_availability(self, service_type):
        if self.awaited_response is None:
            self.__consume_from_queue(3)

        if self.awaited_response is None:
            print("No response for \"" + service_type + "\" availability.")
        else:
            response = SN.msg_decode(self.awaited_response)
            if response[0] == SN.Response.ServiceUnavailable:
                self.__reset_response()
                self.__await_service_availability(service_type)
            elif response[0] == SN.Response.ServiceAvailable:
                self.__reset_corr_id()
                responder = self.awaited_responder
                self.__reset_response()
                self.__order_service(service_type, responder)
            else:
                print(self.awaited_responder + " send inappropriate message: " + response)
                self.__reset_response()
                self.__await_service_availability(service_type)

    def __order_service(self, service_type, carrier_key):
        order_no = self.__add_order_to_list()
        msg = SN.msg([SN.Request.RequestService, service_type, order_no])

        self.__communication_lock.acquire()
        self.channel.basic_publish(
            exchange=SN.Exchanges.Main,
            routing_key=carrier_key,
            properties=pika.BasicProperties(
                reply_to=self.key,
            ),
            body=msg)
        print("Ordered(no." + order_no.__str__() + ") " + service_type)
        self.__communication_lock.release()

    def __set_corr_id(self):
        self.corr_id = str(uuid.uuid4())

    def __reset_corr_id(self):
        self.corr_id = 0

    def __reset_response(self):
        self.awaited_response = None
        self.awaited_responder = None

    def __add_order_to_list(self) -> int:
        self.order_counter += 1
        order_number = self.order_counter
        self.orders.append(order_number)
        return order_number

    def __handle_order_completion(self, order_no):
        if order_no in self.orders:
            if order_no in self.completed_orders:
                print("Received already completed order: " + order_no.__str__())
            else:
                print("Completed order: " + order_no.__str__())
                self.completed_orders.append(order_no)
        else:
            print("Received wrong order: " + order_no.__str__())

    def __consume_from_queue(self, timeout=1):
        time.sleep(0.5)
        self.__communication_lock.acquire()
        self.connection.process_data_events(timeout)
        self.__communication_lock.release()
        time.sleep(0.5)

    def consume(self):
        while True:
            self.__consume_from_queue()


def DEBUG(msg):
    print("DEBUG:\t" + msg.__str__())


def interface(agency):
    print("Services:\n\tPT -> people transport\n\tCT -> Cargo Transport\n\tSS -> satellite start")
    print("Request services: ")

    while True:
        _input = input()
        if _input == "PT":
            agency.request_service(SN.Service.PeopleTransport)
        elif _input == "CT":
            agency.request_service(SN.Service.CargoTransport)
        elif _input == "SS":
            agency.request_service(SN.Service.SatelliteStart)
        else:
            print("Couldn't recognized service")


def main():
    name = input("Input your agency's name: ")
    agency = AgencyClient(name)
    consume_thread = threading.Thread(target=agency.consume)
    consume_thread.start()
    interface(agency)


if __name__ == "__main__":
    main()
