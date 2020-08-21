import threading
import pika
import time
import SystemNamespace as SN


class CarrierClient(object):
    def __init__(self, service_name, service1, service2):
        self.services = []
        self.services.append(service1)
        self.services.append(service2)

        self.__communication_lock = threading.RLock()

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=SN.Host))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange=SN.Exchanges.Main, exchange_type='topic')

        self.key = SN.key(SN.Key.Direct, service_name)
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.queue = result.method.queue
        self.__bind_carrier_queues()

        self.corr_id = 0
        self.awaited_response = None
        self.awaited_responder = None

        self.channel.basic_consume(queue=self.queue, on_message_callback=self.__on_response, auto_ack=True)

    def __bind_carrier_queues(self):
        self.channel.queue_bind(exchange=SN.Exchanges.Main, queue=self.queue, routing_key=SN.key(SN.Key.Carriers))
        self.channel.queue_bind(exchange=SN.Exchanges.Main, queue=self.queue, routing_key=self.key)

    def __on_response(self, ch, method, props, body):
        msg = SN.msg_decode(body.decode())

        if msg[0] == SN.Request.ServiceAvailability:
            service_type = msg[1]
            print(SN.title(props.reply_to) + " asked for " + service_type.__str__() + " availability")
            self.__answer_if_service_available(service_type, props.reply_to, props.correlation_id)
        elif msg[0] == SN.Request.RequestService:
            service_type = msg[1]
            order_no = msg[2]
            print(SN.title(props.reply_to) + " requested " + service_type.__str__())
            self.__provide_service(service_type, props.reply_to, order_no)
        else:
            print("Message\t" + SN.title(props.reply_to) + ": " + body.decode())

    def __answer_if_service_available(self, service_type, agency_key, corr_id):
        if service_type in self.services:
            msg = SN.msg([SN.Response.ServiceAvailable, service_type])
        else:
            msg = SN.msg([SN.Response.ServiceUnavailable, service_type])

        self.__communication_lock.acquire()
        self.channel.basic_publish(
            exchange=SN.Exchanges.Main,
            routing_key=agency_key,
            properties=pika.BasicProperties(
                reply_to=self.key,
                correlation_id=corr_id
                ),
            body=msg
        )
        self.__communication_lock.release()

    def __provide_service(self, service_type, agency_key, order_no):
        if self.__produce_service(service_type, agency_key) is False:
            return

        msg = SN.msg([SN.Response.ServiceCompleted, order_no])

        self.__communication_lock.acquire()
        self.channel.basic_publish(
            exchange=SN.Exchanges.Main,
            routing_key=agency_key,
            properties=pika.BasicProperties(reply_to=self.key),
            body=msg
        )
        self.__communication_lock.release()

    def __produce_service(self, service_type, agency_key) -> bool:
        if service_type in self.services:
            print(service_type.__str__() + " provided to " + SN.title(agency_key))
            return True
        else:
            return False

    def __consume_from_queue(self, timeout=1):
        time.sleep(0.5)
        self.__communication_lock.acquire()
        self.connection.process_data_events(timeout)
        self.__communication_lock.release()
        time.sleep(0.5)

    def consume(self):
        while True:
            self.__consume_from_queue()


def interface_instantiate() -> CarrierClient:
    name = input("Input your carrier's name: ")
    print("Services:\n\tPT -> people transport\n\tCT -> Cargo Transport\n\tSS -> satellite start")
    services = ['', '']

    i = 0
    while i < 2:
        _input = input("Input service " + str(i+1) + ": ")

        if _input == "PT":
            services[i] = SN.Service.PeopleTransport
        elif _input == "CT":
            services[i] = SN.Service.CargoTransport
        elif _input == "SS":
            services[i] = SN.Service.SatelliteStart
        else:
            print("Couldn't recognized service")
            i -= 1
        i += 1

        if i == 2 and services[0] == services[1]:
            print("Services must be different!")
            i -= 1

    return CarrierClient(name, services[0], services[1])


def main():
    carrier = interface_instantiate()
    consume_thread = threading.Thread(target=carrier.consume)
    consume_thread.start()


if __name__ == "__main__":
    main()
