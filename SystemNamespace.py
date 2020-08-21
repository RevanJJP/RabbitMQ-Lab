import pika

Host = 'localhost'


class Service:
    PeopleTransport = "service_people_transport"
    CargoTransport = "cargo_people_transport"
    SatelliteStart = "service_place_satellite_on_orbit"


class Request:
    ServiceAvailability = "is_service_available"
    RequestService = "request_service"


class Response:
    ServiceAvailable = "requested_service_is_available"
    ServiceUnavailable = "requested_service_is_unavailable"
    ServiceCompleted = "requested_service_completed"


class Key:
    Direct = "direct"
    Agencies = "agencies"
    Carriers = "carriers"
    Admins = "admins"


class Exchanges:
    Main = "system_kosmiczny"


def key(group_key, name=None):
    if name is None:
        return group_key.__str__() + "." + "*"
    else:
        return group_key.__str__() + "." + name.__str__()


def msg(args) -> str:
    message = ""
    if isinstance(args, str):
        message += args.__str__()
    else:
        for arg in args:
            message += arg.__str__() + ';'
    return message


def msg_decode(message):
    args = message.__str__().split(";")
    return args


def title(sender_key) -> str:
    args = sender_key.__str__().split(".")
    return args[-1]