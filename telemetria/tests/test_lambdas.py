import importlib, json, os, sys, types, unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "lambdas"))


class ClientError(Exception):
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}


class FakeDynamo:
    def __init__(self, items=None):
        self.items = items or []
        self.puts, self.deletes = [], []
    def put_item(self, **kw): self.puts.append(kw)
    def delete_item(self, **kw): self.deletes.append(kw)
    def scan(self, **kw):
        if "ExclusiveStartKey" not in kw:
            return {"Items": self.items[:1], "LastEvaluatedKey": {"connectionId": {"S": "x"}}}
        return {"Items": self.items[1:]}


class FakeApi:
    def __init__(self, gone=()):
        self.gone, self.enviados = set(gone), []
    def post_to_connection(self, ConnectionId, Data):
        if ConnectionId in self.gone:
            raise ClientError("GoneException")
        self.enviados.append((ConnectionId, json.loads(Data)))


def cargar(nombre, dynamo):
    boto3 = types.ModuleType("boto3"); boto3.client = lambda *a, **k: dynamo
    botocore = types.ModuleType("botocore"); exc = types.ModuleType("botocore.exceptions"); exc.ClientError = ClientError
    with mock.patch.dict(sys.modules, {"boto3": boto3, "botocore": botocore, "botocore.exceptions": exc}):
        sys.modules.pop(nombre, None)
        return importlib.import_module(nombre)


class Mensajero(unittest.TestCase):
    def test_connect_guarda(self):
        d = FakeDynamo(); m = cargar("mensajero", d)
        r = m.handler({"requestContext": {"routeKey": "$connect", "connectionId": "abc"},
                       "queryStringParameters": {"clientId": "cliente_test"}}, None)
        self.assertEqual(r["statusCode"], 200)
        item = d.puts[0]["Item"]
        self.assertEqual(item["connectionId"], {"S": "abc"}); self.assertEqual(item["clientId"], {"S": "cliente_test"})
        self.assertIn("ttl", item)
    def test_connect_sin_params(self):
        d = FakeDynamo(); m = cargar("mensajero", d)
        m.handler({"requestContext": {"routeKey": "$connect", "connectionId": "abc"}, "queryStringParameters": None}, None)
        self.assertEqual(d.puts[0]["Item"]["clientId"], {"S": "anonimo"})
    def test_connect_error_500(self):
        d = FakeDynamo(); d.put_item = lambda **k: (_ for _ in ()).throw(RuntimeError("x"))
        m = cargar("mensajero", d)
        r = m.handler({"requestContext": {"routeKey": "$connect", "connectionId": "abc"}}, None)
        self.assertEqual(r["statusCode"], 500)
    def test_disconnect_borra(self):
        d = FakeDynamo(); m = cargar("mensajero", d)
        r = m.handler({"requestContext": {"routeKey": "$disconnect", "connectionId": "abc"}}, None)
        self.assertEqual(r["statusCode"], 200); self.assertEqual(d.deletes[0]["Key"], {"connectionId": {"S": "abc"}})
    def test_default(self):
        d = FakeDynamo(); m = cargar("mensajero", d)
        self.assertEqual(m.handler({"requestContext": {"routeKey": "$default", "connectionId": "a"}}, None)["statusCode"], 200)


class Streams(unittest.TestCase):
    def evento(self):
        return {"Records": [
            {"eventName": "INSERT", "dynamodb": {"NewImage": {
                "id": {"S": "e1"}, "origen": {"S": "recomendaciones"}, "tipo": {"S": "PRODUCTO_VISTO"},
                "clienteId": {"S": "u1"}, "productoId": {"N": "7"}, "timestamp": {"N": "1700000000000"}, "ttl": {"N": "9"}}}},
            {"eventName": "REMOVE", "dynamodb": {"OldImage": {"id": {"S": "z"}}}},
            {"eventName": "MODIFY", "dynamodb": {"NewImage": {"id": {"S": "e2"}, "tipo": {"S": "PAGO_INTENTO"}, "monto": {"N": "1990.5"}}}},
        ]}
    def test_broadcast(self):
        d = FakeDynamo(items=[{"connectionId": {"S": "c1"}}, {"connectionId": {"S": "c2"}}])
        s = cargar("streams", d); api = FakeApi(); s._api = api
        r = s.handler(self.evento(), None)
        self.assertEqual(r, {"enviados": 2, "conexiones": 2})
        _, lote = api.enviados[0]
        self.assertEqual(len(lote), 2)
        self.assertEqual(lote[0], {"id": "e1", "origen": "recomendaciones", "tipo": "PRODUCTO_VISTO", "clienteId": "u1", "productoId": 7, "timestamp": 1700000000000})
        self.assertEqual(lote[1]["monto"], 1990.5)
    def test_gone_se_borra(self):
        d = FakeDynamo(items=[{"connectionId": {"S": "c1"}}, {"connectionId": {"S": "c2"}}])
        s = cargar("streams", d); s._api = FakeApi(gone={"c2"})
        r = s.handler(self.evento(), None)
        self.assertEqual(r["enviados"], 1); self.assertEqual(d.deletes[0]["Key"], {"connectionId": {"S": "c2"}})
    def test_sin_registros_utiles(self):
        d = FakeDynamo(); s = cargar("streams", d); s._api = FakeApi()
        self.assertEqual(s.handler({"Records": [{"eventName": "REMOVE"}]}, None)["enviados"], 0)
        self.assertEqual(s.handler({}, None)["enviados"], 0)

unittest.main(verbosity=2)
