import hashlib

BINANCE_CLIENT_ORDER_ID_MAX_LENGTH = 36
_CLIENT_ORDER_ID_PREFIX = "cml_"
_DIGEST_LENGTH = BINANCE_CLIENT_ORDER_ID_MAX_LENGTH - len(_CLIENT_ORDER_ID_PREFIX)


def deterministic_client_order_id(run_id: str, intent_id: str) -> str:
    if not run_id.strip():
        raise ValueError("run_id must not be empty")
    if not intent_id.strip():
        raise ValueError("intent_id must not be empty")
    digest = hashlib.sha256(f"{run_id}\0{intent_id}".encode()).hexdigest()
    return f"{_CLIENT_ORDER_ID_PREFIX}{digest[:_DIGEST_LENGTH]}"
