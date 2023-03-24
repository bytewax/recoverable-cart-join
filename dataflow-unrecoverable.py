import json

from pathlib import Path
from datetime import timedelta

from bytewax.inputs import DynamicInput, StatelessSource
from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutput
from bytewax.execution import run_main


class JSONFileSource(StatelessSource):
    def __init__(self, path: Path):
        self._f = open(path, "rt")

    def next(self):
        line = self._f.readline().rstrip("\n")
        if len(line) <= 0:
            raise StopIteration()
        line = json.loads(line)
        return line

    def close(self):
        self._f.close()


class JSONFileInput(DynamicInput):
    def __init__(self, path: Path):
        self._path = path

    def build(self, _worker_index, _worker_count):
        return JSONFileSource(self._path)


def key_off_user_id(event):
    return event["user_id"], event


def build_state():
    return {"unpaid_order_ids": [], "paid_order_ids": []}


def joiner(state, event):
    e_type = event["type"]
    order_id = event["order_id"]
    if e_type == "order":
        state["unpaid_order_ids"].append(order_id)
    elif e_type == "payment":
        state["unpaid_order_ids"].remove(order_id)
        state["paid_order_ids"].append(order_id)
    return state, state


def format_output(user_id__joined_state):
    user_id, joined_state = user_id__joined_state
    return {
        "user_id": user_id,
        "paid_order_ids": joined_state["paid_order_ids"],
        "unpaid_order_ids": joined_state["unpaid_order_ids"],
    }


flow = Dataflow()
flow.input("input", JSONFileInput(Path("data/cart-join.json")))
flow.map(key_off_user_id)
flow.stateful_map("joiner", build_state, joiner)
flow.map(format_output)
flow.output("out", StdOutput())

if __name__ == "__main__":
    run_main(flow, epoch_interval=timedelta(seconds=0))
