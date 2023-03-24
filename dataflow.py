import json

from pathlib import Path
from datetime import timedelta

from bytewax.dataflow import Dataflow
from bytewax.inputs import PartitionedInput, StatefulSource
from bytewax.connectors.stdio import StdOutput
from bytewax.execution import run_main
from bytewax.recovery import SqliteRecoveryConfig

recovery_config = SqliteRecoveryConfig(".")


class JSONFileSource(StatefulSource):
    def __init__(self, path: Path, resume_state):
        resume_offset = resume_state or 0
        self._f = open(path, "rt")
        self._f.seek(resume_offset)

    def next(self):
        line = self._f.readline().rstrip("\n")
        if len(line) <= 0:
            raise StopIteration()
        if line.startswith("FAIL"):  # Fix the bug.
            return
        line = json.loads(line)
        return line

    def snapshot(self):
        return self._f.tell()

    def close(self):
        self._f.close()


class JSONFileInput(PartitionedInput):
    def __init__(self, path: Path):
        self._path = path

    def list_parts(self):
        return {str(self._path)}

    def build_part(self, for_part, resume_state):
        assert for_part == str(self._path), "Can't resume reading from different file"
        return JSONFileSource(self._path, resume_state)


flow = Dataflow()
flow.input("input", JSONFileInput(Path("data/cart-join.json")))


def key_off_user_id(event):
    return event["user_id"], event


flow.map(key_off_user_id)


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


flow.stateful_map("joiner", build_state, joiner)


def format_output(user_id__joined_state):
    user_id, joined_state = user_id__joined_state
    return {
        "user_id": user_id,
        "paid_order_ids": joined_state["paid_order_ids"],
        "unpaid_order_ids": joined_state["unpaid_order_ids"],
    }


flow.map(format_output)
flow.output("out", StdOutput())

if __name__ == "__main__":
    run_main(flow, epoch_interval=timedelta(seconds=0), recovery_config=recovery_config)
