import json

from bytewax.dataflow import Dataflow
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import ManualOutputConfig
from bytewax.execution import run_main, TestingEpochConfig
from tempfile import TemporaryDirectory
from bytewax.recovery import SqliteRecoveryConfig

# build a recovery directory
recovery_dir = (
    TemporaryDirectory()
)  # We'll store this somewhere temporary for this test.
recovery_config = SqliteRecoveryConfig(recovery_dir.name)


def input_builder(worker_index, worker_count, resume_state):
    assert worker_index == 0  # We're not going to worry about multiple workers yet.
    resume_state = resume_state or 0
    with open("data/cart-join.json") as f:
        for i, line in enumerate(f):
            if i < resume_state:
                continue
            if line.startswith("FAIL"):  # Fix the bug.
                continue
            obj = json.loads(line)
            # Since the file has just read the current line as
            # part of the for loop, note that on resume we should
            # start reading from the next line.
            resume_state += 1
            yield resume_state, obj

            
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


def output_builder(worker_index, worker_count):
    def output_handler(item):
        line = json.dumps(item)
        print(line)

    return output_handler

flow = Dataflow()
flow.input("input", ManualInputConfig(input_builder))
flow.map(key_off_user_id)
flow.stateful_map("joiner", build_state, joiner)
flow.map(format_output)
flow.capture(ManualOutputConfig(output_builder))

if __name__ == "__main__":
  run_main(flow, recovery_config=recovery_config, epoch_config=TestingEpochConfig())
