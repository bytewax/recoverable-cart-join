import json

from bytewax.dataflow import Dataflow
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import ManualOutputConfig
from bytewax.execution import run_main

def input_builder(worker_index, worker_count, resume_state):
    assert worker_index == 0  # We're not going to worry about multiple workers yet.
    with open("data/cart-join.json") as f:
        for i, line in enumerate(f):
            obj = json.loads(line)
            yield i, obj
            
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
  run_main(flow)
