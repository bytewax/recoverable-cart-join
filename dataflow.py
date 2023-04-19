from bytewax.dataflow import Dataflow

flow = Dataflow()

from bytewax.connectors.files import FileInput

flow.input("input", FileInput("data/cart-join.json"))

import json


def deserialize(s):
    return [json.loads(s)]


flow.flat_map(deserialize)


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

from bytewax.connectors.stdio import StdOutput

flow.output("output", StdOutput())
