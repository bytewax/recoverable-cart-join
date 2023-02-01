# Recoverable Shopping Cart Application

- Skill level
    
    **Intermediate, Some prior knowledge required**
    
- Time to complete
    
    **Approx. 15 min**
    
### Introduction

In this example, we're going to build a small online order fulfillment system. It will join two event streams: one containing customer orders and another containing successful payments, and then emit which orders for each customer have been paid. We're also going to design it to be recoverable and restart-able after invalid data.

## ****Prerequisites****

**Sample Data**

Make a file named `cart-join.json` with the following data:

```json
{"user_id": "a", "type": "order", "order_id": 1}
{"user_id": "a", "type": "order", "order_id": 2}
{"user_id": "b", "type": "order", "order_id": 3}
{"user_id": "a", "type": "payment", "order_id": 2}
{"user_id": "b", "type": "order", "order_id": 4}
FAIL HERE
{"user_id": "a", "type": "payment", "order_id": 1}
{"user_id": "b", "type": "payment", "order_id": 4}
```

**Python modules**
bytewax==0.15.0

## Your Takeaway

*Your takeaway from this tutorial will be a streaming application that aggregates shoppers data into a shopping cart that is recoverable in the instance that it fails.*

## Table of content

- Resources
- Step 1. Input
- Step 2. Dataflow
- Step 3. Execution
- Step 4. Making our Dataflow Recoverable
- Summary

## Resources

[GitHub Repo](https://github.com/bytewax/recoverable-cart-join)

## Step 1. Input

In a production application you would most likely be using something like kafka or redpanda as the input source. In this scenario we will build our own input source from the file we created earlier. To start we are going to use the manual input configuration with an input builder that doesn't support recovery yet. The builder will read each line of JSON, and emit a tuple of the line number and the parsed JSON. We'll use the line number later in this example when we work on recovery. 

```python
import json

from bytewax.inputs import ManualInputConfig


def input_builder(worker_index, worker_count, resume_state):
    assert worker_index == 0  # We're not going to worry about multiple workers yet.
    with open("cart-join.json") as f:
        for i, line in enumerate(f):
            obj = json.loads(line)
            yield i, obj
```

## Step 2. Dataflow

A dataflow is the unit of workload. They are data parallel directed acyclic graphs that are made up of processing steps. We will walk through the construction of a dataflow to join orders together into our shopping cart.

We will start with an empty dataflow.

```python
from bytewax.dataflow import Dataflow

flow = Dataflow()
flow.input("input", ManualInputConfig(input_builder))
```

Our plan is to use the stateful map operator to actually do the
join. All stateful operators require their input data to be a `(key, value)`
tuple so that the system can route all keys so they access the same state.
Let's add that key field using the `user_id` field present in every event.

```python
def key_off_user_id(event):
    return event["user_id"], event


flow.map(key_off_user_id)
```

Now onto the join itself. Stateful map needs two callbacks: One that
builds the initial, empty state. And one that combines new items into
the state and emits a value downstream.

We'll make a quick dictionary that holds the relevant data.

```python
def build_state():
    return {"unpaid_order_ids": [], "paid_order_ids": []}
```

The constructor of the class can be re-used as the initial state
builder.

Now we need the join logic, which will return two values: the updated
state and the item to emit downstream. Since we'd like to continuously
be emitting the most updated join info, we'll emit the state again.

```python
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
```

The items that stateful operators emit also have the relevant key still
attached, so in this case we have `(user_id, joined_state)`. Let's
format that into a dictionary for output.

```python
def format_output(user_id__joined_state):
    user_id, joined_state = user_id__joined_state
    return {
        "user_id": user_id,
        "paid_order_ids": joined_state["paid_order_ids"],
        "unpaid_order_ids": joined_state["unpaid_order_ids"],
    }


flow.map(format_output)
```


On that note, we'll serialize the final output as JSON and print it.

```python
def output_builder(worker_index, worker_count):
    def output_handler(item):
        line = json.dumps(item)
        print(line)

    return output_handler
```

And finally, capture this output and send it to our output builder.

```python
from bytewax.outputs import ManualOutputConfig

flow.capture(ManualOutputConfig(output_builder))
```

## Step 3. Execution

At this point our dataflow is constructed, Now we can run our dataflow:

```python
from bytewax.execution import run_main

run_main(flow)
```

Cool, we're chugging along and then OH NO!

```{testoutput}
{"user_id": "a", "paid_order_ids": [], "unpaid_order_ids": [1]}
{"user_id": "a", "paid_order_ids": [], "unpaid_order_ids": [1, 2]}
{"user_id": "b", "paid_order_ids": [], "unpaid_order_ids": [3]}
{"user_id": "a", "paid_order_ids": [2], "unpaid_order_ids": [1]}
{"user_id": "b", "paid_order_ids": [], "unpaid_order_ids": [3, 4]}
Traceback (most recent call last):
...
json.decoder.JSONDecodeError: Expecting value: line 1 column 1 (char 0)
```

Something went wrong! In this case it was that we had a non-JSON line
`FAIL HERE` in the input, but you could imagine that Kafka consumer
breaks or the VM is killed or something else bad happens!

We've also built up very valuable state in our stateful map operator
and we don't want to pay the penalty of having to re-read our input
all the way from the beginning to build it back up. Let's see how we
could have implemented state recovery to allow that to happen in the
future!

Step 4. Making our Dataflow Recoverable

Following our checklist in [Bytewax's documentation on
recovery](/docs/getting-started/recovery/) we need to enhance our input
builder with a few things.

First, we need the ability to resume our input from where we left off.
When using the `ManualInputConfig`, we emit a state object, that we
will be provided with as the argument `resume_state` on our next invocation.

Here we're using the line number from `enumerate` as our state, and
we can use that to skip forward in the file to that line.


```python
def input_builder(worker_index, worker_count, resume_state):
    assert worker_index == 0  # We're not going to worry about multiple workers yet.
    resume_state = resume_state or 0
    with open("cart-join.json") as f:
        for i, line in enumerate(f):
            if i < resume_state:
                continue
            obj = json.loads(line)
            # Since the file has just read the current line as
            # part of the for loop, note that on resume we should
            # start reading from the next line.
            resume_state += 1
            yield resume_state, obj
```

We need a **recovery config** that describes where to store the state
and progress data for this worker. For this example we'll use a local
[SQLite](https://sqlite.org/index.html) database because we're running
on a single machine.

```python
from tempfile import TemporaryDirectory
from bytewax.recovery import SqliteRecoveryConfig

recovery_dir = (
    TemporaryDirectory()
)  # We'll store this somewhere temporary for this test.
recovery_config = SqliteRecoveryConfig(recovery_dir.name)
```

Now if we run the dataflow, the internal state will be persisted at
the end of each epoch so we can recover mid-way. Since we didn't
run with any of the recovery systems activated last time, let's run
the dataflow again with them enabled.

```python doctest:IGNORE_EXCEPTION_DETAIL doctest:ELLIPSIS
from bytewax.execution import TestingEpochConfig

flow = Dataflow()
flow.input("input", ManualInputConfig(input_builder))
flow.map(key_off_user_id)
flow.stateful_map("joiner", build_state, joiner)
flow.map(format_output)
flow.capture(ManualOutputConfig(output_builder))

run_main(flow, recovery_config=recovery_config, epoch_config=TestingEpochConfig())
```

As expected, we have the same error.

```{testoutput}
{"user_id": "a", "paid_order_ids": [], "unpaid_order_ids": [1]}
{"user_id": "a", "paid_order_ids": [], "unpaid_order_ids": [1, 2]}
{"user_id": "b", "paid_order_ids": [], "unpaid_order_ids": [3]}
{"user_id": "a", "paid_order_ids": [2], "unpaid_order_ids": [1]}
{"user_id": "b", "paid_order_ids": [], "unpaid_order_ids": [3, 4]}
Traceback (most recent call last):
...
json.decoder.JSONDecodeError: Expecting value: line 1 column 1 (char 0)
```

But this time, we've persisted state and the epoch of failure into the
recovery store! If we fix whatever is causing the exception, we can
resume the dataflow and still get the correct output. Let's fix up the
input handler, reconstruct our dataflow and run it one more time.

```python
def input_builder(worker_index, worker_count, resume_state):
    assert worker_index == 0  # We're not going to worry about multiple workers yet.
    resume_state = resume_state or 0
    with open("cart-join.json") as f:
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


flow = Dataflow()
flow.input("input", ManualInputConfig(input_builder))
flow.map(key_off_user_id)
flow.stateful_map("joiner", build_state, joiner)
flow.map(format_output)
flow.capture(ManualOutputConfig(output_builder))
```

Running the dataflow again will pickup very close to where we
failed. In this case, the failure happened with an input on line `5`,
so it resumes from there. As the `FAIL HERE` string is ignored,
there's no output when processing line `5`.

```python
run_main(flow, recovery_config=recovery_config, epoch_config=TestingEpochConfig())
```

```{testoutput}
{"user_id": "a", "paid_order_ids": [2, 1], "unpaid_order_ids": []}
{"user_id": "b", "paid_order_ids": [4], "unpaid_order_ids": [3]}
```

Notice how the system did not forget the information from previous
lines; we still see that user `a` has order `1`.

## Summary

Recoverable dataflows are key to any production system. This tutorial demonstrated how you can do this through building a shopping cart application. 

## We want to hear from you!

If you have any trouble with the process or have ideas about how to improve this document, come talk to us in the #troubleshooting Slack channel!

## Where to next?

See our full gallery of tutorials → 

[Share your tutorial progress!](https://twitter.com/intent/tweet?text=I%27m%20mastering%20data%20streaming%20with%20%40bytewax!%20&url=https://bytewax.io/tutorials/&hashtags=Bytewax,Tutorials)
