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
bytewax==0.16.*

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

## Step 1. Dataflow

A dataflow is the unit of work in Bytewax. Dataflows are data-parallel directed acyclic graphs that are made up of processing steps.

Let's start by creating an empty dataflow with no input or processing steps.

```python
from bytewax.dataflow import Dataflow

flow = Dataflow()
```


## Step 2. Input

In a production application you would most likely be using something like Kafka or Redpanda as the input source. In this scenario we will build a custom input source using the file we created earlier.

To build our own custom input source, we'll create a subclass of `StatelessSource` called `JSONFileSource`. `JSONFileSource` will read a line at a time from the supplied `path`, and parse it as JSON. When we reach the end of the file, we'll raise `StopIteration` to indicate that our input won't generate any more input.

```python
from pathlib import Path
from bytewax.inputs import StatelessSource

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

```

The last thing we need for our input is a subclass of `DynamicInput`, which will build an instance of our `JSONFileSource` class. Since we are only processing one file in this example, we are returning a `JSONFileSource` with a single path on a single worker. In a real application, you will want to supply each worker with a disjoint set of data.

``` python
from bytewax.inputs import DynamicInput

class JSONFileInput(DynamicInput):
    def __init__(self, path: Path):
        self._path = path

    def build(self, _worker_index, _worker_count):
        return JSONFileSource(self._path)
```

Our plan is to use the `stateful_map` operator to actually do the join between customers and orders. All stateful operators require their input data to be a `(key, value)` tuple so that Bytewax can ensure that all tems for a given `key` end up on the same worker.

Let's add that key field using the `user_id` field present in every event.

```python
def key_off_user_id(event):
    return event["user_id"], event


flow.map(key_off_user_id)
```

Now onto the join itself. Stateful map needs two callbacks: One that
builds the initial, empty state whenever a new key is encountered. 
And one that combines new items into the state and emits a value downstream.

Our builder function will create the initial dictionary to hold the relevant data.

```python
def build_state():
    return {"unpaid_order_ids": [], "paid_order_ids": []}
```

Now we need the join logic, which will return two values: the updated state and the item to emit downstream. Since we'd like to continuously be emitting the most updated join info, we'll return the updated state each time the joiner is called.

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

The items that stateful operators emit also have the relevant key still attached, so in this case we have `(user_id, joined_state)`. Let's format that into a dictionary for output.

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


And finally, capture this output and send it to our output builder.

```python
flow.output("out", StdOutput())
```

## Step 3. Execution

At this point our dataflow is constructed, and we can run it. We'll set the `epoch_interval` parameter to 0, so that we can see the output of every item as the dataflow runs.

```python
from bytewax.execution import run_main

run_main(flow, epoch_interval=timedelta(seconds=0))
```

Cool, we're chugging along and then OH NO!

```{testoutput}
{'user_id': 'a', 'paid_order_ids': [], 'unpaid_order_ids': [1]}
{'user_id': 'a', 'paid_order_ids': [], 'unpaid_order_ids': [1, 2]}
{'user_id': 'b', 'paid_order_ids': [], 'unpaid_order_ids': [3]}
{'user_id': 'a', 'paid_order_ids': [2], 'unpaid_order_ids': [1]}
{'user_id': 'b', 'paid_order_ids': [], 'unpaid_order_ids': [3, 4]}
thread '<unnamed>' panicked at 'Box<dyn Any>', src/operators/mod.rs:29:9
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

Traceback (most recent call last):
  File "/home/whoahbot/code/bytewax/recoverable-cart-join/./dataflow-unrecoverable.py", line 53, in <module>
    run_main(flow, epoch_interval=timedelta(seconds=0))
TypeError: JSONDecodeError.__init__() missing 2 required positional arguments: 'doc' and 'pos'
```

Something went wrong! In this case it was that we had a non-JSON line `FAIL HERE` in the input, but you could imagine that Kafka consumer breaks or the VM is killed or something else bad happened.

We've also built up very valuable state in our stateful map operator and we don't want to pay the penalty of having to re-read our input all the way from the beginning to build it back up. Let's see how we can implement state recovery to allow that to happen in the future.

## Step 4. Making our Dataflow Recoverable

Following our checklist in [Bytewax's documentation on recovery](/docs/getting-started/recovery/) we need to enhance our input builder with a few things.

First, we need the ability to resume our input from where we left off. Let's modify our `JSONFileInput` to subclass `PartitionedInput` and emit a subclass of `StatefulSource`.


```python
class JSONFileSource(StatefulSource):
    def __init__(self, path: Path, resume_state):
        resume_offset = resume_state or 0
        self._f = open(path, "rt")
        self._f.seek(resume_offset)

    def next(self):
        line = self._f.readline().rstrip("\n")
        if len(line) <= 0:
            raise StopIteration()
        line = json.loads(line)
        return line

    def snapshot(self):
        return self._f.tell()

    def close(self):
        self._f.close()
```

The `__init__` method for StatefulSource now takes an extra argument, `resume_state`. To understand what `resume_state` is, let's look at the other new method we are adding—`snapshot`. The `snapshot` method should return information that can be used to reconstruct the current state of the input source during recovery. In our case, we want to return our current position in the file, which we can get by calling `tell()`.

During recovery, our `JSONFileSource` class will be initialized with the value captured from the `snapshot` method at the end of the last epoch.

Now that we have our stateful source, we need to modify our `JSONFileInput` to subclass `PartitionedInput`, which takes the `resume_state` we will need when constructing our `JSONFileSource`.

``` python
class JSONFileInput(PartitionedInput):
    def __init__(self, path: Path):
        self._path = path

    def list_parts(self):
        return {str(self._path)}

    def build_part(self, for_part, resume_state):
        assert for_part == str(self._path), "Can't resume reading from different file"
        return JSONFileSource(self._path, resume_state)
```

Lastly, we'll need a **recovery config** that describes where to store the state and progress data for this worker. For this example we'll use [SQLite](https://sqlite.org/index.html).

```python
from bytewax.recovery import SqliteRecoveryConfig

recovery_config = SqliteRecoveryConfig(".")
```

Now if we run the dataflow, the internal state will be persisted at the end of each epoch so we can recover the state of the dataflow at that point. Since we didn't initially run with any recovery systems activated, let's run the dataflow again with them enabled.

As expected, we have the same error:

```{testoutput}
{'user_id': 'a', 'paid_order_ids': [], 'unpaid_order_ids': [1]}
{'user_id': 'a', 'paid_order_ids': [], 'unpaid_order_ids': [1, 2]}
{'user_id': 'b', 'paid_order_ids': [], 'unpaid_order_ids': [3]}
{'user_id': 'a', 'paid_order_ids': [2], 'unpaid_order_ids': [1]}
{'user_id': 'b', 'paid_order_ids': [], 'unpaid_order_ids': [3, 4]}
thread '<unnamed>' panicked at 'Box<dyn Any>', src/inputs.rs:216:35
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

Traceback (most recent call last):
  File "/home/whoahbot/code/bytewax/recoverable-cart-join/./dataflow.py", line 94, in <module>
    run_main(flow, epoch_interval=timedelta(seconds=0), recovery_config=recovery_config)
TypeError: JSONDecodeError.__init__() missing 2 required positional arguments: 'doc' and 'pos'
```

This time, we've persisted state and the epoch of our failure into the recovery store! If we fix whatever is causing the exception, we can resume the dataflow and still get the correct output. Let's fix our `JSONFileSource`, reconstruct our dataflow and run it one more time.

```python
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
```

Running the dataflow again will pickup very close to where we failed. In this case, the failure happened with an input on line `5`, so it resumes from there. As the `FAIL HERE` string is ignored, there's no output when processing line `5`.

```python
run_main(flow, recovery_config=recovery_config, epoch_config=TestingEpochConfig())
```

```{testoutput}
{'user_id': 'a', 'paid_order_ids': [2, 1], 'unpaid_order_ids': []}
{'user_id': 'b', 'paid_order_ids': [4], 'unpaid_order_ids': [3]}
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
