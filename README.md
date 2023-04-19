# Shopping Cart Join Application

- Skill level
    
    **Intermediate, Some prior knowledge required**
    
- Time to complete
    
    **Approx. 15 min**
    
### Introduction

In this example, we're going to build a small online order fulfillment system. It will join two events within a stream: one event type containing customer orders and another containing successful payments. The dataflow will emit completed orders for each customer that have been paid.

## ****Prerequisites****

**Sample Data**

Make a file named `data/cart-join.json` with the following data:

```json
{"user_id": "a", "type": "order", "order_id": 1}
{"user_id": "a", "type": "order", "order_id": 2}
{"user_id": "b", "type": "order", "order_id": 3}
{"user_id": "a", "type": "payment", "order_id": 2}
{"user_id": "b", "type": "order", "order_id": 4}
{"user_id": "a", "type": "payment", "order_id": 1}
{"user_id": "b", "type": "payment", "order_id": 4}
```

**Python modules**
bytewax==0.16.*

## Your Takeaway

*Your takeaway from this tutorial will be a streaming application that aggregates shoppers data into a completed shopping cart.*

## Table of content

- Resources
- Step 1. Dataflow
- Step 2. Input
- Step 3. Execution
- Summary

## Resources

[GitHub Repo](https://github.com/bytewax/recoverable-cart-join)

## Step 1. Dataflow

A dataflow is the unit of work in Bytewax. Dataflows are data-parallel directed acyclic graphs that are made up of processing steps.

Let's start by creating an empty dataflow with no input or processing steps.

https://github.com/bytewax/recoverable-cart-join/blob/main/dataflow.py#L1-L3

## Step 2. Input

In a production application you would most likely be using something like Kafka or Redpanda as the input source. In this example, we will use the `FileInput` source that reads from the file we created earlier and emits one line at a time into our dataflow.

https://github.com/bytewax/recoverable-cart-join/blob/fd4c2e1a368bffc46ea9d823a810c4a2c11770a3/dataflow.py#L5-L7

Each of the lines in the file is a JSON encoded string. Let's add a step to decode our input into a Python dictionary.

https://github.com/bytewax/recoverable-cart-join/blob/fd4c2e1a368bffc46ea9d823a810c4a2c11770a3/dataflow.py#L9-L16

Our plan is to use the `stateful_map` operator to perform the join between customers and orders. All stateful operators require their input data to be in the form of a `(key, value)` tuple so that Bytewax can ensure that all tems for a given `key` end up on the same worker.

Let's add that key field using the `user_id` field present in every event.

https://github.com/bytewax/recoverable-cart-join/blob/fd4c2e1a368bffc46ea9d823a810c4a2c11770a3/dataflow.py#L19-L23

Now onto the join itself. Stateful map needs two functions: a `builder` that
creates the initial, empty state whenever a new key is encountered,
and a `mapper` that combines new items into the existing state.

Our builder function will create the initial dictionary to hold the relevant data.

https://github.com/bytewax/recoverable-cart-join/blob/fd4c2e1a368bffc46ea9d823a810c4a2c11770a3/dataflow.py#L26-L27

Now we need the join logic, which will return two values: the updated state and the item to emit downstream. Since we'd like to continuously be emitting the most updated join info, we'll return the updated state each time the joiner is called.

https://github.com/bytewax/recoverable-cart-join/blob/fd4c2e1a368bffc46ea9d823a810c4a2c11770a3/dataflow.py#L30-L41

The items that stateful operators emit also have the relevant key still attached, so in this case we have `(user_id, joined_state)`. Let's format that into a dictionary for output.

https://github.com/bytewax/recoverable-cart-join/blob/fd4c2e1a368bffc46ea9d823a810c4a2c11770a3/dataflow.py#L44-L53

Finally, capture this output and send it to STDOUT.

https://github.com/bytewax/recoverable-cart-join/blob/fd4c2e1a368bffc46ea9d823a810c4a2c11770a3/dataflow.py#L55-L57

## Step 3. Execution

At this point our dataflow is constructed, and we can run it. 

``` python
> python -m bytewax.run dataflow:flow

{'user_id': 'a', 'paid_order_ids': [], 'unpaid_order_ids': [1]}
{'user_id': 'a', 'paid_order_ids': [], 'unpaid_order_ids': [1, 2]}
{'user_id': 'b', 'paid_order_ids': [], 'unpaid_order_ids': [3]}
{'user_id': 'a', 'paid_order_ids': [2], 'unpaid_order_ids': [1]}
{'user_id': 'b', 'paid_order_ids': [], 'unpaid_order_ids': [3, 4]}
{'user_id': 'a', 'paid_order_ids': [2, 1], 'unpaid_order_ids': []}
{'user_id': 'b', 'paid_order_ids': [4], 'unpaid_order_ids': [3]}
```

We can also run our dataflow using multiple processes. Each process will handle a portion of the overall data:

``` python
❯ python -m bytewax.run dataflow:flow -i0 -a localhost:2101 -a localhost:2102
{'user_id': 'b', 'paid_order_ids': [4], 'unpaid_order_ids': [3]}
{'user_id': 'b', 'paid_order_ids': [4], 'unpaid_order_ids': [3]}
{'user_id': 'b', 'paid_order_ids': [4], 'unpaid_order_ids': [3]}
```

Start the second process in another terminal session

``` python
❯ python -m bytewax.run dataflow:flow -i1 -a localhost:2101 -a localhost:2102
{'user_id': 'a', 'paid_order_ids': [], 'unpaid_order_ids': [1]}
{'user_id': 'a', 'paid_order_ids': [], 'unpaid_order_ids': [1, 2]}
{'user_id': 'a', 'paid_order_ids': [2, 1], 'unpaid_order_ids': []}
{'user_id': 'a', 'paid_order_ids': [2, 1], 'unpaid_order_ids': []}
```

## Summary

This tutorial demonstrated how you can use `stateful_map` to join two event types together from a stream of incoming data.

## We want to hear from you!

If you have any trouble with the process or have ideas about how to improve this document, come talk to us in the #troubleshooting Slack channel!

## Where to next?

See our full gallery of tutorials → 

[Share your tutorial progress!](https://twitter.com/intent/tweet?text=I%27m%20mastering%20data%20streaming%20with%20%40bytewax!%20&url=https://bytewax.io/tutorials/&hashtags=Bytewax,Tutorials)
