import numpy as np
import sys

np.random.seed(0)

with open(sys.argv[1], 'r') as f:
    events = []
    user_num = 0
    item_num = 0
    for line in f:
        event = tuple(int(e) for e in line.split('\t'))
        events.append(event)
        if event[0] > user_num:
            user_num = event[0]
        if event[1] > item_num:
            item_num = event[1]
    full_train_data = np.zeros((user_num, item_num))
    for event in events:
        full_train_data[event[0]-1, event[1]-1] = event[2]
np.save("np_full_train", full_train_data)

permute = np.random.permutation(np.arange(len(events)))
permute_train = permute[:int(0.9 * len(events))]
permute_valid = permute[int(0.9 * len(events)):]

train_data = np.zeros((user_num, item_num))
for i in permute_train:
    event = events[i]
    train_data[event[0]-1, event[1]-1] = event[2]
valid_data = np.zeros((user_num, item_num))
for i in permute_valid:
    event = events[i]
    valid_data[event[0]-1, event[1]-1] = event[2]

np.save("np_train", train_data)
np.save("np_valid", valid_data)

with open(sys.argv[2], 'r') as f:
    events = []
    for line in f:
        event = tuple(int(e) for e in line.split('\t'))
        events.append(event)
    test_data = np.zeros((user_num, item_num))
    for event in events:
        test_data[event[0]-1, event[1]-1] = 1
np.save("np_test", test_data)

