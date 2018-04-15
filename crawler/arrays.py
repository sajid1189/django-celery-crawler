from Queue import Queue as SysQueue


class Stack:
    def __init__(self):
        self.stack = []

    def pop(self):
        if self.is_empty():
            return None
        else:
            self.stack.pop()

    def push(self, val):
        self.stack.append(val)

    def is_empty(self):
        return len(self.stack) == 0


class Queue:
    def __init__(self):
        self.queue = []
        self.unique_items = set()

    def enqueue(self, val):
        if val not in self.unique_items:
            self.queue.insert(0, val)
            self.unique_items.add(val)

    def dequeue(self):
        if self.is_empty():
            return None
        else:
            val = self.queue.pop()
            self.unique_items.remove(val)
            return val

    def is_empty(self):
        return len(self.queue) == 0

    def size(self):
        return len(self.queue)


class ThreadSafeQueue(SysQueue):

    def __init__(self):
        SysQueue.__init__(self)
        self.unique_items = set()

    def put(self, val):
        if val not in self.unique_items:
            SysQueue.put(self, val)
            self.unique_items.add(val)

    def get(self):
        if self.empty():
            return None
        else:
            val = SysQueue.get(self)
            self.unique_items.remove(val)
            return val