# rabbitmq-eventemitter

Simplified rabbitmq events.

	npm install rabbitmq-eventemitter

# Usage

The returned instance exposes a `pull` method for receiving and a `push` method for sending events.

```javascript
var rabbitmq = require('rabbitmq-eventemitter');

var queue = rabbitmq('amqp://localhost');

queue.pull('event_name', function(message, callback) {
	console.log(message);
	callback();
});

queue.push('event_name', 'hello');
```

Call the provided `callback` in `push` to acknowledge the message and remove it from the queue. If the `callback` is called with an error object as first argument the message is inserted back into the queue.

#### Delay

It's also possible to delay message delivery using the `delay` option.

```javascript
queue.push('event_name', 'hello in 5 seconds', { delay: 5000 });
```

#### Namespace

The `namespace` option allows you to control how messages are distributed between consumers. Only one consumer within the same namespace will receive a published message, even though there are others consumers listening on the same event name, this works well for worker queues, where you would have multiple processes receiving messages to be executed. Using different namespaces will on the other hand result in every consumer listening on the same event name to receive the message, which is usefull for a publish-subscribe setup.

```javascript
var workerQueue_1 = rabbitmq('amqp://localhost', { namespace: 'task-queue' });
var workerQueue_2 = rabbitmq('amqp://localhost', { namespace: 'task-queue' });
var publishQueue = rabbitmq('amqp://localhost'); // namespace not needed when publishing

// Only one of the handlers is called
workerQueue_1.pull('task', function(message, callback) {
	console.log(message);
	callback();
});

workerQueue_2.pull('task', function(message, callback) {
	console.log(message);
	callback();
});

publishQueue.push('task', 'work work');
```

```javascript
var pubsubQueue_1 = rabbitmq('amqp://localhost', { namespace: 'pubsub-queue-1' });
var pubsubQueue_2 = rabbitmq('amqp://localhost', { namespace: 'pubsub-queue-2' });
var publishQueue = rabbitmq('amqp://localhost'); // namespace not needed when publishing

// Both handlers called.
pubsubQueue_1.pull('task', function(message, callback) {
	console.log(message);
	callback();
});

pubsubQueue_2.pull('task', function(message, callback) {
	console.log(message);
	callback();
});

publishQueue.push('task', 'hello all');
```

If no namespace is provided, it defaults to a random string.
