var test = require('tape');
var request = require('request');
var waterfall = require('run-waterfall');
var afterAll = require('after-all');

var queue = require('../');

var CONNECTION_URL = 'amqp://localhost';
var QUEUE_OPTIONS = {
	durable: false,
	autoDelete: true
};

var noopHandler = function(message, callback) {
	callback();
};

var rabbit = request.defaults({
	baseUrl: 'http://guest:guest@localhost:15672',
	jar: false,
	pool: false,
	json: true
});

var createQueue = function(namespace) {
	var queueOptions = {
		durable: false,
		autoDelete: true
	};

	return queue('amqp://localhost', {
		namespace: (namespace || 'test-namespace'),
		queueOptions: queueOptions
	});
};

test('pull created queue', function(t) {
	var q = createQueue();

	waterfall([
		function(next) {
			q.pull('test-pattern', noopHandler, next);
		},
		function(next) {
			rabbit('/api/queues/%2f/test-namespace.test-pattern', next);
		},
		function(response, body, next) {
			t.equals(response.statusCode, 200);
			q.close(next);
		},
		function(next) {
			rabbit('/api/queues/%2f/test-namespace.test-pattern', next);
		},
		function(response, body, next) {
			t.equals(response.statusCode, 404);
			next();
		}
	], function(err) {
		t.error(err);
		t.end();
	});
});

test('pull and push queue', function(t) {
	var q = createQueue();

	t.plan(3);

	var handler = function(message, callback) {
		t.deepEqual(message, { ok: 1 });
		callback();

		q.close(function(err) {
			t.error(err);
		});
	};

	waterfall([
		function(next) {
			q.pull('test-pattern', handler, next);
		},
		function(next) {
			q.push('test-pattern', { ok: 1 }, next);
		}
	], function(err) {
		t.error(err);
	});
});

test('pull twice same namespace', function(t) {
	var q1 = createQueue();
	var q2 = createQueue();
	var q3 = createQueue();

	t.plan(3);

	var handler = function(message, callback) {
		t.deepEqual(message, { ok: 1 });
		callback();

		var next = afterAll(function(err) {
			t.error(err);
		});

		q1.close(next());
		q2.close(next());
		q3.close(next());
	};

	waterfall([
		function(next) {
			q1.pull('test-pattern', handler, next);
		},
		function(next) {
			q2.pull('test-pattern', handler, next);
		},
		function(next) {
			q3.push('test-pattern', { ok: 1 }, next);
		}
	], function(err) {
		t.error(err);
	});
});

test('pull twice different namespace', function(t) {
	var q1 = createQueue('test-namespace-1');
	var q2 = createQueue('test-namespace-2');
	var q3 = createQueue();

	t.plan(4);

	var onmessage = afterAll(function() {
		var onclose = afterAll(function(err) {
			t.error(err);
		});

		q1.close(onclose());
		q2.close(onclose());
		q3.close(onclose());
	});

	var handler1 = onmessage(function(message, callback) {
		t.deepEqual(message, { ok: 1 });
		callback();
	});

	var handler2 = onmessage(function(message, callback) {
		t.deepEqual(message, { ok: 1 });
		callback();
	});

	waterfall([
		function(next) {
			q1.pull('test-pattern', handler1, next);
		},
		function(next) {
			q2.pull('test-pattern', handler2, next);
		},
		function(next) {
			q3.push('test-pattern', { ok: 1 }, next);
		}
	], function(err) {
		t.error(err);
	});
});

test('push with delay', function(t) {
	var q = createQueue();

	t.plan(3);

	var handler = function(message, callback) {
		t.deepEqual(message, { ok: 1 });
		callback();

		q.close(function(err) {
			t.error(err);
		});
	};

	waterfall([
		function(next) {
			q.pull('test-pattern', handler, next);
		},
		function(next) {
			q.push('test-pattern', { ok: 1 }, { delay: 1000 }, next);
		}
	], function(err) {
		t.error(err);
	});
});
