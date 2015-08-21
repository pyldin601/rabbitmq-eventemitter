var util = require('util');
var events = require('events');
var amqp = require('amqplib/callback_api');
var thunky = require('thunky');
var once = require('once');
var afterAll = require('after-all');
var rs = require('randomstring');
var extend = require('xtend');

var QUEUE_TTL = 2 * 24 * 60 * 60 * 1000;
var EXCHANGE = 'globalexchange';

var Queue = function(url, options) {
	if(!(this instanceof Queue)) return new Queue(url, options);
	events.EventEmitter.call(this);

	options = options || {};

	var self = this;

	var onerror = once(function(err) {
		self.emit('error', err);
	});

	this._getConnection = thunky(function(callback) {
		amqp.connect(url, options.connectOptions, function(err, connection) {
			if(err) return callback(err);

			connection.on('error', onerror);
			callback(null, connection);
		});
	});

	var getChannel = function(callback) {
		self._getConnection(function(err, connection) {
			if(err) return callback(err);

			connection.createChannel(function(err, channel) {
				if(err) return callback(err);

				channel.on('error', onerror);
				callback(null, channel);
			});
		});
	};

	this._onerror = onerror;
	this._getConsumeChannel = thunky(getChannel);
	this._getPublishChannel = thunky(getChannel);

	var namespace = options.namespace;

	this._queueOptions = extend({
		namespace: namespace || rs.generate(16),
		durable: !!namespace,
		autoDelete: !namespace
	}, options.queueOptions);
};

util.inherits(Queue, events.EventEmitter);

Queue.prototype.push = function(pattern, data, options, callback) {
	if(!callback && typeof options === 'function') {
		callback = options;
		options = null;
	}

	var self = this;

	options = options || {};
	callback = callback || function(err) {
		if(err) self._onerror(err);
	};

	this._getPublishChannel(function(err, channel) {
		if(err) return callback(err);

		var delay = options.delay;
		var publish = function(err, exchange) {
			if(err) return callback(err);

			var content = new Buffer(JSON.stringify(data));
			channel.publish(exchange, pattern, content);
			callback();
		};

		if(delay) {
			var waitExchangeName = self._waitExchangeName(delay);
			var waitQueueName = self._waitQueueName(pattern, delay);
			var next = afterAll(function(err) {
				publish(err, waitExchangeName);
			});

			channel.assertExchange(waitExchangeName, 'direct', {
				durable: true,
				autoDelete: false
			}, next());

			channel.assertQueue(waitQueueName, {
				durable: true,
				autoDelete: false,
				arguments: {
					'x-message-ttl': delay,
					'x-dead-letter-exchange': EXCHANGE
				}
			}, next());

			return channel.bindQueue(waitQueueName, waitExchangeName, pattern, null, next());
		}

		publish(null, EXCHANGE);
	});
};

Queue.prototype.pull = function(pattern, listener, callback) {
	var self = this;
	var queueName = this._queueName(pattern);
	var queueOptions = this._queueOptions;

	callback = callback || function(err) {
		if(err) self._onerror(err);
	};

	this._getConsumeChannel(function(err, channel) {
		if(err) return callback(err);

		var onmessage = function(message) {
			// channel.consume eats uncaught exceptions
			process.nextTick(function() {
				var data = JSON.parse(message.content.toString());

				listener(data, function(err) {
					if(util.isError(err)) channel.nack(message, false, false);
					else channel.ack(message, false);
				});
			});
		};

		var next = afterAll(callback);

		channel.assertExchange(EXCHANGE, 'direct', {
			durable: true,
			autoDelete: false
		}, next());

		channel.assertQueue(queueName, {
			durable: queueOptions.durable,
			autoDelete: queueOptions.autoDelete,
			arguments: { 'x-expires': QUEUE_TTL }
		}, next());

		channel.bindQueue(queueName, EXCHANGE, pattern, null, next());
		channel.consume(queueName, onmessage, null, next());
	});
};

Queue.prototype.close = function(callback) {
	this._getConnection(function(err, connection) {
		if(err) return callback(err);

		connection.close(function(err) {
			callback(err);
		});
	});
};

Queue.prototype._queueName = function(pattern) {
	return this._queueOptions.namespace + '.' + pattern;
};

Queue.prototype._waitQueueName = function(pattern, delay) {
	return 'waitqueue-' + delay + '.' + pattern;
};

Queue.prototype._waitExchangeName = function(delay) {
	return 'waitexchange-' + delay;
};

module.exports = Queue;
