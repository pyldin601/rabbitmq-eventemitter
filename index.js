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
		var channelOptions = options.channelOptions || {};
		var prefetch = channelOptions.prefetch;

		self._getConnection(function(err, connection) {
			if(err) return callback(err);

			connection.createChannel(function(err, channel) {
				if(err) return callback(err);
				channel.on('error', onerror);

				if(prefetch) {
					var onprefetch = afterAll(function(err) {
						if(err) return callback(err);
						callback(null, channel);
					});

					if(prefetch.global) channel.prefetch(prefetch.global, true, onprefetch());
					if(prefetch.local) channel.prefetch(prefetch.local, false, onprefetch());
				} else {
					callback(null, channel);
				}
			});
		});
	};

	this._exchangeName = options.exchange || EXCHANGE;
	this._waitQueues = {};
	this._onerror = onerror;
	this._getConsumeChannel = thunky(getChannel);
	this._getPublishChannel = thunky(getChannel);

	var namespace = options.namespace;

	this._exchangeOptions = extend({
		durable: true,
		autoDelete: false
	}, options.exchangeOptions);

	this._queueOptions = extend({
		namespace: namespace || rs.generate(16),
		durable: !!namespace,
		autoDelete: !namespace,
		expires: QUEUE_TTL
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
			channel.publish(exchange, pattern, content, options);
			callback();
		};

		if(delay) self._assertWaitQueue(channel, pattern, delay, publish);
		else publish(null, self._exchangeName);
	});
};

Queue.prototype.pull = function(pattern, listener, options, callback) {
	if(!callback && typeof options === 'function') {
		callback = options;
		options = null;
	}

	var self = this;

	options = options || {};
	callback = callback || function(err) {
		if(err) self._onerror(err);
	};

	var queueName = this._queueName(pattern);
	var exchangeOptions = this._exchangeOptions;
	var queueOptions = extend(this._queueOptions, options.queueOptions);

	this._getConsumeChannel(function(err, channel) {
		if(err) return callback(err);

		var onmessage = function(message) {
			// channel.consume eats uncaught exceptions
			process.nextTick(function() {
				var data = JSON.parse(message.content.toString());
				var onresponse = function(err) {
					if(util.isError(err)) channel.nack(message, false, true);
					else channel.ack(message, false);
				};

				if(listener.length <= 2) listener(data, onresponse);
				else listener(data, message, onresponse);
			});
		};

		var next = afterAll(callback);

		channel.assertExchange(self._exchangeName, 'direct', {
			durable: exchangeOptions.durable,
			autoDelete: exchangeOptions.autoDelete
		}, next());

		channel.assertQueue(queueName, {
			durable: queueOptions.durable,
			autoDelete: queueOptions.autoDelete,
			arguments: { 'x-expires': queueOptions.expires }
		}, next());

		channel.bindQueue(queueName, self._exchangeName, pattern, null, next());
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

Queue.prototype._assertWaitQueue = function(channel, pattern, delay, callback) {
	var self = this;
	var waitExchangeName = this._waitExchangeName(delay);
	var waitQueueName = this._waitQueueName(pattern, delay);

	var fn = this._waitQueues[waitQueueName];

	if(!fn) {
		fn = thunky(function(cb) {
			var next = afterAll(function(err) {
				cb(err, waitExchangeName);
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
					'x-dead-letter-exchange': self._exchangeName
				}
			}, next());

			channel.bindQueue(waitQueueName, waitExchangeName, pattern, null, next());
		});

		this._waitQueues[waitQueueName] = fn;
	}

	fn(callback);
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
