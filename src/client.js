const {ServiceBusClient, ReceiveMode} = require('@azure/service-bus');
const uuid = require('uuid');
const EventEmitter = require('eventemitter3');
const Receiver = require('./receiver');
const Sender = require('./sender');
const {TimeoutError} = require('./errors');

const DEFAULT_TTL = 1000*30;

class ServiceBusRPCClient extends EventEmitter {
    constructor({connectionString, queueName}) {
        super();
        this.client = ServiceBusClient.createFromConnectionString(connectionString);
        this.queueClient = this.client.createQueueClient(queueName);
        this.uuid = uuid.v4();
        this.calls = new Map();

        this.receiver = new Receiver({queueClient: this.queueClient, sessionId: this.uuid});
        this.sender = new Sender({queueClient: this.queueClient});

        this.receiver.on('message', this.onReceiverMessage, this);
        this.receiver.on('warning', this.onReceiverWarning, this);
        this.sender.on('warning', this.onSenderWarning, this);

        this.receiver.start();
    }

    onSenderWarning(err) {
        this.emit('warning', err);
    }

    onReceiverWarning(err) {
        this.emit('warning', err);
    }

    onReceiverMessage(msg) {
        const call = this.calls.get(msg.body.callId);

        if (!call) {
            return;
        }

        switch (true) {
            case (!!msg.body.acknowledgement):
                call.acknowledge();
                break;
            case (!!msg.body.result):
                this.calls.delete(msg.body.callId);
                call.resolve(msg.body.result);
                break;
            case (!!msg.body.error):
                this.calls.delete(msg.body.callId);
                const err = Error(msg.body.error.message);
                err.name = msg.body.error.name;
                err.code = msg.body.error.code;
                err.details = msg.body.error.details;
                err.stack = msg.body.error.stack;
                call.reject(err);
                break;
        }

        msg.complete().catch(() => {});
    }

    async call(sessionId, method, args, options = {}) {
        const ttl = options.ttl || DEFAULT_TTL;

        const messageId = uuid.v4();
        const message = {
            messageId,
            body: {method, args},
            sessionId,
            replyTo: this.uuid,
            timeToLive: ttl
        };

        let resolve, reject;
        const resolved = new Promise((res,rej)=> {
            resolve = res;
            reject = rej;
        });

        let timeout;
        const stopTimeout = () => {
            if (timeout) {
                clearTimeout(timeout);
                timeout = null;
            }
        };
        const refreshTimeout = (ms) => {
            stopTimeout();
            timeout = setTimeout(() => reject(new TimeoutError("Timeout")), ms);
        };

        const acknowledge = () => {
            refreshTimeout(ttl);
        };

        this.calls.set(messageId, {
            message,
            resolve,
            reject,
            acknowledge
        });
        refreshTimeout(ttl);

        this.sender.send(message);

        return resolved.finally(stopTimeout);
    }

    async close() {
        this.receiver.removeAllListeners();
        this.sender.removeAllListeners();

        await this.receiver.stop();
        await this.queueClient.close();
        await this.client.close();

        this.emit('closed');
        this.removeAllListeners();
    }
}

module.exports = ServiceBusRPCClient;
