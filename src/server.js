const {ServiceBusClient, ReceiveMode} = require('@azure/service-bus');
const uuid = require('uuid');
const EventEmitter = require('eventemitter3');
const Receiver = require('./receiver');
const Sender = require('./sender');
const {MethodNotFoundError} = require('./errors');

class ServiceBusRPCServer extends EventEmitter {
    constructor({connectionString, queueName}) {
        super();
        this.client = ServiceBusClient.createFromConnectionString(connectionString);
        this.queueClient = this.client.createQueueClient(queueName);
        this.uuid = uuid.v4();
        this.sender = new Sender({queueClient: this.queueClient});
        this.sender.on('warning', this.onSenderWarning, this);

        this.sessions = new Map();
    }

    onSenderWarning(err) {
        this.emit('warning', err);
    }

    async listen(sessionId, rpcHost) {
        if (this.sessions.has(sessionId)) {
            return;
        }

        const receiver = new Receiver({queueClient: this.queueClient, sessionId});
        this.sessions.set(sessionId, {receiver, rpcHost});

        receiver.on('warning', (err) => {
            err.sessionId = sessionId;
            this.emit('warning', err);
        });

        receiver.on('message', async (msg) => {
            try {
                if (!(rpcHost[msg.body.method] instanceof Function)) {
                    throw new MethodNotFoundError(`Method "${msg.body.method}" does not exist`);
                }
                this.reply(msg, {acknowledgement: true});
                const result = await rpcHost[msg.body.method](msg.body.args);
                this.reply(msg, {result});
            } catch (err) {
                this.reply(msg, {error: {
                    name: err.name,
                    code: err.code,
                    details: err.details,
                    stack: err.stack,
                }});
            }
            msg.complete().catch(()=>{});
        });

        await receiver.start();
    }

    async reply(msg, content) {
        await this.sender.send({
            sessionId: msg.replyTo,
            timeToLive: 1000*60,
            body: {
                callId: msg.messageId,
                ...content
            }
        });
    }

    async closeSession(sessionId) {
        const session = this.sessions.get(sessionId);
        if (!session) {
            return;
        }
        this.sessions.delete(sessionId);
        session.receiver.removeAllListeners();
        await session.receiver.stop();
        session.rpcHost = null;
    }

    async close() {
        await Promise.all(Array.from(this.sessions.entries()).map(([sessionId, session]) => {
            session.receiver.removeAllListeners();
            const stopping = session.receiver.stop();
            this.sessions.delete(sessionId);
            return stopping;
        }));

        this.emit('closed');
        this.removeAllListeners();
        this.queueClient.close();
        this.client.close();
    }

}

module.exports = ServiceBusRPCServer;
