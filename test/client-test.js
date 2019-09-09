const ServiceBusRPC = require('..');

async function main() {

    const rpc = new ServiceBusRPC.Client({
        connectionString: process.env.CONNSTR,
        queueName: process.env.QUEUE_NAME
    });
    rpc.on('warning', err => {
        console.log('WARNING', err);
    });
    rpc.on('closed', () => {
        console.log('client closed');
    });

    try {
        let rand = Math.random();
        console.log(`echo(${rand})`);
        console.log(await rpc.call('test6', 'echo', {rand}, {ttl: 50000}));
        console.log('5 x 3 =');
        console.log(await rpc.call('test7', 'mul', {a: 5, b: 3}, {ttl: 50000}));
        console.log('Call mul() on bad host [error expected]');
        console.log(await rpc.call('test6', 'mul', {a: 5, b: 3}, {ttl: 50000}));
    } catch (err) {
        console.log(`test err`, err);
    } finally {
        await rpc.close();
        console.log('closed');
    }

}
main();
