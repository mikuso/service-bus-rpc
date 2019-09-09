const ServiceBusRPC = require('..');

class TestTool {
    async echo(what) {
        return what;
    }
}

class TestTool2 {
    mul({a, b}) {
        return a*b;
    }
}

async function main() {

    const rpc = new ServiceBusRPC.Server({
        connectionString: process.env.CONNSTR,
        queueName: process.env.QUEUE_NAME
    });
    rpc.on('warning', err => {
        console.log('WARNING', err);
    });
    rpc.on('closed', () => {
        console.log('server closed');
    });

    rpc.listen('test6', new TestTool());
    rpc.listen('test7', new TestTool2());

    function close() {
        rpc.close();
    }

    process.on('SIGINT', close);
    process.on('SIGTERM', close);

}
main();
