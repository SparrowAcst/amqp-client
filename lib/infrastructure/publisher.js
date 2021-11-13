import Middleware from '../middlewares/wrapper';

class Publisher {
  #connection;
  #client;
  #options;
  #channel;
  #middleware;

  constructor(connection, client, options) {
    this.#connection = connection;
    this.#client = client;
    this.#options = options;
  }

  get connection() {
    return this.#connection;
  }

  get client() {
    return this.#client;
  }

  async moduleInit() {
    this.#channel = await this.#connection.createChannel();
    await this.#channel.assertExchange(
      this.#options.exchange.name,
      this.#options.exchange.mode,
      this.#options.exchange.options,
    );
    this.#middleware = new Middleware(async (err, msg, next) => {
      if (err) throw err;
      await this.#channel.publish(
        this.#options.exchange.name,
        '',
        Buffer.from(msg.content),
      );
      next();
    });
  }

  use(callback) {
    this.#middleware.use(callback);
    return this;
  }

  async send(msg) {
    try {
      await this.#middleware.execute({ content: msg });
    } catch (e) {
      throw e;
    }
  }

  async close(callback) {
    await this.#channel.close();
    await callback(this.#client);
  }
}

export default Publisher;