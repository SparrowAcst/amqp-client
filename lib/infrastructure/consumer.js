/**
 * @module Consumer
 */

const Middleware = require('../middlewares/wrapper');
const { v4 } = require('uuid');
const EventEmitter = require('node:events');

/**
 * Consumer
 */

class Consumer extends EventEmitter {
  /**
   * посилання на amqp-manager
   * {@link  ???} AmqpManager*/
  #manager

  /**
   * @property {URL} connection - посилання на з'єднання з amqp
   */
  #connection;

  /**
   * @property {Object} client - UUID клієнта генерується під час створення екземпляра класу
   */
  #client;

  /**
   * @property {Object} options - налаштування Публікувальника повідомлень
   */
  #options;

  /**
   * @property {Object} channel - посилання на екземпляр каналу
   */
  #channel;

  /**
   * @property {Object} queue - черга для обробки повідомлень
   */
  #queue;

  /**
   * @property {URL} middleware - посилання на ланцюжок оброблення повыдомлень
   * {@link use}
   * {@link execute} (../middlewares/wrapper.js)
   */
  #middleware;

  /**
   * створює екземпляр отримувача
   * @param manager - примірник amqp-manager {@link ???}(./amqp-manager.js)
   * @param options - налаштування отримувача {@link options}
   * @property client - {@see client}
   * */
  constructor(manager, options) {
    this.#manager = manager;
    this.#client = v4();
    this.#options = options;
  }

  /** гетер для з'єднання з amqp
   * @returns {Object} - {@see connection}
   * */
  get connection() {
    return this.#connection;
  }

  /**
   * гетер для UUID клієнта
   * @returns {Object} - {@see client}
   */
  get client() {
    return this.#client;
  }

  /**
   * гетер для налаштування Публікувальника повідомлень
   * @returns {Object} - {@see client}
   */
  get options() {
    return this.#options;
  }

  /**
   * гетер для посилання на екземпляр каналу
   * @returns {URL} - {@see client}
   */
  get channel() {
    return this.#channel;
  }

  /**
   * гетер для черги для обробки повідомлень
   * @returns {Object} - {@see queue}
   */
  get queue() {
    return this.#queue;
  }

  /**
   * гетер для посилання на ланцюжок оброблення повыдомлень
   * @returns {URL} - {@see client}
   */
  get middleware() {
    return this.#middleware;
  }
  /**
   * Ініціалізація Consumer-залежностей
   */
  async moduleInit() {
    this.#connection = await this.#manager.getConnection(this.#client, this.#options.amqp);
    this.#channel = await this.#connection.createChannel();
    await this.#channel.assertExchange(
      this.#options.queue.exchange.name,
      this.#options.queue.exchange.mode,
      this.#options.queue.exchange.options,
    );
    const assertion = await this.#channel.assertQueue(
      this.#options.queue.name,
      this.#options.queue.options,
    );

    this.#queue = assertion.queue;
    
    if(!Number.isNaN(Number.parseInt(this.#options.queue.options.prefetch))) this.#channel.prefetch(this.#options.queue.options.prefetch)
    
    await this.#channel.bindQueue(
      this.#queue,
      this.#options.queue.exchange.name,
      '',
    );
    this.#middleware = new Middleware();
  }

  /**
   * Додає callback до ланцюжка оброблення повідомлень {@link middleware}
   * @param callback
   * */
  use(callback) {
    this.#middleware.use(callback);
    return this;
  }

  /**
   * Запускає ланцюжок оброблення повідомлень
   */
  async start() {
    try {
      await this.#channel.consume(
        this.#queue,
        async (msg) => {
          const ack = () => this.#channel.ack(msg);
          const nack = () => this.#channel.nack(msg);
          try {
            await this.#middleware.execute({ content: msg.content, ack, nack });
          } catch (e) {
            throw e;
          }
        },
        this.#options.queue.options
      );
    } catch (err) {
      throw err;
    }
  }

  /**
   * Закриває канал по клієнту
   */
  async close(callback) {
    await this.#channel.close();
    if( callback) {
      await callback(this.#client);  
    } else {
      this.#manager.closeConnection(this.#client)
    }
  }
}

module.exports = Consumer;
