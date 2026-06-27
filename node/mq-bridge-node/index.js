"use strict";

const native = require("./native.js");

class Message {
  constructor(payload, metadata = null, id = null) {
    this._native = native.createMessage(Buffer.from(payload), metadata, id);
  }

  static fromJson(data, metadata = null, id = null) {
    return Message._fromNative(native.messageFromJson(data, metadata, id));
  }

  static _fromNative(raw) {
    const message = Object.create(Message.prototype);
    message._native = raw;
    return message;
  }

  static _toNative(message) {
    if (message instanceof Message) {
      return message._native;
    }
    return message;
  }

  static _toNativeResult(result) {
    if (result == null) {
      return null;
    }
    return Message._toNative(result);
  }

  get payload() {
    return Buffer.from(this._native.payload);
  }

  get metadata() {
    return { ...(this._native.metadata || {}) };
  }

  get id() {
    return this._native.id || null;
  }

  json() {
    return native.messageJson(this._native);
  }

  text() {
    return native.messageText(this._native);
  }
}

class Publisher {
  constructor(nativePublisher) {
    this._native = nativePublisher;
  }

  static fromFile(path, name) {
    return new Publisher(native.Publisher.fromFile(path, name));
  }

  static fromStr(text, name) {
    return new Publisher(native.Publisher.fromStr(text, name));
  }

  static fromConfig(config, name) {
    return new Publisher(native.Publisher.fromConfig(config, name));
  }

  /** @deprecated Use {@link Publisher.fromFile} instead. */
  static fromYaml(path, name) {
    return new Publisher(native.Publisher.fromFile(path, name));
  }

  /** @deprecated Use {@link Publisher.fromStr} instead. */
  static fromYamlStr(text, name) {
    return new Publisher(native.Publisher.fromStr(text, name));
  }

  send(message) {
    return this._native.send(Message._toNative(message));
  }

  async request(message) {
    return Message._fromNative(await this._native.request(Message._toNative(message)));
  }

  sendJson(data, metadata = null, id = null) {
    return this._native.sendJson(data, metadata, id);
  }

  async requestJson(data, metadata = null, id = null) {
    return Message._fromNative(await this._native.requestJson(data, metadata, id));
  }
}

class Consumer {
  constructor(nativeConsumer) {
    this._native = nativeConsumer;
  }

  static fromFile(path, name) {
    return new Consumer(native.Consumer.fromFile(path, name));
  }

  static fromStr(text, name) {
    return new Consumer(native.Consumer.fromStr(text, name));
  }

  static fromConfig(config, name) {
    return new Consumer(native.Consumer.fromConfig(config, name));
  }

  async poll(max, timeoutMs) {
    const messages = await this._native.poll(max, timeoutMs);
    return messages.map((message) => Message._fromNative(message));
  }

  commit() {
    return this._native.commit();
  }

  get exhausted() {
    return this._native.exhausted;
  }
}

class Route {
  constructor(nativeRoute) {
    this._native = nativeRoute;
  }

  static fromFile(path, name) {
    return new Route(native.Route.fromFile(path, name));
  }

  static fromStr(text, name) {
    return new Route(native.Route.fromStr(text, name));
  }

  static fromConfig(config, name) {
    return new Route(native.Route.fromConfig(config, name));
  }

  /** @deprecated Use {@link Route.fromFile} instead. */
  static fromYaml(path, name) {
    return new Route(native.Route.fromFile(path, name));
  }

  /** @deprecated Use {@link Route.fromStr} instead. */
  static fromYamlStr(text, name) {
    return new Route(native.Route.fromStr(text, name));
  }

  withHandler(handler) {
    this._native.withHandler(async (error, message) => {
      if (error) {
        throw error;
      }
      const result = await handler(Message._fromNative(message));
      return Message._toNativeResult(result);
    });
  }

  addHandler(kind, handler) {
    this._native.addHandler(kind, async (error, data) => {
      if (error) {
        throw error;
      }
      const result = await handler(data);
      return Message._toNativeResult(result);
    });
  }

  start() {
    this._native.start();
  }

  stop() {
    this._native.stop();
  }

  join() {
    this._native.join();
  }
}

function configSchema() {
  if (typeof native.configSchema !== "function") {
    throw new Error(
      "configSchema() is unavailable: this build was compiled without the 'schema' feature",
    );
  }
  return native.configSchema();
}

module.exports = {
  Message,
  Publisher,
  Consumer,
  Route,
  configSchema,
  version: native.VERSION,
};
