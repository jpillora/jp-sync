//wait on all provided promises and insert
//results back into the original array
exports.wait = async (doMap, promises) => {
  //single arg?
  if (promises === undefined && Array.isArray(doMap)) {
    promises = doMap;
    doMap = true; //in most cases, we dont need the promise after its resolved
  } else if (!Array.isArray(promises)) {
    throw `Expected array of promises`;
  }
  return new Promise((resolve, reject) => {
    let n = promises.length;
    if (n === 0) {
      resolve(promises);
      return;
    }
    let done = false;
    promises.forEach((promise, i) => {
      //wipe from array
      promises[i] = undefined;
      //store results
      promise.then(
        result => {
          if (doMap) {
            promises[i] = result;
          }
          n--;
          if (!done && n === 0) {
            done = true;
            resolve(promises);
          }
        },
        err => {
          if (!done) {
            done = true;
            reject(err);
          }
        }
      );
    });
  });
};

//normal Array.map with async concurrency.
//doMap controls whether the returned results are kept.
//async "series" is equivalent to setting concurrency to 1.
const loop = async function(doMap, concurrency, arr, fn) {
  //omit concurrency use maximum
  if (Array.isArray(concurrency) && typeof arr === "function") {
    fn = arr;
    arr = concurrency;
    concurrency = Infinity;
  }
  //validate
  if (typeof concurrency !== "number") throw "expecting concurrency integer";
  if (!Array.isArray(arr)) throw "expecting array";
  if (typeof fn !== "function") throw "expecting map function";
  //case, concurrency >= size
  //equiv to map->promise.all
  if (concurrency >= arr.length) {
    let promises = arr.map((d, i) => {
      let promise = fn(d, i);
      if (!promise || !(promise instanceof Promise)) {
        throw new Error("Function must return a promise");
      }
      return promise;
    });
    return await exports.wait(doMap, promises);
  }
  //case, concurrency <= size
  //need workers and a queue.
  //start!
  let queue = new Array(arr.length);
  let error = null;
  async function worker() {
    //worker looks through all entries,
    //skipping entries already being worked on
    for (let i = 0; i < arr.length; i++) {
      if (error) {
        break;
      }
      //if queue[i] is undefined, then job "i" is free
      //if queue[i] is true,      then job "i" is taken
      //NOTE: node is single-threaded so this is fine :)
      if (queue[i]) {
        continue;
      }
      queue[i] = true;
      //start!
      try {
        let d = arr[i];
        let promise = fn(d, i);
        if (!promise || !(promise instanceof Promise)) {
          throw new Error("Function must return a promise");
        }
        let result = await promise;
        //replace value with result
        if (doMap) {
          arr[i] = result;
        }
      } catch (err) {
        //mark failure to ensure no more new jobs are accepted,
        //but still complete the remaining jobs inflight.
        error = err;
      }
    }
    return;
  }
  //create <concurrency> many workers
  let workers = [];
  for (let w = 1; w <= concurrency; w++) {
    workers.push(worker());
  }
  //start <concurrency> many workers,
  //blocks until all return
  await Promise.all(workers);
  //encountered an error, throw it!
  if (error !== null) {
    throw error;
  }
  //done!
  return arr;
};

//aliases for loop (only map edits the original array)
exports.map = loop.bind(null, true);
exports.each = loop.bind(null, false);
exports.forEach = exports.each;

//NOTE: we used to have a exports.series, which looped with concurrency 1,
//this was removed in favour of a simple "for(const item of arr) {...}" loop.

//provide a number instead of array => each(concurrency, [0..n), fn)
exports.times = async (concurrency, n, fn) => {
  let indices = new Array(n).fill(0).map((_, i) => i);
  return await exports.each(concurrency, indices, fn);
};

//token bucket implements a rate limiter.
//size represents the maximum number of tokens
//that can be handed out at any given time.
exports.TokenBucket = class TokenBucket {
  constructor(size) {
    this._tokens = {};
    this._numTokens = 0;
    this._maxTokens = size;
    this._queue = [];
    this._numTakes = 0;
  }
  get numTokens() {
    return this._numTokens;
  }
  get maxTokens() {
    return this._maxTokens;
  }
  get numQueued() {
    return this._queue.length;
  }
  get numTotal() {
    return this.numTokens + this.numQueued;
  }
  //take a token from the bucket!
  async take() {
    const tid = ++this._numTakes;
    const t = Symbol(tid);
    //DEBUG const log = console.log.bind(console, `[${tid}]`);
    //bucket has no more tokens, wait for
    //one to be put back...
    if (this.numTotal >= this.maxTokens) {
      let dequeue = null;
      let promise = new Promise(d => (dequeue = d));
      this._queue.push(dequeue);
      await promise;
    }
    //take token from the bucket
    this._numTokens++;
    this._tokens[t] = true;
    //put the token back into the bucket
    const put = () => {
      if (!this._tokens[t]) {
        throw "invalid token";
      }
      this._numTokens--;
      delete this._tokens[t];
      if (this._queue.length > 0) {
        let dequeue = this._queue.shift();
        dequeue();
      }
    };
    return put;
  }
  //wrapper around take + put
  async run(fn) {
    let put = await this.take();
    try {
      return await fn(); //proxy
    } catch (error) {
      throw error; //proxy
    } finally {
      put();
    }
  }
};

//a queue is simply a one-slot token bucket
exports.queue = () => new exports.TokenBucket(1);

//sleep(milliseconds) !
exports.sleep = async ms => new Promise(r => setTimeout(r, ms));

exports.sleepUntilInterupt = async () => {
  let resolve = null;
  const p = new Promise(r => (resolve = r));
  process.stdin.resume();
  process.once("SIGINT", () => {
    resolve();
  });
  return p;
};

//promisify a function. unlike node's
//util.promisify, this still calls the original
//callback. return values are lost since a promise
//must be returned.
exports.promisify = function(fn) {
  return function wrapped() {
    //store promise on callback
    var resolve, reject;
    var promise = new Promise(function(res, rej) {
      resolve = res;
      reject = rej;
    });
    //
    var args = Array.from(arguments);
    var callback = args[args.length - 1];
    var hasCallback = typeof callback === "function";
    //resolve/reject promise
    var fullfilled = function(err, data) {
      if (err) {
        reject(err);
      } else {
        var datas = Array.prototype.slice.call(arguments, 1);
        if (datas.length >= 2) {
          resolve(datas);
        } else {
          resolve(data);
        }
      }
      if (hasCallback) {
        callback.apply(this, arguments);
      }
    };
    //replace/add callback
    if (hasCallback) {
      args[args.length - 1] = fullfilled;
    } else {
      args.push(fullfilled);
    }
    //call underlying function
    var returned = fn.apply(this, args);
    if (typeof returned !== "undefined") {
      // console.log("promisify warning: discarded return value", returned);
    }
    //return promise!
    return promise;
  };
};

//run a diff against two sets.
exports.diff = async run => {
  if (!run || typeof run !== "object") {
    throw `diff: Invalid run`;
  }
  const { prev, next } = run;
  if (!Array.isArray(prev)) {
    throw `diff: Expected "prev" array`;
  } else if (!Array.isArray(next)) {
    throw `diff: Expected "next" array`;
  }
  //find "prev" indexer
  let { index, indexPrev = index, indexNext = index } = run;
  if (typeof indexPrev === "string") {
    const key = indexPrev;
    indexPrev = o => o[key];
  }
  if (typeof indexPrev !== "function") {
    throw `diff: Expected "index" function`;
  }
  //find "next" indexer
  if (typeof indexNext === "string") {
    const key = indexNext;
    indexNext = o => o[key];
  }
  if (typeof indexNext !== "function") {
    throw `diff: Expected "index" function`;
  }
  //find "equals" function,
  //by default, matching index implies matching items
  let { equal, equals } = run;
  if (!equal && equals) {
    equal = equals;
  }
  if (!equal) {
    equal = (p, n) => true;
  } else if (typeof equal !== "function") {
    throw `diff: Expected "equals" to be a function`;
  }
  //optional
  const { status } = run;
  //final results
  const results = {
    match: [],
    create: [],
    update: [],
    delete: []
  };
  //note and ignore duplicate
  const dupes = new Set();
  //construct join map (index-key => item)
  const join = {};
  for (const prevItem of prev) {
    const id = indexPrev(prevItem);
    if (id in join) {
      dupes.add(id);
    } else {
      join[id] = prevItem;
    }
  }
  if (dupes.size > 0) {
    results.duplicate = Array.from(dupes);
  }
  const joined = new Map();
  //compare incoming to existing
  for (const nextItem of next) {
    const id = indexNext(nextItem);
    const exists = id in join;
    if (!exists) {
      results.create.push(nextItem);
      continue;
    }
    const prevItem = join[id];
    if (equal(prevItem, nextItem)) {
      results.match.push(nextItem);
    } else {
      results.update.push(nextItem);
    }
    joined.set(nextItem, prevItem);
    delete join[id];
  }
  for (const id in join) {
    const prevItem = join[id];
    results.delete.push(prevItem);
  }
  //using results, build a set of all operations
  const operations = [];
  for (const op in results) {
    const set = results[op];
    if (!set || set.length === 0) {
      continue;
    }
    const fn = run[op];
    if (!fn) {
      continue;
    } else if (typeof fn !== "function") {
      throw `diff: Expected "${op}" to be a function`;
    }
    for (let item of set) {
      let other = joined.get(item);
      operations.push({ fn, item, other });
    }
  }
  //optionally report diff status
  if (status) status.add(operations.length);
  //execute all with <concurrency>
  const { concurrency = 1 } = run;
  await exports.each(concurrency, operations, async ({ fn, item, other }) => {
    let result = fn(item, other);
    if (result instanceof Promise) {
      await result;
    }
    //optionally report diff status
    if (status) status.done(1);
  });
  //done!
  return results;
};

//timer is a high-resolution timer:
//  let stop = timer.start()
//  let ms = stop();
exports.timer = {
  start() {
    const d0 = +new Date();
    const [s0, n0] = process.hrtime();
    const stop = () => {
      //use high-res timer if its a short timer
      const [s1, n1] = process.hrtime();
      const d1 = +new Date();
      const dms = d1 - d0;
      //anything longer than a second should use low res timer
      if (dms > 1000) {
        return dms;
      }
      //otherwise, use high-res timer
      const hrms = (s1 - s0) * 1e3 + (n1 - n0) / 1e6;
      return hrms.toPrecision(6);
    };
    return stop;
  }
};

exports.time = async (msg, fn) => {
  const stop = exports.timer.start();
  await fn();
  const ms = stop();
  console.log(`[sync.time] took ${ms}ms: ${msg}`);
};

//async versions of fs methods
const fs = require("fs");
exports.mkdir = exports.promisify(fs.mkdir);
exports.readdir = exports.promisify(fs.readdir);
exports._stat = exports.promisify(fs.stat);
exports.readFile = exports.promisify(fs.readFile);
exports.writeFile = exports.promisify(fs.writeFile);
exports.remove = exports.promisify(fs.unlink);

//custom fs methods

//stat async where missing returns null
//instead of throwing
exports.stat = async (...args) => {
  try {
    return await exports._stat(...args);
  } catch (err) {
    if (err.code === "ENOENT") {
      return null;
    }
    throw err;
  }
};

//stat mkDir where already exist null
//instead of throwing
exports.mkDir = async (...args) => {
  try {
    return await exports.mkdir(...args);
  } catch (err) {
    if (err.code === "EEXIST") {
      return null;
    }
    throw err;
  }
};

exports.readDir = async (...args) => {
  try {
    return await exports.readdir(...args);
  } catch (err) {
    return null;
  }
};

exports.readBytes = (filepath, n, offset = 0) => {
  return new Promise((resolve, reject) => {
    if (n === 0) {
      return resolve(Buffer.alloc(0));
    }
    fs.open(filepath, "r", (err, fd) => {
      if (err) {
        return reject(err);
      }
      let b = Buffer.alloc(n);
      fs.read(fd, b, 0, n, offset, (err, wrote) => {
        if (err) {
          fs.close(fd, () => {});
          return reject(err);
        }
        if (wrote < n) {
          b = b.slice(0, wrote);
        }
        fs.close(fd, err => {
          if (err) {
            return reject(err);
          }
          resolve(b);
        });
      });
    });
  });
};
