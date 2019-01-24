'use strict'

const EventEmitter = require('events')
const assert = require('assert')

const transform = require('lodash.transform')
const camelCase = require('lodash.camelcase')
const isPlainObject = require('lodash.isplainobject')

const LruCache = require('mnemonist/lru-cache')

const pify = require('pify')

const log = require('debug')('kitsunet:slice-tracker')

const DEFAULT_TOPIC = `kitsunet:slice`
const DEFAULT_SLICE_TIMEOUT = 80 * 1000
const DEFAULT_DEPTH = 10

const TRACK_SLICE = `kitsunet:slice:track`
const TRACK_STORAGE_SLICE = `kitsunet:slice:track-storage`

function normalizeKeys (obj) {
  return transform(obj, (result, value, key) => {
    if (key === 'metadata') { return }
    if (isPlainObject(value)) {
      value = normalizeKeys(value)
    }

    if (key.indexOf('-') > 0) {
      key = camelCase(key)
    }
    result[key] = value
  }, {})
}

function timeout (length) {
  return new Promise((resolve, reject) => {
    setTimeout(() => reject(new Error('timeout')), length)
  })
}

class KitsunetSliceTracker extends EventEmitter {
  constructor ({ node, blockTracker, depth }) {
    super()

    assert(node, 'node is required!')
    assert(node.multicast, 'node needs multicast!')
    assert(blockTracker, 'blockTracker is required!')

    this.node = node
    this.blockTracker = blockTracker
    this.topic = DEFAULT_TOPIC
    this.started = false
    this.depth = depth || DEFAULT_DEPTH

    this.forwardedSlices = new LruCache(1000)
    this.slices = new LruCache(100)
    this.waitingSlice = new Map()

    this.multicast = pify(this.node.multicast)

    this._track = (msg) => this._handleTrack(msg)
    this._trackStorage = (msg) => this._handleTrack(msg, true)

    this.slicesHook = this._slicesHook.bind(this)
    this.handleSlice = this._handleSlice.bind(this)
  }

  _slicesHook (peer, msg, cb) {
    let slice = null
    try {
      slice = normalizeKeys(JSON.parse(msg.data.toString()))
      if (!slice) {
        return cb(new Error(`No slice in message!`))
      }
    } catch (err) {
      log(err)
      return cb(err)
    }

    const peerId = peer.info.id.toB58String()
    const slices = this.forwardedSlices.get(peerId) || new LruCache(1000)
    if (!slices.has(slice.sliceId)) {
      slices.set(slice.sliceId, true)
      this.forwardedSlices.set(peerId, slices)
      return cb(null, msg)
    }

    const skipMsg = `already forwarded to peer ${peerId}, skipping slice ${slice.sliceId}`
    log(skipMsg)
    return cb(skipMsg)
  }

  async start () {
    this.multicast.subscribe(TRACK_SLICE, this._track)
    this.multicast.subscribe(TRACK_STORAGE_SLICE, this._trackStorage)
  }

  async stop () {
    this.multicast.unsubscribe(TRACK_SLICE, this._track)
    this.multicast.unsubscribe(TRACK_STORAGE_SLICE, this._trackStorage)
  }

  async getLatestSlice (path, depth, isStorage) {
    const block = await this.blockTracker.getLatestBlock()
    const slice = await this.getSliceForBlock(path, depth || this.depth, block, isStorage)
    this.emit(`latest:${path}-${depth}`, slice)
    return slice
  }

  async getSliceForBlock (path, depth, block, isStorage) {
    let stateRoot = block.stateRoot
    if (stateRoot.slice(0, 2) === '0x') {
      stateRoot = block.stateRoot.slice(2)
    }

    return this.getSliceById(`${path}-${depth || this.depth}-${stateRoot}`, isStorage)
  }

  async getSliceById (sliceId, isStorage) {
    const [path, depth, root] = sliceId.split('-')
    // if slice exists, return it
    if (this.slices.has(sliceId)) {
      return this.slices.get(sliceId)
    }

    if (this.waitingSlice.has(sliceId)) {
      return this.waitingSlice.get(sliceId)
    }

    const deferred = Promise.race([
      new Promise((resolve) => {
        this.once(`slice:${path}-${depth}-${root}`, (slice) => {
          if (slice.sliceId === sliceId) {
            return resolve(slice)
          }

          if (this.slices.has(sliceId)) {
            return resolve(slice)
          }
        })
      }).then((slice) => {
        this.waitingSlice.delete(sliceId)
        return slice
      }),
      timeout(DEFAULT_SLICE_TIMEOUT)
    ]).catch((err) => {
      log(err)
      this.waitingSlice.delete(sliceId)
      throw new Error(err)
    })

    this.waitingSlice.set(sliceId, deferred)

    // subscribe to slice topic
    this.subscribe({ path, depth, root, isStorage })
    return deferred
  }

  async publish (slice) {
    slice = normalizeKeys(slice)
    const [path, depth] = slice.sliceId.split('-')
    const topic = `${this.topic}:${path}-${depth || this.depth}`
    this.multicast.addFrwdHooks(topic, [this.slicesHook])
    this.multicast.publish(topic, Buffer.from(JSON.stringify(slice)), -1)
  }

  async subscribe ({ path, depth, root, isStorage }) {
    try {
      let sliceId = `${path}-${depth}`
      if (root) { sliceId = `${sliceId}-${root}` }

      const subscriptions = await this.multicast.ls()
      const topic = `${this.topic}:${path}-${depth || this.depth}`
      if (subscriptions.indexOf(topic) < 0) {
        if (isStorage) {
          this.multicast.publish(TRACK_STORAGE_SLICE, Buffer.from(sliceId), -1)
        } else {
          this.multicast.publish(TRACK_SLICE, Buffer.from(sliceId), -1)
        }

        this.multicast.addFrwdHooks(topic, [this.slicesHook])
        this.multicast.subscribe(topic, this._handleSlice.bind(this))
      }
    } catch (err) {
      log(err)
    }
  }

  _handleSlice (msg) {
    const data = msg.data.toString()
    try {
      const slice = normalizeKeys(JSON.parse(data))
      this.slices.set(slice.sliceId, slice)
      const [path, depth, root] = slice.sliceId.split('-')
      this.emit(`slice:${path}-${depth}-${root}`, slice)
      this.emit(`latest`, slice)
    } catch (err) {
      log(err)
    }
  }

  _handleTrack (msg, isStorage) {
    const slice = msg.data.toString()
    if (isStorage) {
      return this.emit('track-storage', slice)
    }

    this.emit('track', slice)
  }
}

module.exports = KitsunetSliceTracker
