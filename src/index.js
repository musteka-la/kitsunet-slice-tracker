'use strict'

const EventEmitter = require('events')
const assert = require('assert')

const transform = require('lodash.transform')
const camelCase = require('lodash.camelcase')
const isPlainObject = require('lodash.isplainobject')

const pify = require('pify')

const log = require('debug')('kitsunet:slice-tracker')

const DEFAULT_TOPIC = `kitsunet:slice`
const DEFAULT_SLICE_TIMEOUT = 2 * 60 * 10000
const DEFAULT_DEPTH = 10

const TRACK_SLICE = `kitsunet:slice:track`
const TRACK_STORAGE_SLICE = `kitsunet:slice:track-storage`

const noop = () => {}

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
  return new Promise((resolve) => {
    setTimeout(resolve, length)
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

    this.peerSlices = new Map()
    this.slices = new Map()
    this.waitingSlice = new Map()

    this.multicast = pify(this.node.multicast)

    this._track = (msg) => this._handleTrack(msg)
    this._trackStorage = (msg) => this._handleTrack(msg, true)
  }

  _hook (peer, msg, cb) {
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

    const peerSlices = this.peerSlices.has(peer.info.id.toB58String()) || new Set()
    if (peerSlices.has(slice.sliceId)) {
      const msg = `already forwarded to peer, skipping slice ${slice.sliceId}`
      log(msg)
      return cb(msg)
    }
    peerSlices.add(slice.sliceId)
    return cb(null, msg)
  }

  async getLatestSlice (path, depth, isStorage) {
    const block = await this.blockTracker.getLatestBlock()
    const slice = await this.getSliceForBlock(path, depth || this.depth, block)
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

  async start () {
    this.multicast.subscribe(TRACK_SLICE, this._track)
    this.multicast.subscribe(TRACK_STORAGE_SLICE, this._trackStorage)
  }

  async stop () {
    this.multicast.unsubscribe(TRACK_SLICE, this._track)
    this.multicast.unsubscribe(TRACK_STORAGE_SLICE, this._trackStorage)
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
    ])

    this.waitingSlice.set(sliceId, deferred)

    // subscribe to slice topic
    this.subscribe({ path, depth, isStorage })
    return deferred
  }

  async subscribe ({path, depth, isStorage}) {
    try {
      const subscriptions = await this.multicast.ls()
      const topic = `${this.topic}:${path}-${depth || this.depth}`
      if (subscriptions.indexOf(topic) < 0) {
        this.multicast.addFrwdHooks(topic, [this._hook.bind(this)])

        this.multicast.subscribe(topic, this._handleSlice.bind(this))

        if (isStorage) {
          return this.multicast.publish(TRACK_STORAGE_SLICE, Buffer.from(`${path}-${depth}`), -1)
        }

        this.multicast.publish(TRACK_SLICE, Buffer.from(`${path}-${depth}`), -1)
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
