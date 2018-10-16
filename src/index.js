'use strict'

const EventEmitter = require('events')
const assert = require('assert')

const mapKeys = require('lodash.mapkeys')
const camelCase = require('lodash.camelcase')

const log = require('debug')('kitsunet:slice-tracker')

const DEFAULT_TOPIC = `kitsunet:slice`
const DEFAULT_SLICE_TIMEOUT = 60 * 10000

function normalizeSlice (slice) {
  mapKeys(slice, (_, key) => {
    return camelCase(key)
  })
}

function timeout (length) {
  return new Promise((resolve) => {
    setTimeout(resolve, length)
  })
}

class KitsunetSliceTracker extends EventEmitter {
  constructor ({ node, blockTracker }) {
    super()

    assert(node, 'node is required!')
    assert(node.multicast, 'node needs multicast!')
    assert(blockTracker, 'blockTracker is required!')

    this.node = node
    this.blockTracker = blockTracker
    this.topic = DEFAULT_TOPIC
    this.started = false

    this.peerSlices = new Map()
    this.slices = new Map()

    this.node.multicast.addFrwdHooks(this.topic, [(peer, msg, cb) => {
      let slice = null
      try {
        slice = normalizeSlice(JSON.parse(msg.data.toString()))
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
      return true
    }])
  }

  async getLatestSlice (path, depth) {
    const block = await this.blockTracker.getLatestBlock()
    return this.getSliceForBlock(path, depth, block)
  }

  async getSliceForBlock (path, depth, block) {
    const sliceId = `${path}-${depth}-${block.stateRoot}`
    const slices = this.slices(sliceId)

    // if slice exists, return it
    if (slices.has(sliceId)) {
      return slices.get(sliceId)
    }

    const deferred = Promise.race([
      Promise((resolve) => {
        this.on(`latest:${path}-${depth}`, (slice) => {
          if (slice.sliceId === sliceId) {
            return resolve(slice)
          }
        })
      }).then(() => slices.get(sliceId)),
      timeout(DEFAULT_SLICE_TIMEOUT)
    ])

    // subscribe to slice topic
    this.node.multicast.subscribe(`${this.topic}:${sliceId}`, this._handler.bind(this), () => { })
    return deferred
  }

  _handler (msg) {
    const data = msg.data.toString()
    try {
      const slice = normalizeSlice(JSON.parse(data))
      this.slices.set(slice.sliceId, slice)
      const [path, depth] = slice.sliceId.split('-')
      this.emit(`latest:${path}-${depth}`, slice)
      this.emit(`slice:${path}-${depth}`, slice)
    } catch (err) {
      log(err)
    }
  }
}

module.exports = KitsunetSliceTracker
