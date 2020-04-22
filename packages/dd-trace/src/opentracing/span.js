'use strict'

const opentracing = require('opentracing')
const Span = opentracing.Span
const SpanContext = require('./span_context')
const platform = require('../platform')
const constants = require('../constants')
const id = require('../id')
const xorshift = require('xorshift')
const tagger = require('../tagger')

const SAMPLE_RATE_METRIC_KEY = constants.SAMPLE_RATE_METRIC_KEY

const jaegerClient = require('jaeger-client')
const initTracer = jaegerClient.initTracer

let dd_trace_count = 0
let jaeger_trace_count = 0

function initJaegerTracer(serviceName) {
  var config = {
    serviceName: serviceName,
    sampler: {
      type: "const",
      param: 1,
    },
    reporter: {
      logSpans: true,
    },
  };
  var options = {
    logger: {
      info: function logInfo(msg) {
        console.log("INFO ", msg);
      },
      error: function logError(msg) {
        console.log("ERROR", msg);
      },
    },
  };
  return initTracer(config, options);
}

const jaegerTracer = initJaegerTracer("nodejs-sensor-test")

const JaegerSpan = jaegerClient.Span
const JaegerSpanContext = jaegerClient.SpanContext

class DatadogSpan extends Span {
  constructor (tracer, processor, sampler, prioritySampler, fields) {
    super()

    const startTime = fields.startTime || platform.now()
    const operationName = fields.operationName
    const parent = fields.parent || null
    const tags = Object.assign({
      [SAMPLE_RATE_METRIC_KEY]: sampler.rate()
    }, fields.tags)
    const hostname = fields.hostname

    this._parentTracer = tracer
    this._sampler = sampler
    this._processor = processor
    this._prioritySampler = prioritySampler
    this._startTime = startTime

    this._spanContext = this._createContext(parent)
    this._spanContext._name = operationName
    this._spanContext._tags = tags
    this._spanContext._hostname = hostname

    this._handle = platform.metrics().track(this)
  }

  toString () {
    const spanContext = this.context()
    const resourceName = spanContext._tags['resource.name']
    const resource = resourceName.length > 100
      ? `${resourceName.substring(0, 97)}...`
      : resourceName
    const json = JSON.stringify({
      traceId: spanContext._traceId,
      spanId: spanContext._spanId,
      parentId: spanContext._parentId,
      service: spanContext._tags['service.name'],
      name: spanContext._name,
      resource
    })

    return `Span${json}`
  }

  /**
   * @param {number} input - a number of octets to allocate.
   * @return {Buffer} - returns an empty buffer.
   **/
  newBuffer (size) {
    if (Buffer.alloc) {
      return Buffer.alloc(size)
    }
    const buffer = Buffer.alloc(size)
    buffer.fill(0)
    return buffer
  }
  _createId () {
    const randint = xorshift.randomint()
    const buf = this.newBuffer(8)
    buf.writeUInt32BE(randint[0], 0)
    buf.writeUInt32BE(randint[1], 4)
    return buf
  }

  _createContext (parent) {
    let spanContext

    if (parent) {
      spanContext = new SpanContext({
        traceId: parent._traceId,
        // spanId: id(),
        spanId: this._createId(),
        parentId: parent._spanId,
        sampling: parent._sampling,
        baggageItems: parent._baggageItems,
        trace: parent._trace
      })
    } else {
      // const spanId = id()
      const spanId = this._createId()
      spanContext = new SpanContext({
        traceId: spanId,
        spanId
      })
    }

    spanContext._trace.started.push(this)

    return spanContext
  }

  _context () {
    return this._spanContext
  }

  _tracer () {
    return this._parentTracer
  }

  _setOperationName (name) {
    this._spanContext._name = name
  }

  _setBaggageItem (key, value) {
    this._spanContext._baggageItems[key] = value
  }

  _getBaggageItem (key) {
    return this._spanContext._baggageItems[key]
  }

  _addTags (keyValuePairs) {
    tagger.add(this._spanContext._tags, keyValuePairs)
  }

  _convert2JaegerSpan (finishTime) {
    const operationName = this._spanContext._name + ':' + this._spanContext._tags['resource.name']

    // Baggage is not set in SpanContext till now
    const spanContext = new JaegerSpanContext(this._spanContext._traceId,
      this._spanContext._spanId, this._spanContext._parentId)
    spanContext.finalizeSampling()
    spanContext._samplingState._flags = 1
    const jaegerSpan = new JaegerSpan(jaegerTracer, operationName, spanContext, this._startTime)

    jaegerSpan.addTags(this._spanContext._tags)
    jaegerSpan.finish(finishTime)
  }

  _finish (finishTime) {
    if (this._duration !== undefined) {
      return
    }

    finishTime = parseFloat(finishTime) || platform.now()
    this._convert2JaegerSpan(finishTime)
    // this._duration = finishTime - this._startTime
    // this._spanContext._trace.finished.push(this)
    // this._spanContext._isFinished = true
    // this._handle.finish()
    // this._processor.process(this)
  }
}

module.exports = DatadogSpan
