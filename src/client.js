'use strict'

let Promise = require('bluebird')

let _ = require('lodash')
let assert = require('assert')
let util = require('util')

let sprintf = util.format

let createClient = (db, collections) => {
  let self = { }

  self.col = name => collections[name]

  self.close = () => db.close()

  return self
}

let joinKeys = function() {
  return Array.prototype.slice.call(arguments).join('.')
}

let tools = { }

let schemaRules = { }

_.each([
  [ Boolean, _.isBoolean ],
  [ Date, _.isDate ],
  [ Number, _.isNumber ],
  [ String, _.isString ]
], _.spread( (typedef, rule) => {
  console.log('Adding schemaRule %s', rule.name)
  schemaRules[typedef.name] = descId => {
    let err = sprintf(
      'ValidationError for %s: Expected %s',
      descId,
      typedef.name || typedef.toString()
    )

    return val => assert(rule(val), err)
  }
}))

tools.resolveSchemaDesc = (desc, descId) => {
  console.log('Generating model for %s and desc %s', descId, desc.name)

  if (_.isArray(desc)) {
    assert(desc.length === 1, sprintf(
      'SchemaError for %s: Array schema to have exactly one element, got [%s]',
      descId,
      JSON.stringify(desc)
    ))

    let nestedF = tools.resolveSchemaDesc(desc[0])
    let err = sprintf(
      'ValidationError for %s: Expected array of [%s]',
      descId,
      desc[0].name || desc[0].toString()
    )

    return array => {
      assert(_.isArray(array), err)
      _.each(array, nestedF)
    }
  }
  else if (schemaRules[desc.name] !== undefined) {
    return schemaRules[desc.name](descId)
  }
  else if (_.isPlainObject(desc)) {
    return tools.resolveNestedSchema(desc, descId)
  }
  else {
    throw Error(sprintf(
      'SchemaError for %s: Schema %s is not a valid description.',
      descId,
      JSON.stringify(desc, null, 2)
    ))
  }
}

tools.resolveNestedSchema = (schema, schemaId) => {
  console.log('Generating model for %s', schemaId)

  let schemaKeys = _.filter(_.keys(schema), k => !k.startsWith('_'))

  let schemaFuncs = _.reduce(schemaKeys, (acc, key) => {
    let desc = schema[key]
    let func = tools.resolveSchemaDesc(desc, joinKeys(schemaId, key))

    acc[key] = func
    return acc
  }, Object())

  if (schema._exclusive) {
    // This is an exclusive shema, the functions must completely cover the object.
    return obj => {
      let objKeys = _.keys(obj)
      let keysDiff = _.difference(objKeys, schemaKeys)

      if (keysDiff.length !== 0) {
        throw Error(sprintf(
          'ValidationError for %s: Found extra keys [%s]. Reason: Exclusive flag set to true in schema options.',
          schemaId,
          keysDiff
        ))
      }

      // The keys are good, iterate through them.
      _.each(schemaKeys, key => {
        let _val = obj[key]
        let func = schemaFuncs[key]

        func(_val)
      })
    }
  }
  else {
    return obj => {
      _.each(schemaKeys, key => {
        let _val = obj[key]
        let func = schemaFuncs[key]

        func(_val)
      })
    }
  }
}

let Collection = (name, schema) => {
  let model = tools.resolveNestedSchema(schema, name)

  return db => {
    let col = db.collection(name)

    let self = layout => {
      model(layout)
      return layout
    }

    self.find = filter => new Promise(
      (resolve, reject) => {
        return col.find(filter).toArray( (err, docs) => {
          if (err) {
            return reject(err)
          }
          _.each(docs, model)
          resolve(docs)
        })
      }
    )

    self.insert = val => new Promise(
      (resolve, reject) => {
        model(val)
        col.insert(val, (err, result) => {
          if (err) {
            return reject(err)
          }
          console.dir(result)

          resolve(result.ops[0])
        })
      }
    )

    return self
  }
}

module.exports = () => {
  let mongo = require('mongodb').MongoClient

  // This is the object that will be exported.
  let self = { }
  let collections = { }

  self.connect = url => new Promise(
    (resolve, reject) => {
      mongo.connect(url, (err, db) => {
        if (err) {
          return reject(err)
        }
        let _collections = { }

        _.each(collections, (col, name) => {
          _collections[name] = col(db)
        })

        resolve(createClient(db, _collections))
      })
    }
  )

  self.model = (name, schema) => {
    let col = Collection(name, schema)
    collections[name] = col
    return collections[name]
  }

  return self
}
