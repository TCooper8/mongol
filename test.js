'use strict'

let Client = require('./src/client')()

let _ = require('lodash')

Client.model('File', {
  fileUuid: String
})

Client.connect('mongodb://localhost:27017/test')
.then( db => {

  let File = db.col('File')

  let file = File({
    fileUuid: '1234'
  })

  File.insert(file).then( doc => {
    console.log('inserted file')
    console.dir(doc)

    db.close()
  })
})
