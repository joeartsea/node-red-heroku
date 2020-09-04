
/**
 * Copyright 2014 IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

var when = require('when')
var pgutil = require('./pgutil')
const e = require('express')

var settings
var appname

function timeoutWrap(func) {
  return when.promise(function (resolve, reject, notify) {
    var promise = func().timeout(5000, 'timeout')
    promise.then(function (a, b, c, d) {
      resolve(a, b, c, d)
    })
    promise.otherwise(function (err) {
      console.log('func', func)
      console.log('timeout err', err)
      console.log('TIMEOUT: ', func.name)
      if (err == 'timeout') {
        reject(err)
      }
    })
  })
}

function getFlows() {
  console.log('getFlows')
  return when.promise(async (resolve, reject, notify) => {
    try {
      const data = await pgutil.loadConfig(appname)
      if (data && data.flows) {
        resolve(data.flows)
      } else {
        resolve([])
      }
    } catch (err) {
      reject(err)
    }
  })
}

function saveFlows(flows) {
  console.log('saveFlows')
  return when.promise(async (resolve, reject, notify) => {
    try {
      let secureLink = process.env.SECURE_LINK
      await pgutil.saveConfig(appname, { appname, flows, secureLink })
      resolve()
    } catch (err) {
      reject(err)
    }
  })
}

function getCredentials() {
  console.log('getCredentials')
  return when.promise(async (resolve, reject, notify) => {
    try {
      const data = await pgutil.loadConfig(appname)
      if (data && data.credentials) {
        resolve(data.credentials)
      } else {
        resolve({})
      }
    } catch (err) {
      reject(err)
    }
  })
}

function saveCredentials(credentials) {
  console.log('saveCredentials')
  return when.promise(async (resolve, reject, notify) => {
    try {
      await pgutil.saveConfig(appname, { appname, credentials })
      resolve()
    } catch (err) {
      reject(err)
    }
  })
}

function getSettings() {
  console.log('getSettings')
  return when.promise(async (resolve, reject, notify) => {
    try {
      const data = await pgutil.loadConfig(appname)
      if (data && data.settings) {
        resolve(data.settings)
      } else {
        resolve({})
      }
    } catch (err) {
      reject(err)
    }
  })
}

function saveSettings(settings) {
  console.log('saveSettings')
  return when.promise(async (resolve, reject, notify) => {
    try {
      await pgutil.saveConfig(appname, { appname, settings })
      resolve()
    } catch (err) {
      reject(err)
    }
  })
}

function getLibraryEntry(type, path) {
  console.log('getLibraryEntry')
  return when.promise(async (resolve, reject, notify) => {
    try {
      const data = await pgutil.loadLib(appname, type, path)
      if (data && data.body) {
        resolve(data.body)
      } else {
        if (path != '' && path.substr(-1) != '/') {
          path = path + '/'
        }
        let list = await pgutil.loadLibList(appname, type, path)
        let dirs = []
        let files = []
        for (var i = 0; i < list.length; i++) {
          let d = list[i]
          let subpath = d.path.substr(path.length)
          let parts = subpath.split('/')
          if (parts.length == 1) {
            let meta = d.meta
            meta.fn = parts[0]
            files.push(meta)
          } else if (dirs.indexOf(parts[0]) == -1) {
            dirs.push(parts[0])
          }
        }
        resolve(dirs.concat(files))
      }
    } catch (err) {
      reject(err)
    }
  })
}

function saveLibraryEntry(type, path, meta, body) {
  console.log('saveLibraryEntry')
  return when.promise(async (resolve, reject, notify) => {
    try {
      await pgutil.saveLib(appname, {
        appname,
        type,
        path,
        meta,
        body
      })
      resolve()
    } catch (err) {
      reject(err)
    }
  })
}

var pgstorage = {
  init: function (_settings) {
    settings = _settings
    appname = settings.pgAppname || require('os').hostname()
    return when.promise(async (resolve, reject, notify) => {
      try {
        const _pool = pgutil.initPG()
        resolve(_pool)
      } catch (err) {
        reject(err)
      }
    })
  },
  getFlows: function () {
    return timeoutWrap(getFlows)
  },
  saveFlows: function (flows) {
    return timeoutWrap(function () {
      return saveFlows(flows)
    })
  },

  getCredentials: function () {
    return timeoutWrap(getCredentials)
  },

  saveCredentials: function (credentials) {
    return timeoutWrap(function () {
      return saveCredentials(credentials)
    })
  },

  getSettings: function () {
    return timeoutWrap(getSettings)
  },

  saveSettings: function (data) {
    return timeoutWrap(function () {
      return saveSettings(data)
    })
  },

  getLibraryEntry: function (type, path) {
    return timeoutWrap(function () {
      return getLibraryEntry(type, path)
    })
  },
  saveLibraryEntry: function (type, path, meta, body) {
    return timeoutWrap(function () {
      return saveLibraryEntry(type, path, meta, body)
    })
  },
  mapNodeTypes: function (flows, credentials) {
    for (let props in credentials) {
      for (let i = 0; i < flows.length; i++) {
        const item = flows[i]
        if (item.id === props) {
          credentials[props].type = item.type
          break
        }
      }
    }
    return credentials
  }
}

module.exports = pgstorage